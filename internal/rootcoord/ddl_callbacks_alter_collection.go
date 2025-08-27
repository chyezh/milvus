package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// broadcastPutCollectionV2ForAlterCollection broadcasts the put collection v2 message for alter collection.
func (c *Core) broadcastPutCollectionV2ForAlterCollection(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
	if len(req.GetProperties()) == 0 && len(req.GetDeleteKeys()) == 0 {
		return errors.New("The collection properties to alter and keys to delete must not be empty at the same time")
	}

	if len(req.GetProperties()) > 0 && len(req.GetDeleteKeys()) > 0 {
		return errors.New("can not provide properties and deletekeys at the same time")
	}

	if hookutil.ContainsCipherProperties(req.GetProperties(), req.GetDeleteKeys()) {
		log.Info("skip to alter collection due to cipher properties were detected in the properties")
		return errors.New("can not alter cipher related properties")
	}

	ok, targetValue, err := common.IsEnableDynamicSchema(req.GetProperties())
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid dynamic schema property value: %s", req.GetProperties()[0].GetValue())
	}
	if ok {
		// if there's dynamic schema property, it will add a new dynamic field into the collection.
		// the property cannot be seen at collection properties, only add a new field into the collection.
		return c.broadcastPutCollectionV2ForAlterDynamicField(ctx, req, targetValue)
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(req.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
	if err != nil {
		return err
	}

	coll, err := c.meta.GetCollectionByName(ctx, req.DbName, req.CollectionName, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	if coll.State != etcdpb.CollectionState_CollectionCreated {
		return errors.Errorf("collection is not created, can not alter collection, state: %s", coll.State.String())
	}

	header := &messagespb.PutCollectionMessageHeader{
		DbId:         coll.DBID,
		CollectionId: coll.CollectionID,
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{},
		},
	}
	body := &messagespb.PutCollectionMessageBody{}

	// Apply the properties to override the existing properties.
	newProperties := common.CloneKeyValuePairs(coll.Properties).ToMap()
	for _, prop := range req.GetProperties() {
		switch prop.GetKey() {
		case common.CollectionDescription:
			if prop.GetValue() != coll.Description {
				body.Description = prop.GetValue()
				header.UpdateMask.Paths = append(header.UpdateMask.Paths, message.FieldMaskCollectionDescription)
			}
		case common.ConsistencyLevel:
			if lv, ok := unmarshalConsistencyLevel(prop.GetValue()); ok && lv != coll.ConsistencyLevel {
				body.ConsistencyLevel = lv
				header.UpdateMask.Paths = append(header.UpdateMask.Paths, message.FieldMaskCollectionConsistencyLevel)
			}
		default:
			newProperties[prop.GetKey()] = prop.GetValue()
		}
	}
	for _, deleteKey := range req.GetDeleteKeys() {
		delete(newProperties, deleteKey)
	}
	// Check if the properties are changed.
	newPropsKeyValuePairs := common.NewKeyValuePairs(newProperties)
	if !newPropsKeyValuePairs.Equal(coll.Properties) {
		body.Properties = newPropsKeyValuePairs
		header.UpdateMask.Paths = append(header.UpdateMask.Paths, message.FieldMaskCollectionProperties)
	}

	// if there's no change, return nil directly to promise idempotent.
	if len(header.UpdateMask.Paths) == 0 {
		return nil
	}

	// fill the put load config if rg or replica number is changed.
	body.PutLoadConfig = c.getPutLoadConfigOfPutCollection(ctx, coll.Properties, body.Properties)

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	for _, vchannel := range coll.VirtualChannelNames {
		channels = append(channels, vchannel)
	}
	msg := message.NewPutCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// broadcastPutCollectionV2ForAlterDynamicField broadcasts the put collection v2 message for alter dynamic field.
func (c *Core) broadcastPutCollectionV2ForAlterDynamicField(ctx context.Context, req *milvuspb.AlterCollectionRequest, targetValue bool) error {
	if len(req.GetProperties()) != 1 {
		return merr.WrapErrParameterInvalidMsg("cannot alter dynamic schema with other properties")
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(req.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
	if err != nil {
		return err
	}

	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		log.Ctx(ctx).Warn("get collection failed during alter dynamic schema",
			zap.String("collectionName", req.GetCollectionName()))
		return err
	}

	// return nil for no-op
	if coll.EnableDynamicField == targetValue {
		return nil
	}

	// not support disabling since remove field not support yet.
	if !targetValue {
		return merr.WrapErrParameterInvalidMsg("dynamic schema cannot supported to be disabled")
	}

	// convert to add $meta json field, nullable, default value `{}`
	fieldSchema := &schemapb.FieldSchema{
		Name:      common.MetaFieldName,
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
		Nullable:  true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_BytesData{
				BytesData: []byte("{}"),
			},
		},
	}
	if err := checkFieldSchema([]*schemapb.FieldSchema{fieldSchema}); err != nil {
		return err
	}

	fieldSchema.FieldID = nextFieldID(coll)
	schema := &schemapb.CollectionSchema{
		Name:               coll.Name,
		Description:        coll.Description,
		AutoID:             coll.AutoID,
		Fields:             model.MarshalFieldModels(coll.Fields),
		StructArrayFields:  model.MarshalStructArrayFieldModels(coll.StructArrayFields),
		Functions:          model.MarshalFunctionModels(coll.Functions),
		EnableDynamicField: targetValue,
		Properties:         coll.Properties,
	}
	schema.Fields = append(schema.Fields, fieldSchema)

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	for _, vchannel := range coll.VirtualChannelNames {
		channels = append(channels, vchannel)
	}
	// broadcast the put collection v2 message.
	msg := message.NewPutCollectionMessageBuilderV2().
		WithHeader(&messagespb.PutCollectionMessageHeader{
			DbId:         coll.DBID,
			CollectionId: coll.CollectionID,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionSchema},
			},
		}).
		WithBody(&messagespb.PutCollectionMessageBody{
			Schema: schema,
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// getPutLoadConfigOfPutCollection gets the put load config of put collection.
func (c *Core) getPutLoadConfigOfPutCollection(ctx context.Context, oldProps []*commonpb.KeyValuePair, newProps []*commonpb.KeyValuePair) *message.PutLoadConfigOfPutCollection {
	oldReplicaNumber, _ := common.DatabaseLevelReplicaNumber(oldProps)
	oldResourceGroups, _ := common.DatabaseLevelResourceGroups(oldProps)
	newReplicaNumber, _ := common.DatabaseLevelReplicaNumber(newProps)
	newResourceGroups, _ := common.DatabaseLevelResourceGroups(newProps)
	left, right := lo.Difference(oldResourceGroups, newResourceGroups)
	rgChanged := len(left) > 0 || len(right) > 0
	replicaChanged := oldReplicaNumber != newReplicaNumber
	if !replicaChanged && !rgChanged {
		return nil
	}

	return &message.PutLoadConfigOfPutCollection{
		ReplicaNumber:  int32(newReplicaNumber),
		ResourceGroups: newResourceGroups,
	}
}

func (c *DDLCallback) putCollectionV2AckCallback(ctx context.Context, result message.BroadcastResultPutCollectionMessageV2) error {
	header := result.Message.Header()
	body := result.Message.MustBody()
	if err := c.meta.AlterCollection(ctx, result); err != nil {
		return err
	}

	if body.PutLoadConfig != nil {
		resp, err := c.mixCoord.UpdateLoadConfig(ctx, &querypb.UpdateLoadConfigRequest{
			CollectionIDs:  []int64{header.CollectionId},
			ReplicaNumber:  body.PutLoadConfig.ReplicaNumber,
			ResourceGroups: body.PutLoadConfig.ResourceGroups,
		})
		return merr.CheckRPCCall(resp, err)
	}
	return nil
}
