package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// broadcastPutCollectionV2ForAddField broadcasts the put collection v2 message for add field.
func (c *Core) broadcastPutCollectionV2ForAddField(ctx context.Context, req *milvuspb.AddCollectionFieldRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewDatabaseNameResourceKey(req.GetDbName()),
		message.NewCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
	if err != nil {
		return err
	}

	// check if the collection is created.
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	if coll.State != etcdpb.CollectionState_CollectionCreated {
		return errors.Errorf("collection is not created, can not add field, state: %s", coll.State.String())
	}

	// check if the field schema is illegal.
	fieldSchema := &schemapb.FieldSchema{}
	if err = proto.Unmarshal(req.Schema, fieldSchema); err != nil {
		return errors.Wrap(err, "failed to unmarshal field schema")
	}
	if err := checkFieldSchema([]*schemapb.FieldSchema{fieldSchema}); err != nil {
		return errors.Wrap(err, "failed to check field schema")
	}
	// check if the field already exists
	for _, field := range coll.Fields {
		if field.Name == fieldSchema.Name {
			// TODO: idempotency check here.
			return errors.Errorf("field already exists, name: %s", fieldSchema.Name)
		}
	}

	// assign a new field id.
	maxFieldID := int64(common.StartOfUserFieldID)
	for _, field := range coll.Fields {
		if field.FieldID > maxFieldID {
			maxFieldID = field.FieldID
		}
	}
	fieldSchema.FieldID = maxFieldID + 1

	// build new collection schema.
	schema := &schemapb.CollectionSchema{
		Name:               coll.Name,
		Description:        coll.Description,
		AutoID:             coll.AutoID,
		Fields:             model.MarshalFieldModels(coll.Fields),
		StructArrayFields:  model.MarshalStructArrayFieldModels(coll.StructArrayFields),
		Functions:          model.MarshalFunctionModels(coll.Functions),
		EnableDynamicField: coll.EnableDynamicField,
		Properties:         coll.Properties,
	}
	schema.Fields = append(schema.Fields, fieldSchema)

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
		}).MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (c *Core) broadcastPutCollectionV2ForAlterCollectionField(ctx context.Context, req *milvuspb.AlterCollectionFieldRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewDatabaseNameResourceKey(req.GetDbName()),
		message.NewCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
	if err != nil {
		return err
	}

	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	if coll.State != etcdpb.CollectionState_CollectionCreated {
		return errors.Errorf("collection is not created, can not alter collection, state: %s", coll.State.String())
	}

	oldFieldProperties, err := GetFieldProperties(coll, req.GetFieldName())
	if err != nil {
		return err
	}
	oldFieldPropertiesMap := common.CloneKeyValuePairs(oldFieldProperties).ToMap()
	for _, prop := range req.GetProperties() {
		oldFieldPropertiesMap[prop.GetKey()] = prop.GetValue()
	}
	for _, deleteKey := range req.GetDeleteKeys() {
		delete(oldFieldPropertiesMap, deleteKey)
	}

	newFieldProperties := common.NewKeyValuePairs(oldFieldPropertiesMap)
	if newFieldProperties.Equal(oldFieldProperties) {
		// if there's no change, return nil directly to promise idempotent.
		return nil
	}

	// build new collection schema.
	schema := &schemapb.CollectionSchema{
		Name:               coll.Name,
		Description:        coll.Description,
		AutoID:             coll.AutoID,
		Fields:             model.MarshalFieldModels(coll.Fields),
		StructArrayFields:  model.MarshalStructArrayFieldModels(coll.StructArrayFields),
		Functions:          model.MarshalFunctionModels(coll.Functions),
		EnableDynamicField: coll.EnableDynamicField,
		Properties:         coll.Properties,
	}
	for _, field := range schema.Fields {
		if field.Name == req.GetFieldName() {
			field.TypeParams = newFieldProperties
			break
		}
	}

	header := &messagespb.PutCollectionMessageHeader{
		DbId:         coll.DBID,
		CollectionId: coll.CollectionID,
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{message.FieldMaskCollectionSchema},
		},
	}
	body := &messagespb.PutCollectionMessageBody{
		Schema: schema,
	}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, message.ControlChannel)
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

// broadcastPutCollectionV2ForAlterCollection broadcasts the put collection v2 message for alter collection.
func (c *Core) broadcastPutCollectionV2ForAlterCollection(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
	// TODO: schema change should be handled at streamingnode/querynode.
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

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewDatabaseNameResourceKey(req.GetDbName()),
		message.NewCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
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

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, message.ControlChannel)
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

func (c *DDLCallback) putCollectionV2AckCallback(ctx context.Context, msgs ...message.ImmutablePutCollectionMessageV2) error {
	for _, msg := range msgs {
		if msg.VChannel() != message.ControlChannel {
			// ignore the non-control channel message.
			continue
		}
		if err := c.meta.AlterCollection(ctx, msg); err != nil {
			return err
		}
		// TODO: expire proxy cache here.
	}
	return nil
}
