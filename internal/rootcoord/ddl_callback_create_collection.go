package rootcoord

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"google.golang.org/protobuf/proto"
)

// registerCreateCollectionV1Callbacks registers the create collection v1 callbacks.
func (c *DDLCallback) registerCreateCollectionV1Callbacks() {
	registry.RegisterCreateCollectionV1AckCallback(c.createCollectionV1AckCallback)
}

func (c *Core) broadcastCreateCollectionV1(ctx context.Context, req *milvuspb.CreateCollectionRequest) error {
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(req.GetSchema(), schema); err != nil {
		return err
	}
	if req.GetShardsNum() == 0 {
		req.ShardsNum = common.DefaultShardsNum
	}
	if _, err := typeutil.GetPartitionKeyFieldSchema(schema); err == nil {
		req.NumPartitions = common.DefaultPartitionsWithPartitionKey
	}
	if req.GetNumPartitions() == 0 {
		req.NumPartitions = int64(1)
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewDatabaseNameResourceKey(req.GetDbName()),
		message.NewCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
	if err != nil {
		return err
	}

	// prepare and validate the create collection message.
	createCollectionTask := createCollectionTask{
		Core:   c,
		Req:    req,
		header: &message.CreateCollectionMessageHeader{},
		body: &message.CreateCollectionRequest{
			DbName:         req.GetDbName(),
			CollectionName: req.GetCollectionName(),
			Schema:         schema,
		},
	}
	if err := createCollectionTask.Prepare(ctx); err != nil {
		return err
	}

	// setup the broadcast virtual channels and control channel, then make a broadcast message.
	broadcastChannel := make([]string, 0, createCollectionTask.Req.ShardsNum+1)
	broadcastChannel = append(broadcastChannel, message.ControlChannel)
	for i := 0; i < int(createCollectionTask.Req.ShardsNum); i++ {
		broadcastChannel = append(broadcastChannel, createCollectionTask.body.VirtualChannelNames[i])
	}
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(createCollectionTask.header).
		WithBody(createCollectionTask.body).
		WithBroadcast(broadcastChannel,
			message.NewDatabaseNameResourceKey(createCollectionTask.body.DbName),
			message.NewCollectionNameResourceKey(createCollectionTask.body.DbName, createCollectionTask.body.CollectionName),
		).
		MustBuildBroadcast()

	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (c *DDLCallback) createCollectionV1AckCallback(ctx context.Context, msgs ...message.ImmutableCreateCollectionMessageV1) error {
	var collInfos []*model.Collection
	for _, msg := range msgs {
		collInfos = append(collInfos, newCollectionModelWithMessage(msg))
		if msg.VChannel() != message.ControlChannel {
			// create shard info when virtual channel is created.
			if err := c.createCollectionShard(ctx, msg); err != nil {
				return err
			}
		}
	}

	// Put the merged collection info into meta.
	mergedCollInfo := mergeCollectionModel(collInfos...)
	if err := c.meta.AddCollection(ctx, mergedCollInfo); err != nil {
		return err
	}

	// cleanup the proxy cache.
	if err := c.Core.ExpireMetaCache(ctx,
		msgs[0].MustBody().DbName,
		[]string{msgs[0].MustBody().CollectionName},
		msgs[0].Header().CollectionId,
		"",
		msgs[0].TimeTick(),
		proxyutil.SetMsgType(commonpb.MsgType_DropCollection)); err != nil {
		return err
	}
	return nil
}

func (c *DDLCallback) createCollectionShard(ctx context.Context, msg message.ImmutableCreateCollectionMessageV1) error {
	header := msg.Header()
	body := msg.MustBody()

	startPosition := adaptor.MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize()
	resp, err := c.mixCoord.WatchChannels(ctx, &datapb.WatchChannelsRequest{
		CollectionID:    header.CollectionId,
		ChannelNames:    []string{msg.VChannel()},
		StartPositions:  []*commonpb.KeyDataPair{{Key: msg.VChannel(), Data: startPosition}},
		Schema:          body.Schema,
		CreateTimestamp: msg.TimeTick(),
	})
	if err != nil {
		return err
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("failed to watch channels, code: %s, reason: %s", resp.GetStatus().GetErrorCode(), resp.GetStatus().GetReason())
	}
	return nil
}

// mergeCollectionModel merges the given collection models into a single collection model.
func mergeCollectionModel(models ...*model.Collection) *model.Collection {
	if len(models) == 1 {
		return models[0]
	}
	// createTimeTick from cchannel
	createTimeTick := uint64(0)
	// startPosition from vchannel
	startPosition := make(map[string][]byte, len(models[0].PhysicalChannelNames))
	state := etcdpb.CollectionState_CollectionCreating
	for _, model := range models {
		if model.CreateTime != 0 && createTimeTick == 0 {
			createTimeTick = model.CreateTime
		}
		for key, value := range toMap(model.StartPositions) {
			if _, ok := startPosition[key]; !ok {
				startPosition[key] = value
			}
		}
		if len(startPosition) == len(models[0].PhysicalChannelNames) && createTimeTick != 0 {
			// if all vchannels and cchannel are created, the collection is created
			state = etcdpb.CollectionState_CollectionCreated
		}
	}

	mergedModel := models[0]
	mergedModel.CreateTime = createTimeTick
	for _, partition := range mergedModel.Partitions {
		partition.PartitionCreatedTimestamp = createTimeTick
	}
	mergedModel.StartPositions = toKeyDataPairs(startPosition)
	mergedModel.State = state
	return mergedModel
}

// newCollectionModelWithMessage creates a collection model with the given message.
func newCollectionModelWithMessage(msg message.ImmutableCreateCollectionMessageV1) *model.Collection {
	header := msg.Header()
	body := msg.MustBody()

	if msg.VChannel() == message.ControlChannel {
		// Set the global timetick with the cchannel timetick.
		return newCollectionModel(header, body, msg.TimeTick())
	}

	// Setup the start position for the vchannels
	newCollInfo := newCollectionModel(header, body, 0)
	startPosition := make(map[string][]byte, len(body.PhysicalChannelNames))
	startPosition[funcutil.ToPhysicalChannel(msg.VChannel())] = adaptor.MustGetMQWrapperIDFromMessage(msg.MessageID()).Serialize()
	newCollInfo.StartPositions = toKeyDataPairs(startPosition)
	return newCollInfo
}

// newCollectionModel creates a collection model with the given header, body and timestamp.
func newCollectionModel(header *message.CreateCollectionMessageHeader, body *message.CreateCollectionRequest, ts uint64) *model.Collection {
	partitions := make([]*model.Partition, 0, len(body.PartitionIDs))
	for idx, partition := range body.PartitionIDs {
		partitions = append(partitions, &model.Partition{
			PartitionID:               partition,
			PartitionName:             body.PartitionNames[idx],
			PartitionCreatedTimestamp: ts,
			CollectionID:              header.CollectionId,
			State:                     etcdpb.PartitionState_PartitionCreated,
		})
	}
	_, consistencyLevel := getConsistencyLevel(body.Schema.Properties...)
	return &model.Collection{
		CollectionID:         header.CollectionId,
		DBID:                 header.DbId,
		Name:                 body.Schema.Name,
		DBName:               body.DbName,
		Description:          body.Schema.Description,
		AutoID:               body.Schema.AutoID,
		Fields:               model.UnmarshalFieldModels(body.Schema.Fields),
		StructArrayFields:    model.UnmarshalStructArrayFieldModels(body.Schema.StructArrayFields),
		Functions:            model.UnmarshalFunctionModels(body.Schema.Functions),
		VirtualChannelNames:  body.VirtualChannelNames,
		PhysicalChannelNames: body.PhysicalChannelNames,
		ShardsNum:            int32(len(body.VirtualChannelNames)),
		ConsistencyLevel:     consistencyLevel,
		CreateTime:           ts,
		State:                etcdpb.CollectionState_CollectionCreating,
		Partitions:           partitions,
		Properties:           body.Schema.Properties,
		EnableDynamicField:   body.Schema.EnableDynamicField,
		UpdateTimestamp:      ts,
	}
}
