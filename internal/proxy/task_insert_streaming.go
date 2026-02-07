package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// we only overwrite the Execute function
func (it *insertTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Insert-Execute")
	defer sp.End()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute insert streaming %d", it.ID()))

	collectionName := it.insertMsg.CollectionName
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.insertMsg.GetDbName(), collectionName)
	if err != nil {
		mlog.Warn(ctx, "fail to get collection id", mlog.Err(err))
		return err
	}
	it.insertMsg.CollectionID = collID

	getCacheDur := tr.RecordSpan()
	channelNames, err := it.chMgr.getVChannels(collID)
	if err != nil {
		mlog.Warn(ctx, "get vChannels failed", mlog.Int64("collectionID", collID), mlog.Err(err))
		it.result.Status = merr.Status(err)
		return err
	}

	mlog.Debug(ctx, "send insert request to virtual channels",
		mlog.String("partition", it.insertMsg.GetPartitionName()),
		mlog.Int64("collectionID", collID),
		mlog.Strings("virtual_channels", channelNames),
		mlog.Int64("task_id", it.ID()),
		mlog.Bool("is_parition_key", it.partitionKeys != nil),
		mlog.Duration("get cache duration", getCacheDur))

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(it.schema.GetProperties(), it.collectionID).AsMessageConfig()
	}

	// start to repack insert data
	var msgs []message.MutableMessage
	if it.partitionKeys == nil {
		msgs, err = repackInsertDataForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result, ez)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(it.TraceCtx(), channelNames, it.insertMsg, it.result, it.partitionKeys, ez)
	}
	if err != nil {
		mlog.Warn(ctx, "assign segmentID and repack insert data failed", mlog.Err(err))
		it.result.Status = merr.Status(err)
		return err
	}
	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		mlog.Warn(context.TODO(), "append messages to wal failed", mlog.Err(err))
		it.result.Status = merr.Status(err)
	}
	// Update result.Timestamp for session consistency.
	it.result.Timestamp = resp.MaxTimeTick()
	return nil
}

func repackInsertDataForStreamingService(
	ctx context.Context,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	result *milvuspb.MutationResult,
	ez *message.CipherConfig,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0)

	channel2RowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)
	partitionName := insertMsg.PartitionName
	partitionID, err := globalMetaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
	if err != nil {
		return nil, err
	}

	for channel, rowOffsets := range channel2RowOffsets {
		// segment id is assigned at streaming node.
		msgs, err := genInsertMsgsByPartition(ctx, 0, partitionID, partitionName, rowOffsets, channel, insertMsg)
		if err != nil {
			return nil, err
		}
		for _, msg := range msgs {
			insertRequest := msg.(*msgstream.InsertMsg).InsertRequest
			newMsg, err := message.NewInsertMessageBuilderV1().
				WithVChannel(channel).
				WithHeader(&message.InsertMessageHeader{
					CollectionId: insertMsg.CollectionID,
					Partitions: []*message.PartitionSegmentAssignment{
						{
							PartitionId: partitionID,
							Rows:        insertRequest.GetNumRows(),
							BinarySize:  0, // TODO: current not used, message estimate size is used.
						},
					},
				}).
				WithBody(insertRequest).
				WithCipher(ez).
				BuildMutable()
			if err != nil {
				return nil, err
			}
			messages = append(messages, newMsg)
		}
	}
	return messages, nil
}

func repackInsertDataWithPartitionKeyForStreamingService(
	ctx context.Context,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	result *milvuspb.MutationResult,
	partitionKeys *schemapb.FieldData,
	ez *message.CipherConfig,
) ([]message.MutableMessage, error) {
	messages := make([]message.MutableMessage, 0)

	channel2RowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, insertMsg.GetDbName(), insertMsg.CollectionName)
	if err != nil {
		mlog.Warn(ctx, "get default partition names failed in partition key mode",
			mlog.String("collectionName", insertMsg.CollectionName),
			mlog.Err(err))
		return nil, err
	}

	// Get partition ids
	partitionIDs := make(map[string]int64, 0)
	for _, partitionName := range partitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, insertMsg.GetDbName(), insertMsg.CollectionName, partitionName)
		if err != nil {
			mlog.Warn(ctx, "get partition id failed",
				mlog.String("collectionName", insertMsg.CollectionName),
				mlog.String("partitionName", partitionName),
				mlog.Err(err))
			return nil, err
		}
		partitionIDs[partitionName] = partitionID
	}

	hashValues, err := typeutil.HashKey2Partitions(partitionKeys, partitionNames)
	if err != nil {
		mlog.Warn(ctx, "has partition keys to partitions failed",
			mlog.String("collectionName", insertMsg.CollectionName),
			mlog.Err(err))
		return nil, err
	}
	for channel, rowOffsets := range channel2RowOffsets {
		partition2RowOffsets := make(map[string][]int)
		for _, idx := range rowOffsets {
			partitionName := partitionNames[hashValues[idx]]
			if _, ok := partition2RowOffsets[partitionName]; !ok {
				partition2RowOffsets[partitionName] = []int{}
			}
			partition2RowOffsets[partitionName] = append(partition2RowOffsets[partitionName], idx)
		}

		for partitionName, rowOffsets := range partition2RowOffsets {
			msgs, err := genInsertMsgsByPartition(ctx, 0, partitionIDs[partitionName], partitionName, rowOffsets, channel, insertMsg)
			if err != nil {
				return nil, err
			}
			for _, msg := range msgs {
				insertRequest := msg.(*msgstream.InsertMsg).InsertRequest
				newMsg, err := message.NewInsertMessageBuilderV1().
					WithVChannel(channel).
					WithHeader(&message.InsertMessageHeader{
						CollectionId: insertMsg.CollectionID,
						Partitions: []*message.PartitionSegmentAssignment{
							{
								PartitionId: partitionIDs[partitionName],
								Rows:        insertRequest.GetNumRows(),
								BinarySize:  0, // TODO: current not used, message estimate size is used.
							},
						},
					}).
					WithBody(insertRequest).
					WithCipher(ez).
					BuildMutable()
				if err != nil {
					return nil, err
				}
				messages = append(messages, newMsg)
			}
		}
	}
	return messages, nil
}
