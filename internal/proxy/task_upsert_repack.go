package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

// repackUpsertDataForStreamingService repacks upsert data (insert + delete) by hashing primary keys
func repackUpsertDataForStreamingService(
	ctx context.Context,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	deleteMsg *msgstream.DeleteMsg,
	result *milvuspb.MutationResult,
	idAllocator allocator.Interface,
	ts uint64,
	dbName string,
	ez *message.CipherConfig,
) ([]message.MutableMessage, error) {
	// Hash insert data by primary keys
	channel2InsertRowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)

	// Hash delete data by primary keys
	deleteHashValues := typeutil.HashPK2Channels(deleteMsg.PrimaryKeys, channelNames)

	// Group delete primary keys by channel
	channel2DeletePKs := make(map[string]*schemapb.IDs)
	for idx, hashValue := range deleteHashValues {
		channelName := channelNames[hashValue]
		if channel2DeletePKs[channelName] == nil {
			channel2DeletePKs[channelName] = &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: make([]int64, 0)},
				},
			}
			if deleteMsg.PrimaryKeys.GetStrId() != nil {
				channel2DeletePKs[channelName] = &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{Data: make([]string, 0)},
					},
				}
			}
		}

		// Append primary key to channel group
		if intPKs := deleteMsg.PrimaryKeys.GetIntId(); intPKs != nil {
			channel2DeletePKs[channelName].GetIntId().Data = append(
				channel2DeletePKs[channelName].GetIntId().Data,
				intPKs.Data[idx],
			)
		} else if strPKs := deleteMsg.PrimaryKeys.GetStrId(); strPKs != nil {
			channel2DeletePKs[channelName].GetStrId().Data = append(
				channel2DeletePKs[channelName].GetStrId().Data,
				strPKs.Data[idx],
			)
		}
	}

	partitionName := insertMsg.PartitionName
	partitionID, err := globalMetaCache.GetPartitionID(ctx, dbName, insertMsg.CollectionName, partitionName)
	if err != nil {
		return nil, err
	}

	// Build upsert messages for each channel
	messages := make([]message.MutableMessage, 0)
	for channel, insertRowOffsets := range channel2InsertRowOffsets {
		// Generate insert messages for this channel
		insertMsgs, err := genInsertMsgsByPartition(ctx, 0, partitionID, partitionName, insertRowOffsets, channel, insertMsg)
		if err != nil {
			return nil, err
		}

		// Get delete data for this channel
		deletePKs := channel2DeletePKs[channel]
		if deletePKs == nil {
			deletePKs = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, 0)}}}
		}

		// Generate delete messages for this channel
		deleteMsgsForChannel, _, err := repackDeleteMsgByHash(
			ctx, deletePKs, []string{channel}, idAllocator, ts,
			deleteMsg.CollectionID, deleteMsg.CollectionName,
			deleteMsg.PartitionID, deleteMsg.PartitionName, dbName,
		)
		if err != nil {
			return nil, err
		}

		// Combine insert and delete into upsert messages
		for idx, insertMsgItem := range insertMsgs {
			insertRequest := insertMsgItem.(*msgstream.InsertMsg).InsertRequest

			var deleteRequest *msgpb.DeleteRequest
			if len(deleteMsgsForChannel) > 0 {
				// Use the first delete message, or corresponding one if available
				deleteIdx := idx
				if deleteIdx >= len(deleteMsgsForChannel[0]) {
					deleteIdx = len(deleteMsgsForChannel[0]) - 1
				}
				if deleteIdx >= 0 && deleteIdx < len(deleteMsgsForChannel[0]) {
					deleteRequest = deleteMsgsForChannel[0][deleteIdx].DeleteRequest
				}
			}

			// If no delete request, create empty one
			if deleteRequest == nil {
				deleteRequest = &msgpb.DeleteRequest{
					Base:           insertRequest.Base,
					CollectionID:   deleteMsg.CollectionID,
					PartitionID:    deleteMsg.PartitionID,
					CollectionName: deleteMsg.CollectionName,
					PartitionName:  deleteMsg.PartitionName,
					DbName:         dbName,
					ShardName:      channel,
					PrimaryKeys:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, 0)}}},
				}
			}

			// Build upsert message
			upsertBody := &message.UpsertMessageBody{
				InsertRequest: insertRequest,
				DeleteRequest: deleteRequest,
			}

			deleteRows := uint64(0)
			if deleteRequest.PrimaryKeys.GetIntId() != nil {
				deleteRows = uint64(len(deleteRequest.PrimaryKeys.GetIntId().Data))
			} else if deleteRequest.PrimaryKeys.GetStrId() != nil {
				deleteRows = uint64(len(deleteRequest.PrimaryKeys.GetStrId().Data))
			}

			msg, err := message.NewUpsertMessageBuilderV2().
				WithVChannel(channel).
				WithHeader(&message.UpsertMessageHeader{
					CollectionId: insertMsg.CollectionID,
					Partitions: []*message.PartitionSegmentAssignment{
						{
							PartitionId: partitionID,
							Rows:        insertRequest.GetNumRows(),
							BinarySize:  0,
						},
					},
					DeleteRows: deleteRows,
				}).
				WithBody(upsertBody).
				WithCipher(ez).
				BuildMutable()
			if err != nil {
				return nil, err
			}
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

// repackUpsertDataWithPartitionKeyForStreamingService repacks upsert data with partition key support
func repackUpsertDataWithPartitionKeyForStreamingService(
	ctx context.Context,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	deleteMsg *msgstream.DeleteMsg,
	result *milvuspb.MutationResult,
	partitionKeys *schemapb.FieldData,
	idAllocator allocator.Interface,
	ts uint64,
	dbName string,
	ez *message.CipherConfig,
) ([]message.MutableMessage, error) {
	// Hash insert data by primary keys
	channel2InsertRowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)

	// Get default partitions in partition key mode
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, dbName, insertMsg.CollectionName)
	if err != nil {
		log.Ctx(ctx).Warn("get default partition names failed in partition key mode",
			zap.String("collectionName", insertMsg.CollectionName),
			zap.Error(err))
		return nil, err
	}

	// Get partition IDs
	partitionIDs := make(map[string]int64, 0)
	for _, partitionName := range partitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, dbName, insertMsg.CollectionName, partitionName)
		if err != nil {
			log.Ctx(ctx).Warn("get partition id failed",
				zap.String("collectionName", insertMsg.CollectionName),
				zap.String("partitionName", partitionName),
				zap.Error(err))
			return nil, err
		}
		partitionIDs[partitionName] = partitionID
	}

	// Hash partition keys to partitions
	hashValues, err := typeutil.HashKey2Partitions(partitionKeys, partitionNames)
	if err != nil {
		log.Ctx(ctx).Warn("hash partition keys to partitions failed",
			zap.String("collectionName", insertMsg.CollectionName),
			zap.Error(err))
		return nil, err
	}

	// Hash delete data by primary keys
	deleteHashValues := typeutil.HashPK2Channels(deleteMsg.PrimaryKeys, channelNames)
	channel2DeletePKs := make(map[string]*schemapb.IDs)
	for idx, hashValue := range deleteHashValues {
		channelName := channelNames[hashValue]
		if channel2DeletePKs[channelName] == nil {
			channel2DeletePKs[channelName] = &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: make([]int64, 0)},
				},
			}
			if deleteMsg.PrimaryKeys.GetStrId() != nil {
				channel2DeletePKs[channelName] = &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{Data: make([]string, 0)},
					},
				}
			}
		}

		if intPKs := deleteMsg.PrimaryKeys.GetIntId(); intPKs != nil {
			channel2DeletePKs[channelName].GetIntId().Data = append(
				channel2DeletePKs[channelName].GetIntId().Data,
				intPKs.Data[idx],
			)
		} else if strPKs := deleteMsg.PrimaryKeys.GetStrId(); strPKs != nil {
			channel2DeletePKs[channelName].GetStrId().Data = append(
				channel2DeletePKs[channelName].GetStrId().Data,
				strPKs.Data[idx],
			)
		}
	}

	messages := make([]message.MutableMessage, 0)
	for channel, insertRowOffsets := range channel2InsertRowOffsets {
		// Group insert row offsets by partition
		partition2RowOffsets := make(map[string][]int)
		for _, idx := range insertRowOffsets {
			partitionName := partitionNames[hashValues[idx]]
			if _, ok := partition2RowOffsets[partitionName]; !ok {
				partition2RowOffsets[partitionName] = []int{}
			}
			partition2RowOffsets[partitionName] = append(partition2RowOffsets[partitionName], idx)
		}

		// Generate upsert messages for each partition in this channel
		for partitionName, rowOffsets := range partition2RowOffsets {
			insertMsgs, err := genInsertMsgsByPartition(ctx, 0, partitionIDs[partitionName], partitionName, rowOffsets, channel, insertMsg)
			if err != nil {
				return nil, err
			}

			// Get delete data for this channel
			deletePKs := channel2DeletePKs[channel]
			if deletePKs == nil {
				deletePKs = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, 0)}}}
			}

			deleteMsgsForChannel, _, err := repackDeleteMsgByHash(
				ctx, deletePKs, []string{channel}, idAllocator, ts,
				deleteMsg.CollectionID, deleteMsg.CollectionName,
				partitionIDs[partitionName], partitionName, dbName,
			)
			if err != nil {
				return nil, err
			}

			// Combine insert and delete into upsert messages
			for idx, insertMsgItem := range insertMsgs {
				insertRequest := insertMsgItem.(*msgstream.InsertMsg).InsertRequest

				var deleteRequest *msgpb.DeleteRequest
				if len(deleteMsgsForChannel) > 0 {
					deleteIdx := idx
					if deleteIdx >= len(deleteMsgsForChannel[0]) {
						deleteIdx = len(deleteMsgsForChannel[0]) - 1
					}
					if deleteIdx >= 0 && deleteIdx < len(deleteMsgsForChannel[0]) {
						deleteRequest = deleteMsgsForChannel[0][deleteIdx].DeleteRequest
					}
				}

				if deleteRequest == nil {
					deleteRequest = &msgpb.DeleteRequest{
						Base:           insertRequest.Base,
						CollectionID:   deleteMsg.CollectionID,
						PartitionID:    partitionIDs[partitionName],
						CollectionName: deleteMsg.CollectionName,
						PartitionName:  partitionName,
						DbName:         dbName,
						ShardName:      channel,
						PrimaryKeys:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, 0)}}},
					}
				}

				upsertBody := &message.UpsertMessageBody{
					InsertRequest: insertRequest,
					DeleteRequest: deleteRequest,
				}

				deleteRows := uint64(0)
				if deleteRequest.PrimaryKeys.GetIntId() != nil {
					deleteRows = uint64(len(deleteRequest.PrimaryKeys.GetIntId().Data))
				} else if deleteRequest.PrimaryKeys.GetStrId() != nil {
					deleteRows = uint64(len(deleteRequest.PrimaryKeys.GetStrId().Data))
				}

				msg, err := message.NewUpsertMessageBuilderV2().
					WithVChannel(channel).
					WithHeader(&message.UpsertMessageHeader{
						CollectionId: insertMsg.CollectionID,
						Partitions: []*message.PartitionSegmentAssignment{
							{
								PartitionId: partitionIDs[partitionName],
								Rows:        insertRequest.GetNumRows(),
								BinarySize:  0,
							},
						},
						DeleteRows: deleteRows,
					}).
					WithBody(upsertBody).
					WithCipher(ez).
					BuildMutable()
				if err != nil {
					return nil, err
				}
				messages = append(messages, msg)
			}
		}
	}

	return messages, nil
}
