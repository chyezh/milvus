// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipeline

import (
	"context"
	"fmt"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type insertNode struct {
	*BaseNode
	collectionID int64
	channel      string
	manager      *DataManager
	delegator    delegator.ShardDelegator
}

func (iNode *insertNode) addInsertData(insertDatas map[UniqueID]*delegator.InsertData, msg *InsertMsg, collection *Collection) {
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(collection.Schema(), msg)
	if err != nil {
		err = fmt.Errorf("failed to get primary keys, err = %d", err)
		mlog.Error(context.TODO(), err.Error(), mlog.Int64("collectionID", iNode.collectionID), mlog.String("channel", iNode.channel))
		panic(err)
	}
	iData, ok := insertDatas[msg.SegmentID]
	if !ok {
		iData = &delegator.InsertData{
			PartitionID:  msg.PartitionID,
			InsertRecord: insertRecord,
			StartPosition: &msgpb.MsgPosition{
				Timestamp:   msg.BeginTs(),
				ChannelName: msg.GetShardName(),
			},
		}
		insertDatas[msg.SegmentID] = iData
	} else {
		err := typeutil.MergeFieldData(iData.InsertRecord.FieldsData, insertRecord.FieldsData)
		if err != nil {
			mlog.Error(context.TODO(), "failed to merge field data", mlog.String("channel", iNode.channel), mlog.Err(err))
			panic(err)
		}
		iData.InsertRecord.NumRows += insertRecord.NumRows
	}

	pks, err := segments.GetPrimaryKeys(msg, collection.Schema())
	if err != nil {
		mlog.Error(context.TODO(), "failed to get primary keys from insert message", mlog.Err(err))
		panic(err)
	}

	iData.PrimaryKeys = append(iData.PrimaryKeys, pks...)
	iData.RowIDs = append(iData.RowIDs, msg.RowIDs...)
	iData.Timestamps = append(iData.Timestamps, msg.Timestamps...)
	mlog.Debug(context.TODO(), "pipeline fetch insert msg",
		mlog.Int64("collectionID", iNode.collectionID),
		mlog.Int64("segmentID", msg.SegmentID),
		mlog.Int("insertRowNum", len(pks)),
		mlog.Uint64("timestampMin", msg.BeginTimestamp),
		mlog.Uint64("timestampMax", msg.EndTimestamp))
}

// Insert task
func (iNode *insertNode) Operate(in Msg) Msg {
	metrics.QueryNodeWaitProcessingMsgCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).Dec()
	nodeMsg := in.(*insertNodeMsg)

	if len(nodeMsg.insertMsgs) > 0 {
		sort.Slice(nodeMsg.insertMsgs, func(i, j int) bool {
			return nodeMsg.insertMsgs[i].BeginTs() < nodeMsg.insertMsgs[j].BeginTs()
		})

		// build insert data if no embedding node
		if nodeMsg.insertDatas == nil {
			collection := iNode.manager.Collection.Get(iNode.collectionID)
			if collection == nil {
				mlog.Error(context.TODO(), "insertNode with collection not exist", mlog.Int64("collection", iNode.collectionID))
				panic("insertNode with collection not exist")
			}

			nodeMsg.insertDatas = make(map[UniqueID]*delegator.InsertData)
			// get InsertData and merge datas of same segment
			for _, msg := range nodeMsg.insertMsgs {
				iNode.addInsertData(nodeMsg.insertDatas, msg, collection)
			}
		}

		iNode.delegator.ProcessInsert(nodeMsg.insertDatas)
	}
	metrics.QueryNodeWaitProcessingMsgCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.DeleteLabel).Inc()

	return &deleteNodeMsg{
		deleteMsgs:    nodeMsg.deleteMsgs,
		timeRange:     nodeMsg.timeRange,
		schema:        nodeMsg.schema,
		schemaVersion: nodeMsg.schemaVersion,
	}
}

func newInsertNode(
	collectionID UniqueID,
	channel string,
	manager *DataManager,
	delegator delegator.ShardDelegator,
	maxQueueLength int32,
) *insertNode {
	return &insertNode{
		BaseNode:     base.NewBaseNode(fmt.Sprintf("InsertNode-%s", channel), maxQueueLength),
		collectionID: collectionID,
		channel:      channel,
		manager:      manager,
		delegator:    delegator,
	}
}
