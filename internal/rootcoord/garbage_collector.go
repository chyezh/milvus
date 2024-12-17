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

package rootcoord

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/log"
	ms "github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
)

//go:generate mockery --name=GarbageCollector --outpkg=mockrootcoord --filename=garbage_collector.go --with-expecter --testonly
type GarbageCollector interface {
	ReDropCollection(collMeta *model.Collection, dbName string, ts Timestamp)
	RemoveCreatingCollection(collMeta *model.Collection)
	ReDropPartition(collMeta *model.Collection, partition *model.Partition, dbName string, ts Timestamp)
	RemoveCreatingPartition(dbID int64, partition *model.Partition, ts Timestamp)
	GcCollectionData(ctx context.Context, coll *model.Collection) (ddlTs Timestamp, err error)
	GcPartitionData(ctx context.Context, pChannels, vchannels []string, partition *model.Partition) (ddlTs Timestamp, err error)
}

type bgGarbageCollector struct {
	s *Core
}

func newBgGarbageCollector(s *Core) *bgGarbageCollector {
	return &bgGarbageCollector{s: s}
}

func (c *bgGarbageCollector) ReDropCollection(collMeta *model.Collection, dbName string, ts Timestamp) {
	// TODO: remove this after data gc can be notified by rpc.
	c.s.chanTimeTick.addDmlChannels(collMeta.PhysicalChannelNames...)

	// meta cache of all aliases should also be cleaned.
	aliases := c.s.meta.ListAliasesByID(collMeta.CollectionID)

	task := newDropCollectionRedoTask(c.s, collMeta, dbName, aliases, ts, true)
	if err := task.Execute(context.Background()); err != nil {
		log.Warn("Redo drop collection failed, but the error is ignored",
			zap.Int64("collectionID", collMeta.CollectionID),
			zap.String("collectionName", collMeta.Name),
			zap.Error(err))
		return
	}
	log.Info("Redo drop collection success",
		zap.Int64("collectionID", collMeta.CollectionID),
		zap.String("collectionName", collMeta.Name),
		zap.Uint64("ts", ts))
}

func (c *bgGarbageCollector) RemoveCreatingCollection(collMeta *model.Collection) {
	// TODO: remove this after data gc can be notified by rpc.
	c.s.chanTimeTick.addDmlChannels(collMeta.PhysicalChannelNames...)

	redo := newBaseRedoTask(c.s.stepExecutor)

	redo.AddAsyncStep(&unwatchChannelsStep{
		baseStep:     baseStep{core: c.s},
		collectionID: collMeta.CollectionID,
		channels: collectionChannels{
			virtualChannels:  collMeta.VirtualChannelNames,
			physicalChannels: collMeta.PhysicalChannelNames,
		},
		isSkip: !Params.CommonCfg.TTMsgEnabled.GetAsBool(),
	})
	redo.AddAsyncStep(&removeDmlChannelsStep{
		baseStep: baseStep{core: c.s},
		collInfo: collMeta,
	})
	redo.AddAsyncStep(&deleteCollectionMetaStep{
		baseStep:     baseStep{core: c.s},
		collectionID: collMeta.CollectionID,
		// When we undo createCollectionTask, this ts may be less than the ts when unwatch channels.
		ts: collMeta.CreateTime,
	})
	// err is ignored since no sync steps will be executed.
	_ = redo.Execute(context.Background())
}

func (c *bgGarbageCollector) ReDropPartition(collMeta *model.Collection, partition *model.Partition, dbName string, ts Timestamp) {
	// TODO: remove this after data gc can be notified by rpc.
	c.s.chanTimeTick.addDmlChannels(collMeta.PhysicalChannelNames...)
	logger := log.With(zap.Int64("collectionID", collMeta.CollectionID), zap.Int64("partitionID", partition.PartitionID), zap.Uint64("ts", ts))

	task := newDropPartitionTask(c.s, collMeta, partition, dbName, ts, true)
	if err := task.Execute(context.Background()); err != nil {
		logger.Warn("Redo drop partition failed, but the error is ignored", zap.Error(err))
		return
	}
	logger.Info("Redo drop partition success")
}

func (c *bgGarbageCollector) RemoveCreatingPartition(dbID int64, partition *model.Partition, ts Timestamp) {
	redoTask := newBaseRedoTask(c.s.stepExecutor)

	redoTask.AddAsyncStep(&releasePartitionsStep{
		baseStep:     baseStep{core: c.s},
		collectionID: partition.CollectionID,
		partitionIDs: []int64{partition.PartitionID},
	})

	redoTask.AddAsyncStep(&removePartitionMetaStep{
		baseStep:     baseStep{core: c.s},
		dbID:         dbID,
		collectionID: partition.CollectionID,
		partitionID:  partition.PartitionID,
		ts:           ts,
	})

	// err is ignored since no sync steps will be executed.
	_ = redoTask.Execute(context.Background())
}

func (c *bgGarbageCollector) notifyCollectionGc(ctx context.Context, coll *model.Collection) (ddlTs Timestamp, err error) {
	if streamingutil.IsStreamingServiceEnabled() {
		return notifyCollectionGcByStreamingService(ctx, coll, c.s.session.GetServerID())
	}

	ts, err := c.s.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return 0, err
	}

	msgPack := ms.MsgPack{}
	msg := &ms.DropCollectionMsg{
		BaseMsg: ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		DropCollectionRequest: generateDropRequest(coll, ts, c.s.session.GetServerID()),
	}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	if err := c.s.chanTimeTick.broadcastDmlChannels(coll.PhysicalChannelNames, &msgPack); err != nil {
		return 0, err
	}

	return ts, nil
}

func generateDropRequest(coll *model.Collection, ts uint64, serverID int64) *msgpb.DropCollectionRequest {
	return &msgpb.DropCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DropCollection),
			commonpbutil.WithTimeStamp(ts),
			commonpbutil.WithSourceID(serverID),
		),
		CollectionName: coll.Name,
		CollectionID:   coll.CollectionID,
	}
}

func notifyCollectionGcByStreamingService(ctx context.Context, coll *model.Collection, serverID int64) (uint64, error) {
	req := generateDropRequest(coll, 0, serverID) // ts is given by streamingnode.

	msgs := make([]message.MutableMessage, 0, len(coll.VirtualChannelNames))
	for _, vchannel := range coll.VirtualChannelNames {
		msg, err := message.NewDropCollectionMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&message.DropCollectionMessageHeader{
				CollectionId: coll.CollectionID,
			}).
			WithBody(req).
			BuildMutable()
		if err != nil {
			return 0, err
		}
		msgs = append(msgs, msg)
	}
	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		return 0, err
	}
	return resp.MaxTimeTick(), nil
}

func (c *bgGarbageCollector) notifyPartitionGc(ctx context.Context, pChannels []string, partition *model.Partition) (ddlTs Timestamp, err error) {
	ts, err := c.s.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return 0, err
	}

	msgPack := ms.MsgPack{}
	msg := &ms.DropPartitionMsg{
		BaseMsg: ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		DropPartitionRequest: &msgpb.DropPartitionRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_DropPartition),
				commonpbutil.WithTimeStamp(ts),
				commonpbutil.WithSourceID(c.s.session.ServerID),
			),
			PartitionName: partition.PartitionName,
			CollectionID:  partition.CollectionID,
			PartitionID:   partition.PartitionID,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	if err := c.s.chanTimeTick.broadcastDmlChannels(pChannels, &msgPack); err != nil {
		return 0, err
	}

	return ts, nil
}

func (c *bgGarbageCollector) notifyPartitionGcByStreamingService(ctx context.Context, vchannels []string, partition *model.Partition) (uint64, error) {
	req := &msgpb.DropPartitionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DropPartition),
			commonpbutil.WithTimeStamp(0), // Timetick is given by streamingnode.
			commonpbutil.WithSourceID(c.s.session.ServerID),
		),
		PartitionName: partition.PartitionName,
		CollectionID:  partition.CollectionID,
		PartitionID:   partition.PartitionID,
	}

	msgs := make([]message.MutableMessage, 0, len(vchannels))
	for _, vchannel := range vchannels {
		msg, err := message.NewDropPartitionMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&message.DropPartitionMessageHeader{
				CollectionId: partition.CollectionID,
				PartitionId:  partition.PartitionID,
			}).
			WithBody(req).
			BuildMutable()
		if err != nil {
			return 0, err
		}
		msgs = append(msgs, msg)
	}
	// Ts is used as barrier time tick to ensure the message's time tick are given after the barrier time tick.
	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		return 0, err
	}
	return resp.MaxTimeTick(), nil
}

func (c *bgGarbageCollector) GcCollectionData(ctx context.Context, coll *model.Collection) (ddlTs Timestamp, err error) {
	c.s.ddlTsLockManager.Lock()
	c.s.ddlTsLockManager.AddRefCnt(1)
	defer c.s.ddlTsLockManager.AddRefCnt(-1)
	defer c.s.ddlTsLockManager.Unlock()

	ddlTs, err = c.notifyCollectionGc(ctx, coll)
	if err != nil {
		return 0, err
	}
	c.s.ddlTsLockManager.UpdateLastTs(ddlTs)
	return ddlTs, nil
}

func (c *bgGarbageCollector) GcPartitionData(ctx context.Context, pChannels, vchannels []string, partition *model.Partition) (ddlTs Timestamp, err error) {
	c.s.ddlTsLockManager.Lock()
	c.s.ddlTsLockManager.AddRefCnt(1)
	defer c.s.ddlTsLockManager.AddRefCnt(-1)
	defer c.s.ddlTsLockManager.Unlock()

	if streamingutil.IsStreamingServiceEnabled() {
		ddlTs, err = c.notifyPartitionGcByStreamingService(ctx, vchannels, partition)
	} else {
		ddlTs, err = c.notifyPartitionGc(ctx, pChannels, partition)
	}
	if err != nil {
		return 0, err
	}
	c.s.ddlTsLockManager.UpdateLastTs(ddlTs)
	return ddlTs, nil
}
