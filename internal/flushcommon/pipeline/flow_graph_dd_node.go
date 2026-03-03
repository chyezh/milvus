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
	"reflect"
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// make sure ddNode implements flowgraph.Node
var _ flowgraph.Node = (*ddNode)(nil)

// ddNode filters messages from message streams.
//
// ddNode recives all the messages from message stream dml channels, including insert messages,
//
//	delete messages and ddl messages like CreateCollectionMsg and DropCollectionMsg.
//
// ddNode filters insert messages according to the `sealedSegment`.
// ddNode will filter out the insert message for those who belong to `sealedSegment`
//
// When receiving a `DropCollection` message, ddNode will send a signal to DataNode `BackgroundGC`
//
//	goroutinue, telling DataNode to release the resources of this particular flow graph.
//
// After the filtering process, ddNode passes all the valid insert messages and delete message
//
//	to the following flow graph node, which in DataNode is `insertBufferNode`
type ddNode struct {
	BaseNode

	ctx          context.Context
	collectionID typeutil.UniqueID
	vChannelName string

	dropMode   atomic.Value
	msgHandler util.MsgHandler

	// for recovery
	growingSegInfo    map[typeutil.UniqueID]*datapb.SegmentInfo // segmentID
	sealedSegInfo     map[typeutil.UniqueID]*datapb.SegmentInfo // segmentID
	droppedSegmentIDs []int64
}

// Name returns node name, implementing flowgraph.Node
func (ddn *ddNode) Name() string {
	return fmt.Sprintf("ddNode-%s", ddn.vChannelName)
}

func (ddn *ddNode) IsValidInMsg(in []Msg) bool {
	if !ddn.BaseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn(context.TODO(), "type assertion failed for MsgStreamMsg", log.String("name", reflect.TypeOf(in[0]).Name()))
		return false
	}
	return true
}

// Operate handles input messages, implementing flowgrpah.Node
func (ddn *ddNode) Operate(in []Msg) []Msg {
	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn(context.TODO(), "type assertion failed for MsgStreamMsg", log.String("channel", ddn.vChannelName), log.String("name", reflect.TypeOf(in[0]).Name()))
		return []Msg{}
	}

	if msMsg.IsCloseMsg() {
		fgMsg := FlowGraphMsg{
			BaseMsg:        flowgraph.NewBaseMsg(true),
			InsertMessages: make([]*msgstream.InsertMsg, 0),
			TimeRange: util.TimeRange{
				TimestampMin: msMsg.TimestampMin(),
				TimestampMax: msMsg.TimestampMax(),
			},
			StartPositions: msMsg.StartPositions(),
			EndPositions:   msMsg.EndPositions(),
			dropCollection: false,
		}
		log.Warn(context.TODO(), "MsgStream closed", log.Any("ddNode node", ddn.Name()), log.String("channel", ddn.vChannelName), log.Int64("collection", ddn.collectionID))
		return []Msg{&fgMsg}
	}

	if load := ddn.dropMode.Load(); load != nil && load.(bool) {
		log.RatedInfo(context.TODO(), log.RateDefault, "ddNode in dropMode", log.String("channel", ddn.vChannelName))
		return []Msg{}
	}

	var spans []trace.Span
	for _, msg := range msMsg.TsMessages() {
		ctx, sp := util.StartTracer(msg, "DDNode-Operate")
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}
	defer func() {
		for _, sp := range spans {
			sp.End()
		}
	}()

	fgMsg := FlowGraphMsg{
		InsertMessages: make([]*msgstream.InsertMsg, 0),
		TimeRange: util.TimeRange{
			TimestampMin: msMsg.TimestampMin(),
			TimestampMax: msMsg.TimestampMax(),
		},
		StartPositions: make([]*msgpb.MsgPosition, 0),
		EndPositions:   make([]*msgpb.MsgPosition, 0),
		dropCollection: false,
	}

	for _, msg := range msMsg.TsMessages() {
		switch msg.Type() {
		case commonpb.MsgType_DropCollection:
			if msg.(*msgstream.DropCollectionMsg).GetCollectionID() == ddn.collectionID {
				log.Info(ddn.ctx, "Receiving DropCollection msg", log.String("channel", ddn.vChannelName))
				ddn.dropMode.Store(true)
				fgMsg.dropCollection = true
			}

		case commonpb.MsgType_DropPartition:
			dpMsg := msg.(*msgstream.DropPartitionMsg)
			if dpMsg.GetCollectionID() == ddn.collectionID {
				log.Info(ddn.ctx, "drop partition msg received", log.String("channel", ddn.vChannelName), log.Int64("partitionID", dpMsg.GetPartitionID()))
				fgMsg.dropPartitions = append(fgMsg.dropPartitions, dpMsg.PartitionID)
			}

		case commonpb.MsgType_Insert:
			imsg := msg.(*msgstream.InsertMsg)
			if imsg.CollectionID != ddn.collectionID {
				log.Warn(ddn.ctx, "filter invalid insert message, collection mis-match",
					log.Int64("Get collID", imsg.CollectionID),
					log.String("channel", ddn.vChannelName),
					log.Int64("Expected collID", ddn.collectionID))
				continue
			}

			if ddn.tryToFilterSegmentInsertMessages(imsg) {
				log.Debug(ddn.ctx, "filter insert messages",
					log.Int64("filter segmentID", imsg.GetSegmentID()),
					log.String("channel", ddn.vChannelName),
					log.Uint64("message timestamp", msg.EndTs()),
				)
				continue
			}

			util.GetRateCollector().Add(metricsinfo.InsertConsumeThroughput, float64(proto.Size(imsg.InsertRequest)))

			metrics.DataNodeConsumeBytesCount.
				WithLabelValues(paramtable.GetStringNodeID(), metrics.InsertLabel).
				Add(float64(proto.Size(imsg.InsertRequest)))

			metrics.DataNodeConsumeMsgCount.
				WithLabelValues(paramtable.GetStringNodeID(), metrics.InsertLabel, fmt.Sprint(ddn.collectionID)).
				Inc()

			metrics.DataNodeConsumeMsgRowsCount.
				WithLabelValues(paramtable.GetStringNodeID(), metrics.InsertLabel).
				Add(float64(imsg.GetNumRows()))

			log.Debug(ddn.ctx, "DDNode receive insert messages",
				log.Int64("segmentID", imsg.GetSegmentID()),
				log.String("channel", ddn.vChannelName),
				log.Int("numRows", len(imsg.GetRowIDs())),
				log.Uint64("startPosTs", msMsg.StartPositions()[0].GetTimestamp()),
				log.Uint64("endPosTs", msMsg.EndPositions()[0].GetTimestamp()))
			fgMsg.InsertMessages = append(fgMsg.InsertMessages, imsg)

		case commonpb.MsgType_Delete:
			dmsg := msg.(*msgstream.DeleteMsg)

			if dmsg.CollectionID != ddn.collectionID {
				log.Warn(ddn.ctx, "filter invalid DeleteMsg, collection mis-match",
					log.Int64("Get collID", dmsg.CollectionID),
					log.String("channel", ddn.vChannelName),
					log.Int64("Expected collID", ddn.collectionID))
				continue
			}

			log.Debug(ddn.ctx, "DDNode receive delete messages",
				log.String("channel", ddn.vChannelName),
				log.Int64("numRows", dmsg.NumRows),
				log.Uint64("startPosTs", msMsg.StartPositions()[0].GetTimestamp()),
				log.Uint64("endPosTs", msMsg.EndPositions()[0].GetTimestamp()),
			)
			util.GetRateCollector().Add(metricsinfo.DeleteConsumeThroughput, float64(proto.Size(dmsg.DeleteRequest)))

			metrics.DataNodeConsumeBytesCount.
				WithLabelValues(paramtable.GetStringNodeID(), metrics.DeleteLabel).
				Add(float64(proto.Size(dmsg.DeleteRequest)))

			metrics.DataNodeConsumeMsgCount.
				WithLabelValues(paramtable.GetStringNodeID(), metrics.DeleteLabel, fmt.Sprint(ddn.collectionID)).
				Inc()

			metrics.DataNodeConsumeMsgRowsCount.
				WithLabelValues(paramtable.GetStringNodeID(), metrics.DeleteLabel).
				Add(float64(dmsg.GetNumRows()))
			fgMsg.DeleteMessages = append(fgMsg.DeleteMessages, dmsg)
		case commonpb.MsgType_CreateSegment:
			createSegment := msg.(*adaptor.CreateSegmentMessageBody)
			logger := log.With(
				log.String("vchannel", ddn.Name()),
				log.Int32("msgType", int32(msg.Type())),
				log.Uint64("timetick", createSegment.CreateSegmentMessage.TimeTick()),
			)
			logger.Info(ddn.ctx, "receive create segment message")
			if err := ddn.msgHandler.HandleCreateSegment(ddn.ctx, createSegment.CreateSegmentMessage); err != nil {
				logger.Warn(ddn.ctx, "handle create segment message failed", log.Err(err))
			} else {
				logger.Info(ddn.ctx, "handle create segment message success")
			}
		case commonpb.MsgType_FlushSegment:
			flushMsg := msg.(*adaptor.FlushMessageBody)
			logger := log.With(
				log.String("vchannel", ddn.Name()),
				log.Int32("msgType", int32(msg.Type())),
				log.Uint64("timetick", flushMsg.FlushMessage.TimeTick()),
			)
			logger.Info(ddn.ctx, "receive flush message")
			if err := ddn.msgHandler.HandleFlush(flushMsg.FlushMessage); err != nil {
				logger.Warn(ddn.ctx, "handle flush message failed", log.Err(err))
			} else {
				logger.Info(ddn.ctx, "handle flush message success")
			}
		case commonpb.MsgType_ManualFlush:
			manualFlushMsg := msg.(*adaptor.ManualFlushMessageBody)
			logger := log.With(
				log.String("vchannel", ddn.Name()),
				log.Int32("msgType", int32(msg.Type())),
				log.Uint64("timetick", manualFlushMsg.ManualFlushMessage.TimeTick()),
				log.Uint64("flushTs", manualFlushMsg.ManualFlushMessage.Header().FlushTs),
				log.Int64s("segmentIDs", manualFlushMsg.ManualFlushMessage.Header().SegmentIds),
			)
			logger.Info(ddn.ctx, "receive manual flush message")
			if err := ddn.msgHandler.HandleManualFlush(manualFlushMsg.ManualFlushMessage); err != nil {
				logger.Warn(ddn.ctx, "handle manual flush message failed", log.Err(err))
			} else {
				logger.Info(ddn.ctx, "handle manual flush message success")
			}
		case commonpb.MsgType_FlushAll:
			flushAllMsg := msg.(*adaptor.FlushAllMessageBody)
			log.Info(ddn.ctx, "receive flush all message",
				log.String("vchannel", ddn.Name()),
				log.Int32("msgType", int32(msg.Type())),
				log.Uint64("timetick", flushAllMsg.FlushAllMessage.TimeTick()),
			)
			ddn.msgHandler.HandleFlushAll(ddn.vChannelName, flushAllMsg.FlushAllMessage)
		case commonpb.MsgType_AddCollectionField:
			schemaMsg := msg.(*adaptor.SchemaChangeMessageBody)
			logger := log.With(
				log.String("vchannel", ddn.Name()),
				log.Int32("msgType", int32(msg.Type())),
				log.Uint64("timetick", schemaMsg.SchemaChangeMessage.TimeTick()),
				log.Int64s("segmentIDs", schemaMsg.SchemaChangeMessage.Header().FlushedSegmentIds),
			)
			logger.Info(ddn.ctx, "receive schema change message")
			ddn.msgHandler.HandleSchemaChange(ddn.ctx, schemaMsg.SchemaChangeMessage)
		case commonpb.MsgType_AlterCollection:
			alterCollectionMsg := msg.(*adaptor.AlterCollectionMessageBody)
			logger := log.With(
				log.String("vchannel", ddn.Name()),
				log.Int32("msgType", int32(msg.Type())),
				log.Uint64("timetick", alterCollectionMsg.AlterCollectionMessage.TimeTick()),
			)
			logger.Info(ddn.ctx, "receive put collection message")
			if err := ddn.msgHandler.HandleAlterCollection(ddn.ctx, alterCollectionMsg.AlterCollectionMessage); err != nil {
				logger.Warn(ddn.ctx, "handle put collection message failed", log.Err(err))
			} else {
				logger.Info(ddn.ctx, "handle put collection message success")
			}
		case commonpb.MsgType_TruncateCollection:
			truncateCollectionMsg := msg.(*adaptor.TruncateCollectionMessageBody)
			logger := log.With(
				log.String("vchannel", ddn.Name()),
				log.Int32("msgType", int32(msg.Type())),
				log.Uint64("timetick", truncateCollectionMsg.TruncateCollectionMessage.TimeTick()),
				log.Int64s("segmentIDs", truncateCollectionMsg.TruncateCollectionMessage.Header().SegmentIds),
			)
			logger.Info(ddn.ctx, "receive truncate collection message")
			if err := ddn.msgHandler.HandleTruncateCollection(truncateCollectionMsg.TruncateCollectionMessage); err != nil {
				logger.Warn(ddn.ctx, "handle truncate collection message failed", log.Err(err))
			} else {
				logger.Info(ddn.ctx, "handle truncate collection message success")
			}
		case commonpb.MsgType_AlterWAL:
			alterWALMsg := msg.(*adaptor.AlterWALMessageBody)
			logger := log.With(
				log.String("pchannel", alterWALMsg.AlterWALMessage.VChannel()), // pchannel that received the alter wal message
				log.String("vchannel", ddn.vChannelName),                       // vchannel of the current flow graph pipeline
				log.Stringer("targetWalName", alterWALMsg.AlterWALMessage.Header().TargetWalName),
				log.Uint64("timetick", alterWALMsg.AlterWALMessage.TimeTick()),
			)
			logger.Info(ddn.ctx, "receive alter wal message")
			if err := ddn.msgHandler.HandleAlterWAL(ddn.ctx, alterWALMsg.AlterWALMessage, ddn.vChannelName); err != nil {
				logger.Warn(ddn.ctx, "handle alter wal message failed", log.Err(err))
			} else {
				logger.Info(ddn.ctx, "handle alter wal message success")
			}
			fgMsg.isAlterWal = true
			fgMsg.alterWalTimeTick = alterWALMsg.AlterWALMessage.TimeTick()
		}
	}

	fgMsg.StartPositions = append(fgMsg.StartPositions, msMsg.StartPositions()...)
	fgMsg.EndPositions = append(fgMsg.EndPositions, msMsg.EndPositions()...)

	return []Msg{&fgMsg}
}

func (ddn *ddNode) tryToFilterSegmentInsertMessages(msg *msgstream.InsertMsg) bool {
	if msg.GetShardName() != ddn.vChannelName {
		return true
	}

	// Filter all dropped segments
	if ddn.isDropped(msg.GetSegmentID()) {
		return true
	}

	// Filter all sealed segments until current Ts > Sealed segment cp
	for segID, segInfo := range ddn.sealedSegInfo {
		if msg.EndTs() > segInfo.GetDmlPosition().GetTimestamp() {
			delete(ddn.sealedSegInfo, segID)
		}
	}
	if _, ok := ddn.sealedSegInfo[msg.GetSegmentID()]; ok {
		return true
	}

	// Filter all growing segments until current Ts > growing segment dmlPosition
	if si, ok := ddn.growingSegInfo[msg.GetSegmentID()]; ok {
		if msg.EndTs() <= si.GetDmlPosition().GetTimestamp() {
			return true
		}

		delete(ddn.growingSegInfo, msg.GetSegmentID())
	}

	return false
}

func (ddn *ddNode) isDropped(segID typeutil.UniqueID) bool {
	for _, droppedSegmentID := range ddn.droppedSegmentIDs {
		if droppedSegmentID == segID {
			return true
		}
	}
	return false
}

func (ddn *ddNode) Close() {
	log.Info(context.TODO(), "Flowgraph DD Node closing")
}

func newDDNode(ctx context.Context, collID typeutil.UniqueID, vChannelName string, droppedSegmentIDs []typeutil.UniqueID,
	sealedSegments []*datapb.SegmentInfo, growingSegments []*datapb.SegmentInfo, handler util.MsgHandler,
) *ddNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(paramtable.Get().DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	baseNode.SetMaxParallelism(paramtable.Get().DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32())

	dd := &ddNode{
		ctx:               ctx,
		BaseNode:          baseNode,
		collectionID:      collID,
		sealedSegInfo:     make(map[typeutil.UniqueID]*datapb.SegmentInfo, len(sealedSegments)),
		growingSegInfo:    make(map[typeutil.UniqueID]*datapb.SegmentInfo, len(growingSegments)),
		droppedSegmentIDs: droppedSegmentIDs,
		vChannelName:      vChannelName,
		msgHandler:        handler,
	}

	dd.dropMode.Store(false)

	for _, s := range sealedSegments {
		dd.sealedSegInfo[s.GetID()] = s
	}

	for _, s := range growingSegments {
		dd.growingSegInfo[s.GetID()] = s
	}
	log.Info(context.TODO(), "ddNode add sealed and growing segments",
		log.Int64("collectionID", collID),
		log.Int("No. sealed segments", len(sealedSegments)),
		log.Int("No. growing segments", len(growingSegments)),
	)

	return dd
}
