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

package dist

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type TriggerUpdateTargetVersion = func(collectionID int64)

type NotifyDelegatorChanges = func(collectionID ...int64)

type distHandler struct {
	nodeID       int64
	c            chan struct{}
	wg           sync.WaitGroup
	client       session.Cluster
	nodeManager  *session.NodeManager
	scheduler    task.Scheduler
	dist         *meta.DistributionManager
	target       meta.TargetManagerInterface
	mu           sync.Mutex
	stopOnce     sync.Once
	lastUpdateTs int64

	notifyFunc NotifyDelegatorChanges
}

func (dh *distHandler) start(ctx context.Context) {
	defer dh.wg.Done()
	ctx = mlog.WithFields(ctx, mlog.Int64("nodeID", dh.nodeID))
	mlog.Info(ctx, "start dist handler")
	distInterval := Params.QueryCoordCfg.DistPullInterval.GetAsDuration(time.Millisecond)
	ticker := time.NewTicker(distInterval)
	defer ticker.Stop()
	flagInterval := Params.QueryCoordCfg.CheckExecutedFlagInterval.GetAsDuration(time.Millisecond)
	checkExecutedFlagTicker := time.NewTicker(flagInterval)
	defer checkExecutedFlagTicker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			mlog.Info(ctx, "close dist handler due to context done")
			return
		case <-dh.c:
			mlog.Info(ctx, "close dist handler")
			return
		case <-checkExecutedFlagTicker.C:
			executedFlagChan := dh.scheduler.GetExecutedFlag(dh.nodeID)
			if executedFlagChan != nil {
				select {
				case <-executedFlagChan:
					dh.pullDist(ctx, &failures, false)
				default:
				}
			}
			// only reset when interval updated
			newFlagInterval := Params.QueryCoordCfg.CheckExecutedFlagInterval.GetAsDuration(time.Millisecond)
			if newFlagInterval != flagInterval {
				flagInterval = newFlagInterval
				select {
				case <-checkExecutedFlagTicker.C:
				default:
				}
				checkExecutedFlagTicker.Reset(flagInterval)
			}
		case <-ticker.C:
			dh.pullDist(ctx, &failures, true)
			// only reset when interval updated
			newDistInterval := Params.QueryCoordCfg.DistPullInterval.GetAsDuration(time.Millisecond)
			if newDistInterval != distInterval {
				distInterval = newDistInterval
				select {
				case <-ticker.C:
				default:
				}
				ticker.Reset(distInterval)
			}
		}
	}
}

func (dh *distHandler) pullDist(ctx context.Context, failures *int, dispatchTask bool) {
	tr := timerecord.NewTimeRecorder("")
	resp, err := dh.getDistribution(ctx)
	d1 := tr.RecordSpan()
	if err != nil {
		node := dh.nodeManager.Get(dh.nodeID)
		*failures = *failures + 1
		fields := []zap.Field{mlog.Int("times", *failures)}
		if node != nil {
			fields = append(fields, mlog.Time("lastHeartbeat", node.LastHeartbeat()))
		}
		fields = append(fields, mlog.Err(err))
		mlog.RatedWarn(ctx, 1.0/60, "failed to get data distribution", fields...)
	} else {
		*failures = 0
		dh.handleDistResp(ctx, resp, dispatchTask)
	}
	mlog.RatedInfo(ctx, 1.0/120, "pull and handle distribution done",
		mlog.Int("respSize", proto.Size(resp)), mlog.Duration("pullDur", d1), mlog.Duration("handleDur", tr.RecordSpan()))
}

func (dh *distHandler) handleDistResp(ctx context.Context, resp *querypb.GetDataDistributionResponse, dispatchTask bool) {
	node := dh.nodeManager.Get(resp.GetNodeID())
	if node == nil {
		return
	}

	if time.Since(node.LastHeartbeat()) > paramtable.Get().QueryCoordCfg.HeartBeatWarningLag.GetAsDuration(time.Millisecond) {
		mlog.Warn(ctx, "node last heart beat time lag too behind", mlog.Time("now", time.Now()),
			mlog.Time("lastHeartBeatTime", node.LastHeartbeat()), mlog.Int64("nodeID", node.ID()))
	}
	now := time.Now()
	node.SetLastHeartbeat(now)
	metrics.QueryCoordLastHeartbeatTimeStamp.WithLabelValues(fmt.Sprint(resp.GetNodeID())).Set(float64(now.UnixNano()))

	// skip  update dist if no distribution change happens in query node
	if resp.GetLastModifyTs() != 0 && resp.GetLastModifyTs() <= dh.lastUpdateTs {
		mlog.RatedInfo(ctx, 1.0/30, "skip update dist due to no distribution change", mlog.Int64("lastModifyTs", resp.GetLastModifyTs()), mlog.Int64("lastUpdateTs", dh.lastUpdateTs))
	} else {
		dh.lastUpdateTs = resp.GetLastModifyTs()

		node.UpdateStats(
			session.WithSegmentCnt(len(resp.GetSegments())),
			session.WithChannelCnt(len(resp.GetChannels())),
			session.WithMemCapacity(resp.GetMemCapacityInMB()),
			session.WithChannelCnt(int(resp.GetCpuNum())),
		)
		dh.updateSegmentsDistribution(ctx, resp)
		dh.updateChannelsDistribution(ctx, resp)
	}

	if dispatchTask {
		dh.scheduler.Dispatch(dh.nodeID)
	}
}

func (dh *distHandler) SetNotifyFunc(notifyFunc NotifyDelegatorChanges) {
	dh.notifyFunc = notifyFunc
}

func (dh *distHandler) updateSegmentsDistribution(ctx context.Context, resp *querypb.GetDataDistributionResponse) {
	updates := make([]*meta.Segment, 0, len(resp.GetSegments()))
	for _, s := range resp.GetSegments() {
		// To maintain compatibility with older versions of QueryNode,
		// QueryCoord should neither process nor interact with L0 segments.
		if s.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}
		segmentInfo := dh.target.GetSealedSegment(ctx, s.GetCollection(), s.GetID(), meta.CurrentTargetFirst)
		if segmentInfo == nil {
			segmentInfo = &datapb.SegmentInfo{
				ID:            s.GetID(),
				CollectionID:  s.GetCollection(),
				PartitionID:   s.GetPartition(),
				InsertChannel: s.GetChannel(),
				Level:         s.GetLevel(),
				IsSorted:      s.GetIsSorted(),
			}
		}
		updates = append(updates, &meta.Segment{
			SegmentInfo:        segmentInfo,
			Node:               resp.GetNodeID(),
			Version:            s.GetVersion(),
			LastDeltaTimestamp: s.GetLastDeltaTimestamp(),
			IndexInfo:          s.GetIndexInfo(),
			JSONStatsField:     s.GetJsonStatsInfo(),
			ManifestPath:       s.GetManifestPath(),
		})
	}

	dh.dist.SegmentDistManager.Update(resp.GetNodeID(), updates...)
}

func (dh *distHandler) updateChannelsDistribution(ctx context.Context, resp *querypb.GetDataDistributionResponse) {
	channelMap := lo.SliceToMap(resp.GetChannels(), func(ch *querypb.ChannelVersionInfo) (string, *querypb.ChannelVersionInfo) {
		return ch.GetChannel(), ch
	})

	updates := make([]*meta.DmChannel, 0, len(resp.GetChannels()))
	for _, lview := range resp.GetLeaderViews() {
		channel, ok := channelMap[lview.GetChannel()]
		if !ok {
			// unreachable path, querynode should return leader view and channel dist at same time
			mlog.RatedInfo(ctx, 1.0/60, "channel not found in distribution",
				mlog.Int64("collectionID", lview.GetCollection()),
				mlog.String("channel", lview.GetChannel()))
			continue
		}
		delegatorVersion := channel.GetVersion()

		// Get or create channel info
		collectionID := lview.GetCollection()
		channelName := lview.GetChannel()
		channelInfo := dh.target.GetDmChannel(ctx, collectionID, channelName, meta.CurrentTarget)
		var vChannelInfo *datapb.VchannelInfo
		if channelInfo != nil {
			vChannelInfo = channelInfo.VchannelInfo
		} else {
			vChannelInfo = &datapb.VchannelInfo{
				ChannelName:  channelName,
				CollectionID: collectionID,
			}
		}

		// Pre-allocate growing segments map
		growings := lo.MapValues(lview.GetGrowingSegments(), func(position *msgpb.MsgPosition, id int64) *meta.Segment {
			return &meta.Segment{
				SegmentInfo: &datapb.SegmentInfo{
					ID:            id,
					CollectionID:  collectionID,
					StartPosition: position,
					InsertChannel: channelName,
				},
				Node: resp.GetNodeID(),
			}
		})

		// Update DmChannel and register shard leader in same loop
		dmChannel := &meta.DmChannel{
			VchannelInfo: vChannelInfo,
			Node:         resp.NodeID,
			Version:      delegatorVersion,
			View: &meta.LeaderView{
				ID:                     resp.NodeID,
				CollectionID:           collectionID,
				Channel:                channelName,
				Version:                delegatorVersion,
				Segments:               lview.GetSegmentDist(),
				GrowingSegments:        growings,
				NumOfGrowingRows:       lview.GetNumOfGrowingRows(),
				PartitionStatsVersions: lview.PartitionStatsVersions,
				TargetVersion:          lview.GetTargetVersion(),
				Status:                 lview.GetStatus(),
			},
		}
		updates = append(updates, dmChannel)

		serviceable := checkDelegatorServiceable(ctx, dh, dmChannel.View)
		// trigger pull next target until shard leader is ready
		if !serviceable {
			dh.lastUpdateTs = 0
		}
	}

	newLeaderOnNode := dh.dist.ChannelDistManager.Update(resp.GetNodeID(), updates...)
	if dh.notifyFunc != nil {
		collectionIDs := typeutil.NewUniqueSet()
		for _, ch := range newLeaderOnNode {
			collectionIDs.Insert(ch.VchannelInfo.CollectionID)
		}
		dh.notifyFunc(collectionIDs.Collect()...)
	}
}

func checkDelegatorServiceable(ctx context.Context, dh *distHandler, view *meta.LeaderView) bool {
	if status := view.Status; status != nil {
		if !status.GetServiceable() {
			mlog.RatedInfo(ctx, 1.0/10, "delegator is not serviceable", mlog.Int64("queryViewVersion", view.TargetVersion))
			return false
		}
		return true
	}

	// check leader data ready for version before 2.5.8
	if err := utils.CheckDelegatorDataReady(dh.nodeManager, dh.target, view, meta.CurrentTarget); err != nil {
		mlog.RatedInfo(ctx, 1.0/10, "delegator is not serviceable due to distribution not ready", mlog.Err(err))
		view.Status = &querypb.LeaderViewStatus{
			Serviceable: false,
		}
		return false
	}

	// if target version hasn't been synced, delegator will get empty readable segment list
	// so shard leader should be unserviceable until target version is synced
	currentTargetVersion := dh.target.GetCollectionTargetVersion(ctx, view.CollectionID, meta.CurrentTarget)
	if view.TargetVersion <= 0 {
		mlog.RatedInfo(ctx, 1.0/10, "delegator is not serviceable due to target version not ready",
			mlog.Int64("currentTargetVersion", currentTargetVersion),
			mlog.Int64("leaderTargetVersion", view.TargetVersion))
		view.Status = &querypb.LeaderViewStatus{
			Serviceable: false,
		}
		return false
	}

	view.Status = &querypb.LeaderViewStatus{
		Serviceable: true,
	}
	return true
}

func (dh *distHandler) getDistribution(ctx context.Context) (*querypb.GetDataDistributionResponse, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.DistributionRequestTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	resp, err := dh.client.GetDataDistribution(ctx, dh.nodeID, &querypb.GetDataDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetDistribution),
		),
		LastUpdateTs: dh.lastUpdateTs,
	})
	if err != nil {
		return nil, err
	}
	if !merr.Ok(resp.GetStatus()) {
		return nil, merr.Error(resp.GetStatus())
	}
	return resp, nil
}

func (dh *distHandler) stop() {
	dh.stopOnce.Do(func() {
		close(dh.c)
		dh.wg.Wait()

		// clear dist
		dh.dist.ChannelDistManager.Update(dh.nodeID)
		dh.dist.SegmentDistManager.Update(dh.nodeID)
	})
}

func newDistHandler(
	ctx context.Context,
	nodeID int64,
	client session.Cluster,
	nodeManager *session.NodeManager,
	scheduler task.Scheduler,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	notifyFunc NotifyDelegatorChanges,
) *distHandler {
	h := &distHandler{
		nodeID:      nodeID,
		c:           make(chan struct{}),
		client:      client,
		nodeManager: nodeManager,
		scheduler:   scheduler,
		dist:        dist,
		target:      targetMgr,
		notifyFunc:  notifyFunc,
	}
	h.wg.Add(1)
	go h.start(ctx)
	return h
}
