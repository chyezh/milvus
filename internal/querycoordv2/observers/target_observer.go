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

package observers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type targetOp int

func (op *targetOp) String() string {
	switch *op {
	case UpdateCollection:
		return "UpdateCollection"
	case ReleaseCollection:
		return "ReleaseCollection"
	case ReleasePartition:
		return "ReleasePartition"
	default:
		return "Unknown"
	}
}

const (
	UpdateCollection targetOp = iota + 1
	ReleaseCollection
	ReleasePartition
	UpdatePartition
)

type targetUpdateRequest struct {
	CollectionID  int64
	PartitionIDs  []int64
	Notifier      chan error
	ReadyNotifier chan struct{}
	opType        targetOp
}

type initRequest struct{}

type TargetObserver struct {
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	meta      *meta.Meta
	targetMgr meta.TargetManagerInterface
	distMgr   *meta.DistributionManager
	broker    meta.Broker
	cluster   session.Cluster
	nodeMgr   *session.NodeManager

	initChan chan initRequest
	// nextTargetLastUpdate map[int64]time.Time
	nextTargetLastUpdate *typeutil.ConcurrentMap[int64, time.Time]
	updateChan           chan targetUpdateRequest
	mut                  sync.Mutex                // Guard readyNotifiers
	readyNotifiers       map[int64][]chan struct{} // CollectionID -> Notifiers

	// loadingDispatcher updates targets for collections that are loading (also collections without a current target).
	loadingDispatcher *taskDispatcher[int64]
	// loadedDispatcher updates targets for loaded collections.
	loadedDispatcher *taskDispatcher[int64]

	keylocks *lock.KeyLock[int64]

	startOnce sync.Once
	stopOnce  sync.Once
}

func NewTargetObserver(
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	distMgr *meta.DistributionManager,
	broker meta.Broker,
	cluster session.Cluster,
	nodeMgr *session.NodeManager,
) *TargetObserver {
	result := &TargetObserver{
		meta:                 meta,
		targetMgr:            targetMgr,
		distMgr:              distMgr,
		broker:               broker,
		cluster:              cluster,
		nodeMgr:              nodeMgr,
		nextTargetLastUpdate: typeutil.NewConcurrentMap[int64, time.Time](),
		updateChan:           make(chan targetUpdateRequest, 10),
		readyNotifiers:       make(map[int64][]chan struct{}),
		initChan:             make(chan initRequest),
		keylocks:             lock.NewKeyLock[int64](),
	}

	result.loadingDispatcher = newTaskDispatcher(result.check)
	result.loadedDispatcher = newTaskDispatcher(result.check)
	return result
}

func (ob *TargetObserver) Start() {
	ob.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		ob.cancel = cancel

		ob.loadingDispatcher.Start()
		ob.loadedDispatcher.Start()

		ob.wg.Add(1)
		go func() {
			defer ob.wg.Done()
			ob.schedule(ctx)
		}()

		// after target observer start, update target for all collection
		ob.initChan <- initRequest{}
	})
}

func (ob *TargetObserver) Stop() {
	ob.stopOnce.Do(func() {
		if ob.cancel != nil {
			ob.cancel()
		}
		ob.wg.Wait()

		ob.loadingDispatcher.Stop()
		ob.loadedDispatcher.Stop()
	})
}

func (ob *TargetObserver) schedule(ctx context.Context) {
	log.Info("Start update next target loop")

	interval := params.Params.QueryCoordCfg.UpdateNextTargetInterval.GetAsDuration(time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("Close target observer")
			return

		case <-ob.initChan:
			for _, collectionID := range ob.meta.GetAll(ctx) {
				ob.init(ctx, collectionID)
			}
			log.Info("target observer init done")

		case <-ticker.C:
			ob.clean()

			collections := ob.meta.GetAllCollections(ctx)
			var loadedIDs, loadingIDs []int64
			for _, c := range collections {
				if c.GetStatus() == querypb.LoadStatus_Loaded {
					loadedIDs = append(loadedIDs, c.GetCollectionID())
				} else {
					loadingIDs = append(loadingIDs, c.GetCollectionID())
				}
			}

			ob.loadedDispatcher.AddTask(loadedIDs...)
			ob.loadingDispatcher.AddTask(loadingIDs...)

			// apply dynamic update only when changed
			newInterval := params.Params.QueryCoordCfg.UpdateNextTargetInterval.GetAsDuration(time.Second)
			if newInterval != interval {
				interval = newInterval
				select {
				case <-ticker.C:
				default:
				}
				ticker.Reset(interval)
			}

		case req := <-ob.updateChan:
			log.Info("manually trigger update target",
				zap.Int64("collectionID", req.CollectionID),
				zap.String("opType", req.opType.String()),
			)
			switch req.opType {
			case UpdateCollection:
				ob.keylocks.Lock(req.CollectionID)
				err := ob.updateNextTarget(ctx, req.CollectionID)
				ob.keylocks.Unlock(req.CollectionID)
				if err != nil {
					log.Warn("failed to manually update next target",
						zap.Int64("collectionID", req.CollectionID),
						zap.String("opType", req.opType.String()),
						zap.Error(err))
					close(req.ReadyNotifier)
				} else {
					ob.mut.Lock()
					ob.readyNotifiers[req.CollectionID] = append(ob.readyNotifiers[req.CollectionID], req.ReadyNotifier)
					ob.mut.Unlock()
				}
				req.Notifier <- err
			case ReleaseCollection:
				ob.mut.Lock()
				for _, notifier := range ob.readyNotifiers[req.CollectionID] {
					close(notifier)
				}
				delete(ob.readyNotifiers, req.CollectionID)
				ob.mut.Unlock()

				ob.keylocks.Lock(req.CollectionID)
				ob.targetMgr.RemoveCollection(ctx, req.CollectionID)
				ob.keylocks.Unlock(req.CollectionID)
				req.Notifier <- nil
			case ReleasePartition:
				ob.keylocks.Lock(req.CollectionID)
				ob.targetMgr.RemovePartitionFromNextTarget(ctx, req.CollectionID, req.PartitionIDs...)
				ob.keylocks.Unlock(req.CollectionID)
				req.Notifier <- nil
			case UpdatePartition:
				// Fast path: check with read lock first
				ob.keylocks.RLock(req.CollectionID)
				exists := ob.targetMgr.IsCurrentTargetExist(ctx, req.CollectionID, req.PartitionIDs[0])
				ob.keylocks.RUnlock(req.CollectionID)

				if exists {
					close(req.ReadyNotifier)
					req.Notifier <- nil
				} else {
					// Slow path: need to update next target
					ob.keylocks.Lock(req.CollectionID)
					// Double check after acquiring write lock
					if ob.targetMgr.IsCurrentTargetExist(ctx, req.CollectionID, req.PartitionIDs[0]) {
						close(req.ReadyNotifier)
						req.Notifier <- nil
					} else {
						err := ob.updateNextTarget(ctx, req.CollectionID)
						if err != nil {
							log.Warn("failed to manually update next target",
								zap.Int64("collectionID", req.CollectionID),
								zap.String("opType", req.opType.String()),
								zap.Error(err))
							close(req.ReadyNotifier)
						} else {
							ob.mut.Lock()
							ob.readyNotifiers[req.CollectionID] = append(ob.readyNotifiers[req.CollectionID], req.ReadyNotifier)
							ob.mut.Unlock()
						}
						req.Notifier <- err
					}
					ob.keylocks.Unlock(req.CollectionID)
				}
			}
			log.Info("manually trigger update target done",
				zap.Int64("collectionID", req.CollectionID),
				zap.String("opType", req.opType.String()))
		}
	}
}

// Check whether provided collection is has current target.
// If not, submit an async task into dispatcher.
func (ob *TargetObserver) Check(ctx context.Context, collectionID int64, partitionID int64) bool {
	result := ob.targetMgr.IsCurrentTargetExist(ctx, collectionID, partitionID)
	if !result {
		ob.loadingDispatcher.AddTask(collectionID)
	}
	return result
}

func (ob *TargetObserver) TriggerUpdateCurrentTarget(collectionID int64) {
	ob.loadingDispatcher.AddTask(collectionID)
}

func (ob *TargetObserver) check(ctx context.Context, collectionID int64) {
	ob.keylocks.Lock(collectionID)
	defer ob.keylocks.Unlock(collectionID)

	// if collection release, skip check
	if ob.meta.GetCollection(ctx, collectionID) == nil {
		return
	}

	if ob.shouldUpdateCurrentTarget(ctx, collectionID) {
		ob.updateCurrentTarget(ctx, collectionID)
	}

	if ob.shouldUpdateNextTarget(ctx, collectionID) {
		// update next target in collection level
		ob.updateNextTarget(ctx, collectionID)

		// sync next target to delegator if current target not exist, to support partial search
		if !ob.targetMgr.IsCurrentTargetExist(ctx, collectionID, -1) {
			newVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
			ob.syncNextTargetToDelegator(ctx, collectionID, ob.distMgr.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(collectionID)), newVersion)
		}
	}

	// Update the all-replicas checkpoint metric
	ob.updateAllReplicasCheckpointMetric(ctx, collectionID)
}

func (ob *TargetObserver) init(ctx context.Context, collectionID int64) {
	// pull next target first if not exist
	if !ob.targetMgr.IsNextTargetExist(ctx, collectionID) {
		ob.updateNextTarget(ctx, collectionID)
	}

	// try to update current target if all segment/channel are ready
	if ob.shouldUpdateCurrentTarget(ctx, collectionID) {
		ob.updateCurrentTarget(ctx, collectionID)
	}
	// refresh collection loading status upon restart
	ob.check(ctx, collectionID)
}

// UpdateNextTarget updates the next target,
// returns a channel which will be closed when the next target is ready,
// or returns error if failed to pull target
func (ob *TargetObserver) UpdateNextTarget(collectionID int64) (chan struct{}, error) {
	notifier := make(chan error)
	readyCh := make(chan struct{})
	defer close(notifier)

	ob.updateChan <- targetUpdateRequest{
		CollectionID:  collectionID,
		opType:        UpdateCollection,
		Notifier:      notifier,
		ReadyNotifier: readyCh,
	}
	return readyCh, <-notifier
}

func (ob *TargetObserver) UpdatePartition(collectionID int64, partitionID int64) (chan struct{}, error) {
	notifier := make(chan error)
	readyCh := make(chan struct{})
	defer close(notifier)
	ob.updateChan <- targetUpdateRequest{
		CollectionID:  collectionID,
		PartitionIDs:  []int64{partitionID},
		opType:        UpdatePartition,
		Notifier:      notifier,
		ReadyNotifier: readyCh,
	}
	return readyCh, <-notifier
}

func (ob *TargetObserver) ReleaseCollection(collectionID int64) {
	notifier := make(chan error)
	defer close(notifier)
	ob.updateChan <- targetUpdateRequest{
		CollectionID: collectionID,
		opType:       ReleaseCollection,
		Notifier:     notifier,
	}
	<-notifier
}

func (ob *TargetObserver) ReleasePartition(collectionID int64, partitionID ...int64) {
	notifier := make(chan error)
	defer close(notifier)
	ob.updateChan <- targetUpdateRequest{
		CollectionID: collectionID,
		PartitionIDs: partitionID,
		opType:       ReleasePartition,
		Notifier:     notifier,
	}
	<-notifier
}

func (ob *TargetObserver) clean() {
	collectionSet := typeutil.NewUniqueSet(ob.meta.GetAll(context.TODO())...)
	// for collection which has been removed from target, try to clear nextTargetLastUpdate
	ob.nextTargetLastUpdate.Range(func(collectionID int64, _ time.Time) bool {
		if !collectionSet.Contain(collectionID) {
			ob.nextTargetLastUpdate.Remove(collectionID)
		}
		return true
	})

	ob.mut.Lock()
	defer ob.mut.Unlock()
	for collectionID, notifiers := range ob.readyNotifiers {
		if !collectionSet.Contain(collectionID) {
			for i := range notifiers {
				close(notifiers[i])
			}
			delete(ob.readyNotifiers, collectionID)
		}
	}
}

func (ob *TargetObserver) shouldUpdateNextTarget(ctx context.Context, collectionID int64) bool {
	return !ob.targetMgr.IsNextTargetExist(ctx, collectionID) || ob.isNextTargetExpired(collectionID)
}

func (ob *TargetObserver) isNextTargetExpired(collectionID int64) bool {
	lastUpdated, has := ob.nextTargetLastUpdate.Get(collectionID)
	if !has {
		return true
	}
	return time.Since(lastUpdated) > params.Params.QueryCoordCfg.NextTargetSurviveTime.GetAsDuration(time.Second)
}

func (ob *TargetObserver) updateNextTarget(ctx context.Context, collectionID int64) error {
	log := log.Ctx(context.TODO()).WithRateGroup("qcv2.TargetObserver", 1, 60).
		With(zap.Int64("collectionID", collectionID))

	log.RatedInfo(10, "observer trigger update next target")
	oldNextVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	if oldNextVersion > 0 {
		if !ob.reconcileDelegatorsBeforeNextTargetUpdate(ctx, collectionID, oldNextVersion) {
			return merr.WrapErrServiceInternal("failed to reconcile old next target delegators before updating next target")
		}
	}
	err := ob.targetMgr.UpdateCollectionNextTarget(ctx, collectionID)
	if err != nil {
		log.Warn("failed to update next target for collection",
			zap.Error(err))
		return err
	}
	ob.updateNextTargetTimestamp(collectionID)
	return nil
}

func (ob *TargetObserver) updateNextTargetTimestamp(collectionID int64) {
	ob.nextTargetLastUpdate.Insert(collectionID, time.Now())
}

func (ob *TargetObserver) shouldUpdateCurrentTarget(ctx context.Context, collectionID int64) bool {
	replicaNum := ob.meta.GetReplicaNumber(ctx, collectionID)
	log := log.Ctx(ctx).WithRateGroup(
		fmt.Sprintf("qcv2.TargetObserver-shouldUpdateCurrentTarget-%d", collectionID),
		10,
		60,
	).With(
		zap.Int64("collectionID", collectionID),
		zap.Int32("replicaNum", replicaNum),
	)

	// check channel first
	channelNames := ob.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)
	if len(channelNames) == 0 {
		// next target is empty, no need to update
		log.RatedInfo(10, "next target is empty, no need to update")
		return false
	}

	newVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)

	replicas := ob.meta.GetByCollection(ctx, collectionID)
	if len(replicas) == 0 {
		return false
	}

	allReplicaChannelsSynced := true
	for _, replica := range replicas {
		for channel := range channelNames {
			delegatorList := ob.distMgr.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica), meta.WithChannelName2Channel(channel))
			channelSynced := false
			for _, delegator := range delegatorList {
				if ob.delegatorSyncedToTarget(delegator, newVersion) {
					channelSynced = true
					continue
				}
				if !ob.checkDelegatorDataReadyForTarget(ctx, log, replica, delegator, newVersion, meta.NextTarget) {
					continue
				}
				if ob.syncTargetToDelegator(ctx, collectionID, delegator, newVersion, meta.NextTarget) {
					channelSynced = true
				}
			}
			if !channelSynced {
				allReplicaChannelsSynced = false
			}
		}
	}

	// segment data satisfies next target spec
	segmentDataReady := !paramtable.Get().QueryCoordCfg.UpdateTargetNeedSegmentDataReady.GetAsBool() ||
		utils.CheckSegmentDataReady(ctx, collectionID, ob.distMgr, ob.targetMgr, meta.NextTarget) == nil

	return allReplicaChannelsSynced && segmentDataReady
}

// sync next target info to delegator as readable snapshot
// 1. if next target is changed before delegator becomes serviceable, we need to sync the new next target to delegator to support partial search
// 2. if next target is ready to read, we need to sync the next target to delegator to support full search
func (ob *TargetObserver) syncNextTargetToDelegator(ctx context.Context, collectionID int64, collReadyDelegatorList []*meta.DmChannel, newVersion int64) bool {
	return ob.syncTargetToDelegators(ctx, collectionID, collReadyDelegatorList, newVersion, meta.NextTarget)
}

func (ob *TargetObserver) syncTargetToDelegators(ctx context.Context, collectionID int64, collReadyDelegatorList []*meta.DmChannel, targetVersion int64, scope meta.TargetScope) bool {
	for _, d := range collReadyDelegatorList {
		if !ob.syncTargetToDelegator(ctx, collectionID, d, targetVersion, scope) {
			return false
		}
	}
	return true
}

func (ob *TargetObserver) syncTargetToDelegator(ctx context.Context, collectionID int64, d *meta.DmChannel, targetVersion int64, scope meta.TargetScope) bool {
	var partitions []int64
	var indexInfo []*indexpb.IndexInfo
	var err error
	if d == nil || d.View == nil {
		return false
	}
	updateVersionAction := ob.genSyncActionForScope(ctx, d.View, targetVersion, scope)
	replica := ob.meta.GetByCollectionAndNode(ctx, collectionID, d.Node)
	if replica == nil {
		log.Warn("replica not found", zap.Int64("nodeID", d.Node), zap.Int64("collectionID", collectionID))
		return false
	}
	partitions, err = ob.targetMgr.GetPartitions(ctx, collectionID, targetFirstScope(scope))
	if err != nil {
		log.Warn("failed to get partitions", zap.Error(err))
		return false
	}

	indexInfo, err = ob.broker.ListIndexes(ctx, collectionID)
	if err != nil {
		log.Warn("fail to get index info of collection", zap.Error(err))
		return false
	}

	return ob.syncToDelegator(ctx, replica, d.View, updateVersionAction, partitions, indexInfo)
}

func (ob *TargetObserver) syncToDelegator(ctx context.Context, replica *meta.Replica, LeaderView *meta.LeaderView, action *querypb.SyncAction,
	partitions []int64, indexInfo []*indexpb.IndexInfo,
) bool {
	replicaID := replica.GetID()

	log := log.With(
		zap.Int64("leaderID", LeaderView.ID),
		zap.Int64("collectionID", LeaderView.CollectionID),
		zap.String("channel", LeaderView.Channel),
	)

	req := &querypb.SyncDistributionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SyncDistribution),
		),
		CollectionID: LeaderView.CollectionID,
		ReplicaID:    replicaID,
		Channel:      LeaderView.Channel,
		Actions:      []*querypb.SyncAction{action},
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:      ob.meta.GetLoadType(ctx, LeaderView.CollectionID),
			CollectionID:  LeaderView.CollectionID,
			PartitionIDs:  partitions,
			ResourceGroup: replica.GetResourceGroup(),
		},
		Version:       time.Now().UnixNano(),
		IndexInfoList: indexInfo,
	}
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	resp, err := ob.cluster.SyncDistribution(ctx, LeaderView.ID, req)
	if err != nil {
		log.Warn("failed to sync distribution", zap.Error(err))
		return false
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to sync distribution", zap.String("reason", resp.GetReason()))
		return false
	}

	return true
}

// sync next target info to delegator
// 1. if next target is changed before delegator becomes serviceable, we need to sync the new next target to delegator to support partial search
// 2. if next target is ready to read, we need to sync the next target to delegator to support full search
func (ob *TargetObserver) genSyncAction(ctx context.Context, leaderView *meta.LeaderView, targetVersion int64) *querypb.SyncAction {
	return ob.genSyncActionForScope(ctx, leaderView, targetVersion, meta.NextTarget)
}

func (ob *TargetObserver) genSyncActionForScope(ctx context.Context, leaderView *meta.LeaderView, targetVersion int64, scope meta.TargetScope) *querypb.SyncAction {
	log.Ctx(ctx).WithRateGroup("qcv2.LeaderObserver", 1, 60).
		RatedInfo(10, "Update readable segment version",
			zap.Int64("collectionID", leaderView.CollectionID),
			zap.String("channelName", leaderView.Channel),
			zap.Int64("nodeID", leaderView.ID),
			zap.Int64("oldVersion", leaderView.TargetVersion),
			zap.Int64("newVersion", targetVersion),
			zap.Int32("targetScope", scope),
		)

	sealedSegments := ob.targetMgr.GetSealedSegmentsByChannel(ctx, leaderView.CollectionID, leaderView.Channel, scope)
	growingSegments := ob.targetMgr.GetGrowingSegmentsByChannel(ctx, leaderView.CollectionID, leaderView.Channel, scope)
	droppedSegments := ob.targetMgr.GetDroppedSegmentsByChannel(ctx, leaderView.CollectionID, leaderView.Channel, scope)
	channel := ob.targetMgr.GetDmChannel(ctx, leaderView.CollectionID, leaderView.Channel, targetFirstScope(scope))
	sealedSegmentRowCount := lo.MapValues(sealedSegments, func(segment *datapb.SegmentInfo, _ int64) int64 {
		return segment.GetNumOfRows()
	})

	action := &querypb.SyncAction{
		Type:                  querypb.SyncType_UpdateVersion,
		GrowingInTarget:       growingSegments.Collect(),
		SealedInTarget:        lo.Keys(sealedSegmentRowCount),
		DroppedInTarget:       droppedSegments,
		TargetVersion:         targetVersion,
		SealedSegmentRowCount: sealedSegmentRowCount,
	}

	if channel != nil {
		action.Checkpoint = channel.GetSeekPosition()
		// used to clean delete buffer in delegator, cause delete record before this ts already be dispatch to sealed segments
		action.DeleteCP = channel.GetDeleteCheckpoint()
	}

	return action
}

func targetFirstScope(scope meta.TargetScope) meta.TargetScope {
	if scope == meta.CurrentTarget {
		return meta.CurrentTargetFirst
	}
	return meta.NextTargetFirst
}

func (ob *TargetObserver) delegatorSyncedToTarget(delegator *meta.DmChannel, targetVersion int64) bool {
	return delegator != nil &&
		delegator.View != nil &&
		delegator.View.TargetVersion == targetVersion &&
		delegator.IsServiceable()
}

func (ob *TargetObserver) checkDelegatorDataReadyForTarget(
	ctx context.Context,
	logger *log.MLogger,
	replica *meta.Replica,
	delegator *meta.DmChannel,
	targetVersion int64,
	scope meta.TargetScope,
) bool {
	if delegator == nil || delegator.View == nil {
		return false
	}
	err := utils.CheckDelegatorDataReady(ob.nodeMgr, ob.targetMgr, delegator.View, scope)
	if err == nil {
		err = ob.checkDelegatorSegmentDataReadyForTarget(ctx, delegator, scope)
	}
	dataReady := err == nil
	if !dataReady {
		logger.Info("check delegator",
			zap.Int64("collectionID", delegator.CollectionID),
			zap.Int64("replicaID", replica.GetID()),
			zap.Int64("nodeID", delegator.Node),
			zap.String("channelName", delegator.GetChannelName()),
			zap.Int64("targetVersion", delegator.View.TargetVersion),
			zap.Int64("newTargetVersion", targetVersion),
			zap.Int32("targetScope", scope),
			zap.Bool("isServiceable", delegator.IsServiceable()),
			zap.Int64("version", delegator.Version),
			zap.Error(err),
		)
	}
	return dataReady
}

func (ob *TargetObserver) checkDelegatorSegmentDataReadyForTarget(ctx context.Context, delegator *meta.DmChannel, scope meta.TargetScope) error {
	targetSegments := ob.targetMgr.GetSealedSegmentsByChannel(ctx, delegator.CollectionID, delegator.GetChannelName(), scope)
	for segmentID, targetSegment := range targetSegments {
		segmentDist, ok := delegator.View.Segments[segmentID]
		if !ok {
			return merr.WrapErrServiceInternalMsg("delegator lacks segment %d", segmentID)
		}
		segments := ob.distMgr.SegmentDistManager.GetByFilter(
			meta.WithCollectionID(delegator.CollectionID),
			meta.WithSegmentID(segmentID),
			meta.WithNodeID(segmentDist.GetNodeID()),
		)
		if len(segments) == 0 {
			return merr.WrapErrServiceInternalMsg("segment %d not found in distribution on node %d", segmentID, segmentDist.GetNodeID())
		}
		segment := segments[0]
		cmp, err := packed.CompareManifestPath(segment.ManifestPath, targetSegment.GetManifestPath())
		if err != nil {
			return err
		}
		if cmp < 0 {
			return merr.WrapErrServiceInternalMsg("segment %d manifest is outdated, dist=%s target=%s", segmentID, segment.ManifestPath, targetSegment.GetManifestPath())
		}
		if segment.DataVersion != nil && *segment.DataVersion < targetSegment.GetDataVersion() {
			return merr.WrapErrServiceInternalMsg("segment %d data version is outdated, dist=%d target=%d", segmentID, *segment.DataVersion, targetSegment.GetDataVersion())
		}
	}
	return nil
}

func (ob *TargetObserver) reconcileDelegatorsBeforeNextTargetUpdate(ctx context.Context, collectionID int64, oldNextVersion int64) bool {
	if !ob.targetMgr.IsCurrentTargetExist(ctx, collectionID, -1) {
		return true
	}
	currentVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget)
	if currentVersion <= 0 || currentVersion == oldNextVersion {
		return true
	}

	delegators := ob.distMgr.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(collectionID))
	for _, delegator := range delegators {
		if delegator == nil || delegator.View == nil || delegator.View.TargetVersion != oldNextVersion {
			continue
		}
		replica := ob.meta.GetByCollectionAndNode(ctx, collectionID, delegator.Node)
		if replica == nil {
			continue
		}
		logger := log.Ctx(ctx).WithRateGroup("qcv2.TargetObserver-reconcile", 10, 60)
		if !ob.checkDelegatorDataReadyForTarget(ctx, logger, replica, delegator, currentVersion, meta.CurrentTarget) {
			logger.Info("failed to reconcile delegator before next target update",
				zap.Int64("collectionID", collectionID),
				zap.Int64("nodeID", delegator.Node),
				zap.String("channelName", delegator.GetChannelName()),
				zap.Int64("oldNextVersion", oldNextVersion),
				zap.Int64("currentVersion", currentVersion))
			return false
		}
		if !ob.syncTargetToDelegator(ctx, collectionID, delegator, currentVersion, meta.CurrentTarget) {
			logger.Info("failed to sync current target to delegator before next target update",
				zap.Int64("collectionID", collectionID),
				zap.Int64("nodeID", delegator.Node),
				zap.String("channelName", delegator.GetChannelName()),
				zap.Int64("oldNextVersion", oldNextVersion),
				zap.Int64("currentVersion", currentVersion))
			return false
		}
	}
	return true
}

func (ob *TargetObserver) updateAllReplicasCheckpointMetric(ctx context.Context, collectionID int64) {
	channels := ob.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)
	if len(channels) == 0 {
		return
	}
	currentVersion := ob.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget)
	if currentVersion == 0 {
		return
	}
	replicas := ob.meta.GetByCollection(ctx, collectionID)
	if len(replicas) == 0 {
		return
	}

	for channelName, dmlChannel := range channels {
		allReady := true
		for _, replica := range replicas {
			delegators := ob.distMgr.ChannelDistManager.GetByFilter(
				meta.WithReplica2Channel(replica),
				meta.WithChannelName2Channel(channelName),
			)
			hasReady := lo.ContainsBy(delegators, func(ch *meta.DmChannel) bool {
				return ch.View != nil &&
					ch.View.TargetVersion >= currentVersion &&
					ch.IsServiceable()
			})
			if !hasReady {
				allReady = false
				break
			}
		}
		if allReady {
			ts, _ := tsoutil.ParseTS(dmlChannel.GetSeekPosition().GetTimestamp())
			metrics.QueryCoordCurrentTargetAllReplicasCheckpointUnixSeconds.WithLabelValues(
				paramtable.GetStringNodeID(),
				channelName,
			).Set(float64(ts.Unix()))
		}
	}
}

func (ob *TargetObserver) updateCurrentTarget(ctx context.Context, collectionID int64) {
	log := log.Ctx(ctx).WithRateGroup("qcv2.TargetObserver", 1, 60)
	log.RatedInfo(10, "observer trigger update current target", zap.Int64("collectionID", collectionID))
	if ob.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID) {
		ob.mut.Lock()
		defer ob.mut.Unlock()
		notifiers := ob.readyNotifiers[collectionID]
		for _, notifier := range notifiers {
			close(notifier)
		}
		// Reuse the capacity of notifiers slice
		if notifiers != nil {
			ob.readyNotifiers[collectionID] = notifiers[:0]
		}
	}
}
