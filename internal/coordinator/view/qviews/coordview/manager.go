package coordview

import (
	"context"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/recovery"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// NewQueryViewManager creates a manage for executing coordview state machine.
func NewQueryViewManager(coordSyncer syncer.CoordSyncer, recoveryStorage recovery.RecoveryStorage) *QueryViewManager {
	el := &QueryViewManager{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		eventObserver:   newEventObservers(),
		apiRequest:      make(chan apiImplementation, 1),
		coordSyncer:     coordSyncer,
		recoveryStorage: recoveryStorage,
		replicas:        make(map[int64][]qviews.ShardID),
		shards:          make(map[qviews.ShardID]*shardViews),
	}
	go el.loop()
	return el
}

// QueryViewManager is the manager of query view.
// There's a event loop underlying to handle the events from syncer, recovery storage and balancer.
type QueryViewManager struct {
	notifier        *syncutil.AsyncTaskNotifier[struct{}]
	eventObserver   *eventObservers
	apiRequest      chan apiImplementation
	coordSyncer     syncer.CoordSyncer
	recoveryStorage recovery.RecoveryStorage
	replicas        map[int64][]qviews.ShardID // map the replica to the shards.
	shards          map[qviews.ShardID]*shardViews
}

// Apply applies the new incoming query view to the event loop.
func (e *QueryViewManager) Apply(newIncomingQV *QueryViewAtCoordBuilder) *syncutil.Future[ApplyResult] {
	api, result := newApplyAPI(newIncomingQV)
	e.apiRequest <- api
	return result
}

// AddReplicas adds the replicas to the query view manager.
func (e *QueryViewManager) AddReplicas(req AddReplicasRequest) *syncutil.Future[AddReplicasResult] {
	api, result := newAddReplicasAPI(req)
	e.apiRequest <- api
	return result
}

// ReleaseReplicas releases the replicas from the query view manager.
func (e *QueryViewManager) ReleaseReplicas(req ReleaseReplicasRequest) *syncutil.Future[ReleaseReplicasResult] {
	api, result := newReleaseReplicasAPI(req)
	e.apiRequest <- api
	return result
}

// Close closes the event loop.
func (e *QueryViewManager) Close() {
	e.notifier.Cancel()
	e.notifier.BlockUntilFinish()
}

// loop is the main loop of the event loop.
func (e *QueryViewManager) loop() {
	defer func() {
		e.notifier.Finish(struct{}{})
	}()

	for {
		select {
		case <-e.notifier.Context().Done():
			return
		case req := <-e.apiRequest:
			e.whenIncomingAPI(req)
		case evs, ok := <-e.coordSyncer.Receiver():
			if !ok {
				// syncer is closed, the event loop cannot be executed.
				return
			}
			for _, ev := range evs {
				e.eventObserver.Observe(ev)
				e.whenWorkNodeAcknowledged(ev)
			}
		case event, ok := <-e.recoveryStorage.Event():
			if !ok {
				// storage is closed, the event loop cannot be executed.
				return
			}
			e.eventObserver.Observe(event)
			e.whenRecoveryStorageDone(event)
		}
	}
}

// whenIncomingAPI is the event handler for the incoming api event.
func (e *QueryViewManager) whenIncomingAPI(api apiImplementation) {
	if err := api.Apply(e); err != nil {
		return
	}
	e.eventObserver.Register(api)
}

// whenWorkNodeAcknowledged is the event handler for the work node acknowledged event.
func (e *QueryViewManager) whenWorkNodeAcknowledged(incomingEv events.SyncerEvent) {
	ev, ok := incomingEv.(events.SyncerEventAck)
	if !ok {
		return
	}

	shard, ok := e.shards[ev.ShardID()]
	if !ok {
		// shard may be dropped.
		// Just ignore the incoming query view if shard is dropped.
		if ev.State() != qviews.QueryViewStateDropped {
			panic("There's a critical bug in the query view state machine")
		}
		return
	}
	shard.WhenWorkNodeAcknowledged(ev.AcknowledgedView)
}

// whenRecoverStorageDone is the event handler for the recovery storage done event.
func (e *QueryViewManager) whenRecoveryStorageDone(event events.RecoveryEvent) {
	shard, ok := e.shards[event.ShardID()]
	if !ok {
		panic("There's a critical bug in the query view state machine")
	}
	switch event := event.(type) {
	case events.EventRecoverySwap:
		shard.WhenSwapPreparingDone()
	case events.EventRecoverySaveNewUp:
		shard.WhenSave(event.Version)
	case events.EventRecoverySave:
		shard.WhenSave(event.Version)
	case events.EventRecoveryDelete:
		shard.WhenDelete(event.Version)
	}
}

// apply is the event handler for the new incoming query view event.
func (e *QueryViewManager) apply(newIncomingQV *QueryViewAtCoordBuilder) (*qviews.QueryViewVersion, error) {
	shard, ok := e.shards[newIncomingQV.ShardID()]
	if !ok {
		// shard may be dropped.
		// Just ignore the incoming query view if shard is dropped.
		return nil, ErrShardReleased
	}
	newVersion, err := shard.ApplyNewQueryView(context.TODO(), newIncomingQV)
	if err != nil {
		return nil, err
	}
	e.eventObserver.Observe(events.EventQVApply{
		EventBase: events.NewEventBase(newIncomingQV.ShardID()),
		Version:   *newVersion,
	})
	return newVersion, nil
}

// addShards adds the replicas to the query view manager.
func (e *QueryViewManager) addShards(req AddReplicasRequest) []*qviews.QueryViewVersion {
	result := make([]*qviews.QueryViewVersion, len(req.Shards))
	evs := make([]events.Event, 0, len(req.Shards))
	for idx, shardReq := range req.Shards {
		shard, ok := e.shards[shardReq.ShardID]
		if !ok {
			// The shard is not existed, create a new shard.
			e.shards[shardReq.ShardID] = newShardViews(e.recoveryStorage, e.coordSyncer)
			e.replicas[shardReq.ShardID.ReplicaID] = append(e.replicas[shardReq.ShardID.ReplicaID], shardReq.ShardID)
			evs = append(evs, events.EventShardJoin{
				EventBase: events.NewEventBase(shardReq.ShardID),
			})
		} else {
			// The replica has been already existed, update the up version directly.
			result[idx] = shard.LatestUpVersion()
		}
	}
	if len(evs) > 0 {
		e.eventObserver.Observe(evs...)
	}
	return result
}

// releaseReplicas releases the replicas from the query view manager.
func (e *QueryViewManager) releaseReplicas(req ReleaseReplicasRequest) []qviews.ShardID {
	unreadyShards := make([]qviews.ShardID, 0, 2*len(req.ReplicaIDs))
	evs := make([]events.Event, 0, 2*len(req.ReplicaIDs))
	for _, replicaID := range req.ReplicaIDs {
		shardIDs, ok := e.replicas[replicaID]
		if !ok {
			continue
		}
		for _, shardID := range shardIDs {
			// Request all the shard to be released.
			e.shards[shardID].RequestRelease(context.TODO())
			unreadyShards = append(unreadyShards, shardID)
			evs = append(evs, events.EventShardRequestRelease{
				EventBase: events.NewEventBase(shardID),
			})
		}
	}
	if len(evs) > 0 {
		e.eventObserver.Observe(evs...)
	}
	return unreadyShards
}
