package coordview

import (
	"context"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/coordview/shardview"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/recovery"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// NewQueryViewManager creates a manage for executing coordview state machine.
func NewQueryViewManager(coordSyncer syncer.CoordSyncer, recoveryStorage recovery.RecoveryStorage) *queryViewManagerImpl {
	el := &queryViewManagerImpl{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		eventObserver:   newEventObservers(),
		apiRequest:      make(chan apiImplementation, 1),
		coordSyncer:     coordSyncer,
		recoveryStorage: recoveryStorage,
		shards:          make(map[qviews.ShardID]shardview.ShardView),
	}
	go el.loop()
	return el
}

// queryViewManagerImpl is the manager of query view.
// There's a event loop underlying to handle the events from syncer, recovery storage and balancer.
type queryViewManagerImpl struct {
	notifier        *syncutil.AsyncTaskNotifier[struct{}]
	eventObserver   *eventObservers
	apiRequest      chan apiImplementation
	coordSyncer     syncer.CoordSyncer
	recoveryStorage recovery.RecoveryStorage
	shards          map[qviews.ShardID]shardview.ShardView
}

// Apply applies the new incoming query view to the event loop.
func (e *queryViewManagerImpl) Apply(newIncomingQV *QueryViewAtCoordBuilder) *syncutil.Future[ApplyResult] {
	api, result := newApplyAPI(newIncomingQV)
	e.apiRequest <- api
	return result
}

// ReleaseShard releases the shard from the query view manager.
func (e *queryViewManagerImpl) ReleaseShard(req ReleaseShardsRequest) *syncutil.Future[ReleaseShardsResult] {
	api, result := newReleaseShardsAPI(req)
	e.apiRequest <- api
	return result
}

// Close closes the event loop.
func (e *queryViewManagerImpl) Close() {
	e.notifier.Cancel()
	e.notifier.BlockUntilFinish()
}

// recover recovers the query view manager from the recovery storage.
func (e *queryViewManagerImpl) recover() error {
	allViews, err := e.recoveryStorage.List(e.notifier.Context())
	if err != nil {
		return err
	}
	shards := make(map[qviews.ShardID][]*viewpb.QueryViewOfShard, len(allViews))
	for _, view := range allViews {
		shardID := qviews.NewShardIDFromQVMeta(view.Meta)
		if _, ok := shards[shardID]; !ok {
			shards[shardID] = make([]*viewpb.QueryViewOfShard, 0, 5)
		}
		shards[shardID] = append(shards[shardID], view)
	}

	for shardID, views := range shards {
		shard := shardview.RecoverShardView(views)
		e.shards[shardID] = shard
	}
	return nil
}

// master is the main loop of the query view manager, it will listen the api request and the syncer event.
// and dispatch the operation event to the worker.
func (e *queryViewManagerImpl) loop() {
	defer func() {
		e.notifier.Finish(struct{}{})
	}()

	if err := e.recover(); err != nil {
		return
	}

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
				if err := e.whenWorkNodeAcknowledged(ev); err != nil {
					return
				}
			}
		}
	}
}

// whenIncomingAPI is the event handler for the incoming api event.
func (e *queryViewManagerImpl) whenIncomingAPI(api apiImplementation) {
	if err := api.Apply(e); err != nil {
		return
	}
	e.eventObserver.Register(api)
}

// whenWorkNodeAcknowledged is the event handler for the work node acknowledged event.
func (e *queryViewManagerImpl) whenWorkNodeAcknowledged(incomingEv events.SyncerEvent) error {
	ev, ok := incomingEv.(events.SyncerEventAck)
	if !ok {
		return nil
	}
	shard, ok := e.shards[ev.AcknowledgedView.ShardID()]
	if !ok {
		// shard may be dropped.
		// Just ignore the incoming query view if shard is dropped.
		if ev.AcknowledgedView.State() != qviews.QueryViewStateDropped {
			panic("There's a critical bug in the query view state machine")
		}
		return nil
	}
	return shard.ApplyViewFromWorkNode(e.notifier.Context(), ev.AcknowledgedView)
}

// apply is the event handler for the new incoming query view event.
func (e *queryViewManagerImpl) apply(newIncomingQV *QueryViewAtCoordBuilder) (*qviews.QueryViewVersion, error) {
	shard, ok := e.shards[newIncomingQV.ShardID()]
	if !ok {
		// create a new shard view if the shard is not exist.
		e.shards[newIncomingQV.ShardID()] = shardview.NewShardView(newIncomingQV.ShardID())
		shard = e.shards[newIncomingQV.ShardID()]
		events.Notify(events.EventShardJoin{ShardID: newIncomingQV.ShardID()})
	}

	newVersion, err := shard.ApplyNewQueryView(context.TODO(), newIncomingQV)
	if err != nil {
		return nil, err
	}
	e.eventObserver.Observe(events.EventQVApply{
		Version: *newVersion,
	})
	return newVersion, nil
}

// releaseReplicas releases the replicas from the query view manager.
func (e *queryViewManagerImpl) releaseReplicas(req ReleaseShardsRequest) []qviews.ShardID {
	unreadyShards := make([]qviews.ShardID, 0, 2*len(req.shards))
	evs := make([]events.Event, 0, 2*len(req.shards))
	for _, shardID := range req.shards {
		// Request all the shard to be released.
		e.shards[shardID].RequestRelease(e.notifier.Context())
		unreadyShards = append(unreadyShards, shardID)
		evs = append(evs, events.EventShardRequestRelease{
			ShardID: shardID,
		})
	}
	if len(evs) > 0 {
		e.eventObserver.Observe(evs...)
	}
	return unreadyShards
}
