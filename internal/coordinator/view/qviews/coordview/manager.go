package coordview

import (
	"context"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// NewEventLoop creates a new event loop for coordview state machine.
func NewEventLoop(coordSyncer qviews.CoordSyncer, recoveryStorage qviews.RecoveryStorage) *QueryViewManager {
	el := &QueryViewManager{
		applier:         make(chan *QueryViewAtCoordBuilder, 1),
		coordSyncer:     coordSyncer,
		recoveryStorage: recoveryStorage,
		shards:          make(map[qviews.ShardID]shardViews),
	}
	go el.loop()
	return el
}

// QueryViewManager is the manager of query view.
// There's a event loop underlying to handle the events from syncer, recovery storage and balancer.
type QueryViewManager struct {
	notifier        *syncutil.AsyncTaskNotifier[struct{}]
	applier         chan *QueryViewAtCoordBuilder
	coordSyncer     qviews.CoordSyncer
	recoveryStorage qviews.RecoveryStorage
	replicas        map[int64]qviews.ShardID // map the replica to the shards.
	shards          map[qviews.ShardID]shardViews
}

func (e *QueryViewManager) AddReplicas(replicas []qviews.ShardID) {
	for _, replica := range replicas {
		e.replicas[replica.ReplicaID] = replica
	}
}

func (e *QueryViewManager) ReleaseShard(ctx context.Context, shardID qviews.ShardID) error {
	panic("not implemented")
}

// Apply applies the new incoming query view to the event loop.
func (e *QueryViewManager) Apply(newIncomingQV *QueryViewAtCoordBuilder) {
	e.applier <- newIncomingQV
}

// Close closes the event loop.
func (e *QueryViewManager) Close() {
	e.notifier.Cancel()
	e.notifier.BlockUntilFinish()
}

// loop is the main loop of the event loop.
func (e *QueryViewManager) loop() {
	defer e.notifier.Finish(struct{}{})

	for {
		select {
		case <-e.notifier.Context().Done():
			return
		case newIncomingQV := <-e.applier:
			e.whenNewIncomingQueryView(newIncomingQV)
		case qvAtWorkNode, ok := <-e.coordSyncer.Receiver():
			if !ok {
				// syncer is closed, the event loop cannot be executed.
				return
			}
			e.whenWorkNodeAcknowledged(qvAtWorkNode)
		case event, ok := <-e.recoveryStorage.Event():
			if !ok {
				// storage is closed, the event loop cannot be executed.
				return
			}
			e.whenRecoveryStorageDone(event)
		}
	}
}

// whenNewIncomingQueryView is the event handler for the new incoming query view event.
func (e *QueryViewManager) whenNewIncomingQueryView(newIncomingQV *QueryViewAtCoordBuilder) {
	shard, ok := e.shards[newIncomingQV.ShardID()]
	if !ok {
		// shard may be dropped.
		// Just ignore the incoming query view if shard is dropped.
		return
	}
	_ = shard.ApplyNewQueryView(context.TODO(), newIncomingQV)
}

// whenWorkNodeAcknowledged is the event handler for the work node acknowledged event.
func (e *QueryViewManager) whenWorkNodeAcknowledged(incomingNodeQV qviews.QueryViewAtWorkNode) {
	shard, ok := e.shards[incomingNodeQV.ShardID()]
	if !ok {
		if incomingNodeQV.State() != qviews.QueryViewStateDropped {
			panic("There's a critical bug in the query view state machine")
		}
		return
	}
	shard.WhenWorkNodeAcknowledged(incomingNodeQV)
}

// whenRecoverStorageDone is the event handler for the recovery storage done event.
func (e *QueryViewManager) whenRecoveryStorageDone(event qviews.RecoveryEvent) {
	shard, ok := e.shards[event.GetShardID()]
	if !ok {
		panic("There's a critical bug in the query view state machine")
	}
	switch event := event.(type) {
	case qviews.RecoveryEventSwapPreparing:
		shard.WhenSwapPreparingDone()
	case qviews.RecoveryEventUpNewPreparingView:
		shard.WhenSave(event.Version)
	case qviews.RecoveryEventSave:
		shard.WhenSave(event.Version)
	case qviews.RecoveryEventDelete:
		shard.WhenDelete(event.Version)
	}
}
