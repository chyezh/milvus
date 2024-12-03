package coordview

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
)

var (
	ErrShardReleased          = errors.New("shard is on-releasing")
	ErrDataVersionTooOld      = errors.New("data version is too old")
	ErrOnPreparingViewIsReady = errors.New("on preparing view is ready")
)

// shardViews is a struct that contains all the query views of a shard which lifetime is not gone.
type shardViews struct {
	recovery qviews.RecoveryStorage
	syncer   qviews.CoordSyncer
	released bool // released is a flag to indicate that the shard is released.

	onPreparingQueryView *onPreparingQueryView // onPreparingQueryView is the unique preparing query view that on-the-way.

	latestUpVersion *qviews.QueryViewVersion                      // latestUpVersion is the latest version that the query view is up.
	maxQueryVersion map[int64]int64                               // maxQueryVersion is the max query version of the data version.
	queryViews      map[qviews.QueryViewVersion]*queryViewAtCoord // A map to store the query view of the shard.
}

// ApplyNewQueryView applies a new query view into the query views of the shard.
// It will replace the on-working preparing view if exists.
func (qvs *shardViews) ApplyNewQueryView(ctx context.Context, b *QueryViewAtCoordBuilder) error {
	if qvs.released {
		return ErrShardReleased
	}

	// if the latest up version is not nil and the data version is too old, return error directly.
	if qvs.latestUpVersion != nil && qvs.latestUpVersion.DataVersion > b.DataVersion() {
		return ErrDataVersionTooOld
	}

	// Assign a new query version for new incoming query view and make a swap.
	newQueryVersion := qvs.getMaxQueryVerion(b.DataVersion())
	newQueryView := b.WithQueryVersion(newQueryVersion).Build()
	if err := qvs.onPreparingQueryView.Swap(ctx, newQueryView); err != nil {
		return err
	}
	qvs.maxQueryVersion[b.DataVersion()] = newQueryVersion
	return nil
}

// WhenPersisted is called when the query view is persisted.
func (qvs *shardViews) WhenSave(version qviews.QueryViewVersion) {
	qv := qvs.queryViews[version]
	if qv == nil {
		panic("the query view is not found in shard, a critical bug in query view state machine")
	}
	switch qv.State() {
	case qviews.QueryViewStateUp:
		qvs.onPreparingQueryView.Reset()
		qvs.latestUpVersion = &version
		// TODO: make a notification to notify the balance recovery.
	case qviews.QueryViewStateDown:
		qvs.syncer.SyncQueryView(qv.Proto())
	}
}

// WhenDelete is called when the query view is deleted.
func (qvs *shardViews) WhenDelete(version qviews.QueryViewVersion) {
	qv := qvs.queryViews[version]
	if qv == nil {
		panic("the query view is not found in shard, a critical bug in query view state machine")
	}
	if qv.State() != qviews.QueryViewStateDropped {
		panic("the query view is not in dropped state")
	}
	delete(qvs.queryViews, version)
}

// WhenSwapPreparingDone is called when the preparing view is persisted.
func (qvs *shardViews) WhenSwapPreparingDone() {
	previousPreparing, currentPreparing := qvs.onPreparingQueryView.WhenPreparingPersisted()
	qvs.queryViews[currentPreparing.Version()] = currentPreparing
	qvs.syncer.SyncQueryView(previousPreparing.Proto(), currentPreparing.Proto())
}

// WhenWorkNodeAcknowledged is called when the work node acknowledged the query view.
func (qvs *shardViews) WhenWorkNodeAcknowledged(w qviews.QueryViewAtWorkNode) {
	qv, ok := qvs.queryViews[w.Version()]
	if !ok {
		if w.State() != qviews.QueryViewStateDropped {
			panic("the query view is not found in shard, a critical bug in query view state machine")
		}
		return
	}
	transition := qv.ApplyViewFromWorkNode(w)
	if transition == nil {
		return
	}
	switch transition.To {
	case qviews.QueryViewStateReady, qviews.QueryViewStateDropping:
		qvs.syncer.SyncQueryView(qv.Proto())
	case qviews.QueryViewStateUp:
		qvs.recovery.UpNewPreparingView(context.TODO(), qv.Proto())
	case qviews.QueryViewStateDropped:
		qvs.recovery.Delete(context.TODO(), qv.Proto())
	case qviews.QueryViewStateUnrecoverable:
		// TODO: make a event to notify balance recovery.
	default:
		panic("work node acknowledged should not transit to this state")
	}
}

// getMaxQueryVersion returns the max query version of the data version.
func (qvs *shardViews) getMaxQueryVerion(dataVersion int64) int64 {
	if _, ok := qvs.maxQueryVersion[dataVersion]; ok {
		return qvs.maxQueryVersion[dataVersion] + 1
	}
	return 1
}
