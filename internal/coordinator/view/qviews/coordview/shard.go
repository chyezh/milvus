package coordview

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/recovery"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
)

var (
	ErrShardReleased          = errors.New("shard is on-releasing")
	ErrDataVersionTooOld      = errors.New("data version is too old")
	ErrOnPreparingViewIsReady = errors.New("on preparing view is ready")
)

// newShardViews creates a new shardViews.
func newShardViews(recovery recovery.RecoveryStorage, syncer syncer.CoordSyncer) *shardViews {
	return &shardViews{
		recovery:             recovery,
		syncer:               syncer,
		released:             false,
		onPreparingQueryView: newEmptyOnPreparingQueryView(recovery),
		latestUpVersion:      nil,
		maxQueryVersion:      make(map[int64]int64),
		queryViews:           make(map[qviews.QueryViewVersion]*queryViewAtCoord),
	}
}

// shardViews is a struct that contains all the query views of a shard which lifetime is not gone.
type shardViews struct {
	shardID  qviews.ShardID
	recovery recovery.RecoveryStorage
	syncer   syncer.CoordSyncer
	released bool // released is a flag to indicate that the shard is released.

	onPreparingQueryView *onPreparingQueryView // onPreparingQueryView is the unique preparing query view that on-the-way.

	latestUpVersion *qviews.QueryViewVersion                      // latestUpVersion is the latest version that the query view is up.
	maxQueryVersion map[int64]int64                               // maxQueryVersion is the max query version of the data version.
	queryViews      map[qviews.QueryViewVersion]*queryViewAtCoord // A map to store the query view of the shard.
}

// ApplyNewQueryView applies a new query view into the query views of the shard.
// It will replace the on-working preparing view if exists.
func (qvs *shardViews) ApplyNewQueryView(ctx context.Context, b *QueryViewAtCoordBuilder) (*qviews.QueryViewVersion, error) {
	if qvs.released {
		return nil, ErrShardReleased
	}

	// if the latest up version is not nil and the data version is too old, return error directly.
	if qvs.latestUpVersion != nil && qvs.latestUpVersion.DataVersion > b.DataVersion() {
		return nil, ErrDataVersionTooOld
	}

	// Assign a new query version for new incoming query view and make a swap.
	newQueryVersion := qvs.getMaxQueryVerion(b.DataVersion())
	newQueryView := b.WithQueryVersion(newQueryVersion).Build()
	newVersion := newQueryView.Version()
	if err := qvs.onPreparingQueryView.Swap(ctx, qvs.shardID, newQueryView); err != nil {
		return nil, err
	}
	qvs.maxQueryVersion[b.DataVersion()] = newQueryVersion
	return &newVersion, nil
}

// RequestRelease releases the shard views.
func (qvs *shardViews) RequestRelease(ctx context.Context) {
	// make a fence by released flag.
	qvs.released = true
	if qvs.onPreparingQueryView != nil {
		// request the on preparing view to be unrecoverable if it exists.
		qvs.onPreparingQueryView.Swap(ctx, qvs.shardID, nil)
	}

	for _, qv := range qvs.queryViews {
		// Make all query view at up state to be Down.
		if qv.State() == qviews.QueryViewStateUp {
			qv.DownView()
			qvs.recovery.Save(context.TODO(), qv.Proto())
		}
	}
}

// LatestUpVersion returns the latest up version of the query view.
func (qvs *shardViews) LatestUpVersion() *qviews.QueryViewVersion {
	return qvs.latestUpVersion
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
		qvs.sync(qv)
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
	qvs.sync(previousPreparing, currentPreparing)
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
		qvs.sync(qv)
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

func (qvs *shardViews) sync(qvc ...*queryViewAtCoord) {
	g := syncer.SyncGroup{}
	for _, qv := range qvc {
		for _, view := range qv.GetPendingAckViews() {
			g.AddView(view)
		}
	}
	qvs.syncer.Sync(g)
}

// getMaxQueryVersion returns the max query version of the data version.
func (qvs *shardViews) getMaxQueryVerion(dataVersion int64) int64 {
	if _, ok := qvs.maxQueryVersion[dataVersion]; ok {
		return qvs.maxQueryVersion[dataVersion] + 1
	}
	return 1
}
