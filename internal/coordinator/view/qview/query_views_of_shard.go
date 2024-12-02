package qview

import (
	"errors"

	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var ErrDataVersionTooOld = errors.New("data version is too old")

type QueryViewOfShardBuilder struct {
	dataVersion     int64
	dataViewOfShard *viewpb.DataViewOfShard
	queryView       *viewpb.QueryViewOfShard
}

// QueryViewsOfShard is a struct that contains all the query views of a shard which lifetime is not gone.
type QueryViewsOfShard struct {
	recovery     Recovery
	collectionID int64
	replicaID    int64
	vchannel     string
	settings     *viewpb.QueryViewSettings

	// If there's any view at state unrecoverable, then the flag setup.
	latestReadyVersion *QueryViewVersion              // latestUpVersion is the latest version that the query view is up.
	onPreparingVersion *QueryViewVersion              // There's always only zero or one preparing view globally.
	persistPendings    typeutil.Set[QueryViewVersion] // persistPendings is the versions that is waiting for persisted.
	// The persist pendings should always be atomically saved.
	syncPendings typeutil.Set[QueryViewVersion] // syncPendings is the versions that is waiting for sync-sent.
	// The sync pendings should always be atomically sent to related node.
	maxQueryVersion map[int64]int64                               // maxQueryVersion is the max query version of the data version.
	queryViews      map[QueryViewVersion]*QueryViewOfShardAtCoord // A map to store the query view of the shard.
}

// ApplyNewQueryView applies a new query view into the query views of the shard.
// It will replace the on-working preparing view if exists.
func (qvs *QueryViewsOfShard) ApplyNewQueryView(b QueryViewOfShardAtCoordBuilder) error {
	// if the latest ready version is not nil and the data version is too old, return error directly.
	if qvs.latestReadyVersion != nil && qvs.latestReadyVersion.DataVersion > b.DataVersion() {
		return ErrDataVersionTooOld
	}

	// If there's a pending preparing view, then make it unrecoverable and replace the onPreparingView.
	// No rollback if the swap failed, the new preparing view generation will be retried.
	if view := qvs.onPreparingView(); view != nil && view.State() == QueryViewStatePreparing {
		view.UnrecoverableView()
	}

	// Assign a new query version for new incoming query view and swap the old one.
	newQueryView := b.WithQueryVersion(qvs.getMaxQueryVerion(b.DataVersion())).Build()
	if err := qvs.swapPreparingView(newQueryView); err != nil {
		return err
	}
	return nil
}

// UpdateQueryViewByWorkNode updates the query view by the work node.
func (qvs *QueryViewsOfShard) UpdateQueryViewByWorkNode(w QueryViewOfShardAtWorkNode) {
	if qv, ok := qvs.queryViews[w.Version()]; ok {
		qv.ApplyViewFromWorkNode(w)
	}
}

// swap the in-mem preparing view and update the max query version of the data version.
func (qvs *QueryViewsOfShard) swapPreparingView(newPerparing *QueryViewOfShardAtCoord) error {
	oldPreparing := qvs.onPreparingView()

	// Swap the preparing view, make globally only one preparing view in recovery info.
	if err := qvs.recovery.SwapPreparing(qvs.onPreparingView(), newPerparing); err != nil {
		return err
	}
	newVersion := newPerparing.Version()
	qvs.onPreparingVersion = &newVersion
	qvs.queryViews[newVersion] = newPerparing
	qvs.maxQueryVersion[newVersion.DataVersion] = newVersion.QueryVersion
	// Two version should be sent to the worknode atomically.
	qvs.joinSyncPendings(oldPreparing.Version())
	qvs.joinSyncPendings(newVersion)
	return nil
}

// joinPersistPendings adds the version into the persist pendings.
func (qvs *QueryViewsOfShard) joinSyncPendings(version QueryViewVersion) {
	qvs.syncPendings.Insert(version)
}

// leaveSyncPendings removes the version from the sync pendings.
func (qvs *QueryViewsOfShard) leaveSyncPendings(version QueryViewVersion) {
	qvs.syncPendings.Remove(version)
}

// onPerparingView returns the preparing view of the shard.
func (qvs *QueryViewsOfShard) onPreparingView() *QueryViewOfShardAtCoord {
	if qvs.onPreparingVersion == nil {
		return nil
	}
	view, ok := qvs.queryViews[*qvs.onPreparingVersion]
	if !ok {
		panic("onPreparingVersion is not nil, but the view is not found in queryViews")
	}
	return view
}

// getMaxQueryVersion returns the max query version of the data version.
func (qvs *QueryViewsOfShard) getMaxQueryVerion(dataVersion int64) int64 {
	if _, ok := qvs.maxQueryVersion[dataVersion]; ok {
		return qvs.maxQueryVersion[dataVersion] + 1
	}
	return 1
}

// generateRecoveryUpdates generates the updates of views.
func (qvs *QueryViewsOfShard) applyTheRecoveryInfo() error {
	saves := make([]*QueryViewOfShardAtCoord, 0, len(qvs.persistPendings)+1)
	deletes := make([]*QueryViewOfShardAtCoord, 0, len(qvs.persistPendings)+1)
	for version := range qvs.persistPendings {
		qv := qvs.queryViews[version]
		if qv.State() == QueryViewStateDropped && (qv.Version().QueryVersion < qvs.maxQueryVersion[qv.Version().DataVersion] ||
			qv.Version().DataVersion < qvs.latestReadyVersion.DataVersion) {
			// When a query view is dropped, we need to persist the latest version of the related data version on coord.
			// The conditions to delete the query view are:
			// 1. When the query version is less than the max query version of the data version.
			// 2. When the latestReadyDataVersion is greater than the data version, no more query view on current data version will be generated.
			deletes = append(deletes, qv)
			continue
		}
		saves = append(saves, qv)
	}
	if err := qvs.recovery.Save(saves, deletes); err != nil {
		return err
	}
	qvs.persistPendings.Clear()
	return nil
}
