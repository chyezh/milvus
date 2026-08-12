package dataview

import (
	"context"
	"sync"

	balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type dataViewSnapshotRef struct {
	snapshot *balancerapi.DataViewSnapshot
	once     sync.Once
	release  func()
}

func (r *dataViewSnapshotRef) Snapshot() *balancerapi.DataViewSnapshot {
	if r == nil {
		return nil
	}
	return r.snapshot
}

func (r *dataViewSnapshotRef) Release() {
	if r == nil {
		return
	}
	r.once.Do(r.release)
}

func (m *dataViewManager) DataViewSnapshotRefForCollections(
	ctx context.Context,
	collectionIDs map[int64]struct{},
) (balancerapi.DataViewSnapshotRef, error) {
	states := make([]*collectionDataViewState, 0)
	if collectionIDs == nil {
		states = m.listStates()
	} else {
		for collectionID := range collectionIDs {
			if state := m.getState(collectionID); state != nil {
				states = append(states, state)
			}
		}
	}

	views := make([]*viewpb.DataViewOfCollection, 0, len(states))
	segmentIDs := make([]int64, 0)
	seenSegments := make(map[int64]struct{})
	releases := make([]DataViewRef, 0, len(states))
	for _, state := range states {
		state.mu.Lock()
		if state.dropped || state.latestVisible == nil {
			state.mu.Unlock()
			continue
		}
		view := canonicalDataViewClone(state.latestVisible)
		ref := newDataViewRef(state, newDataView(view))
		state.mu.Unlock()
		releases = append(releases, ref)
		views = append(views, view)
		for _, partition := range dataViewPartitions(view) {
			for _, segmentID := range partition.GetSegmentIds() {
				if _, ok := seenSegments[segmentID]; ok {
					continue
				}
				seenSegments[segmentID] = struct{}{}
				segmentIDs = append(segmentIDs, segmentID)
			}
		}
	}
	segments := m.getSegments(ctx, segmentIDs)
	setDataViewDeleteTimeticks(views, segments)
	result := balancerapi.NewDataViewSnapshot(0, views, newSegmentSnapshot(segmentIDs, segments))
	return &dataViewSnapshotRef{
		snapshot: result,
		release: func() {
			for _, ref := range releases {
				ref.Deref()
			}
		},
	}, nil
}

func (m *dataViewManager) DataViewSnapshotRef(ctx context.Context) (balancerapi.DataViewSnapshotRef, error) {
	return m.DataViewSnapshotRefForCollections(ctx, nil)
}
