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

package dataview

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type SegmentStore interface {
	GetSegment(ctx context.Context, segID int64) *Segment
	GetSegments(ctx context.Context, segIDs []int64) []*Segment
	SelectSegments(ctx context.Context, collectionID int64) []*Segment
}

type Catalog interface {
	SaveDataView(ctx context.Context, dataView *viewpb.DataViewOfCollection) error
	ListDataViews(ctx context.Context, collectionID int64) ([]*viewpb.DataViewOfCollection, error)
	DropDataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) error
	DropDataViews(ctx context.Context, collectionID int64) error
}

type RecoveryCatalog interface {
	Catalog
	ListAllDataViews(ctx context.Context) ([]*viewpb.DataViewOfCollection, error)
	ListAllDataViewVersionStates(ctx context.Context) ([]*viewpb.CollectionDataVersionState, error)
}

type Manager interface {
	AssignFlushVersion(ctx context.Context, collectionID, segmentID int64) (*viewpb.DataVersion, error)
	CommitPublishedView(ctx context.Context, collectionID int64, assignedVersion *viewpb.DataVersion, mutation PublishedMutation) (*viewpb.DataVersion, error)
	RetryAssignedFlushPublication(ctx context.Context, collectionID, segmentID int64, assignedVersion *viewpb.DataVersion, removeOnly bool) (*viewpb.DataVersion, error)
	CommitStreamingView(ctx context.Context, collectionID int64, mutation PublishedMutation) (*viewpb.DataVersion, error)
	CommitRewrite(ctx context.Context, collectionID int64, mutation PublishedMutation) (*viewpb.DataVersion, error)
	CommitSegmentTrim(ctx context.Context, collectionID int64, resolveTargets SegmentTrimTargetResolver, finalize SegmentTrimFinalize) (*viewpb.DataVersion, error)
	InitializeCollection(ctx context.Context, initialization CollectionInitialization) (*viewpb.DataVersion, error)
	MarkCollectionTerminal(ctx context.Context, collectionID int64) error

	Get(ctx context.Context, collectionID int64, version qviews.DataVersion) (DataViewRef, error)
	LatestPublished(ctx context.Context, collectionID int64) (DataViewRef, error)
	DataViewSnapshotRefForCollections(ctx context.Context, collectionIDs map[int64]struct{}) (SnapshotRef, error)
	SegmentSnapshot(ctx context.Context, segmentIDs []int64) SegmentSnapshot
	ShardTimeTicks(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewShardTimeTick, error)
	IsSegmentReferenced(ctx context.Context, collectionID int64, segmentID int64) (bool, error)
	GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error
}

type CollectionInitialization struct {
	CollectionID int64
	VChannels    []string
}

// SegmentTrimTargetResolver returns the current target IDs while the
// collection DataView state lock is held.
type SegmentTrimTargetResolver func(context.Context) []int64

// SegmentTrimFinalize persistently applies the resolved trim scope before the
// collection DataView state lock is released.
type SegmentTrimFinalize func(context.Context) error

type collectionDataViewState struct {
	mu           sync.RWMutex
	collectionID int64

	published *viewpb.DataViewOfCollection
	dropped   bool
	refs      map[qviews.DataVersion]int

	versionState          *viewpb.CollectionDataVersionState
	versionStatePersisted bool
	versionStateRecovered bool
	persistedAllocated    int64
	publicationRecovered  bool
	pendingAssigned       map[int64]struct{}
	readyPublications     map[int64]PublishedMutation
}

type dataViewManager struct {
	mu             sync.RWMutex
	catalog        Catalog
	segments       SegmentStore
	states         map[int64]*collectionDataViewState
	recoveredAll   bool
	recoveredViews map[int64][]*viewpb.DataViewOfCollection
	retainedMu     sync.RWMutex
	retained       map[int64]map[int64]int
}

type Segment struct {
	ID                          int64
	CollectionID                int64
	PartitionID                 int64
	InsertChannel               string
	NumOfRows                   int64
	MemSize                     int64
	State                       commonpb.SegmentState
	Level                       datapb.SegmentLevel
	IsImporting                 bool
	IsInvisible                 bool
	StartPosition               *msgpb.MsgPosition
	DmlPosition                 *msgpb.MsgPosition
	CommitTimestamp             uint64
	TransformStartAfterTimetick uint64
	CreatedByCompaction         bool
	CompactionFrom              []int64
	SealedAtDataVersion         *viewpb.DataVersion
}

func (s *Segment) GetID() int64 {
	if s == nil {
		return 0
	}
	return s.ID
}

func (s *Segment) GetCollectionID() int64 {
	if s == nil {
		return 0
	}
	return s.CollectionID
}

func (s *Segment) GetPartitionID() int64 {
	if s == nil {
		return 0
	}
	return s.PartitionID
}

func (s *Segment) GetInsertChannel() string {
	if s == nil {
		return ""
	}
	return s.InsertChannel
}

func (s *Segment) GetNumOfRows() int64 {
	if s == nil {
		return 0
	}
	return s.NumOfRows
}

func (s *Segment) GetMemSize() int64 {
	if s == nil {
		return 0
	}
	return s.MemSize
}

func (s *Segment) GetState() commonpb.SegmentState {
	if s == nil {
		return commonpb.SegmentState_SegmentStateNone
	}
	return s.State
}

func (s *Segment) GetLevel() datapb.SegmentLevel {
	if s == nil {
		return datapb.SegmentLevel_Legacy
	}
	return s.Level
}

func (s *Segment) GetIsImporting() bool {
	return s != nil && s.IsImporting
}

func (s *Segment) GetIsInvisible() bool {
	return s != nil && s.IsInvisible
}

func (s *Segment) GetDmlPosition() *msgpb.MsgPosition {
	if s == nil {
		return nil
	}
	return s.DmlPosition
}

func (s *Segment) GetStartPosition() *msgpb.MsgPosition {
	if s == nil {
		return nil
	}
	return s.StartPosition
}

func (s *Segment) GetCommitTimestamp() uint64 {
	if s == nil {
		return 0
	}
	return s.CommitTimestamp
}

func (s *Segment) GetTransformStartAfterTimetick() uint64 {
	if s == nil {
		return 0
	}
	return s.TransformStartAfterTimetick
}

func (s *Segment) GetCreatedByCompaction() bool {
	return s != nil && s.CreatedByCompaction
}

func (s *Segment) GetCompactionFrom() []int64 {
	if s == nil {
		return nil
	}
	return s.CompactionFrom
}

func (s *Segment) GetSealedAtDataVersion() *viewpb.DataVersion {
	if s == nil {
		return nil
	}
	return s.SealedAtDataVersion
}

func NewManager(catalog Catalog, segments SegmentStore) Manager {
	return &dataViewManager{
		catalog:  catalog,
		segments: segments,
		states:   make(map[int64]*collectionDataViewState),
		retained: make(map[int64]map[int64]int),
	}
}

func RecoverManager(ctx context.Context, catalog RecoveryCatalog, segments SegmentStore) (Manager, error) {
	manager := NewManager(catalog, segments).(*dataViewManager)
	dataViews, err := catalog.ListAllDataViews(ctx)
	if err != nil {
		return nil, err
	}
	manager.recoverFromDataViews(dataViews)
	if publishedCatalog, ok := catalog.(publishedDataViewCatalog); ok {
		viewsByCollection := make(map[int64][]*viewpb.DataViewOfCollection)
		collectionIDs := make(map[int64]struct{})
		for _, view := range dataViews {
			if view != nil {
				viewsByCollection[view.GetCollectionId()] = append(viewsByCollection[view.GetCollectionId()], view)
				collectionIDs[view.GetCollectionId()] = struct{}{}
			}
		}
		versionStates, err := catalog.ListAllDataViewVersionStates(ctx)
		if err != nil {
			return nil, err
		}
		for _, versionState := range versionStates {
			if versionState != nil {
				collectionIDs[versionState.GetCollectionId()] = struct{}{}
			}
		}
		orderedCollectionIDs := make([]int64, 0, len(collectionIDs))
		for collectionID := range collectionIDs {
			orderedCollectionIDs = append(orderedCollectionIDs, collectionID)
		}
		sort.Slice(orderedCollectionIDs, func(i, j int) bool {
			return orderedCollectionIDs[i] < orderedCollectionIDs[j]
		})
		for _, collectionID := range orderedCollectionIDs {
			persistedViews := viewsByCollection[collectionID]
			durable, published, err := recoverPublishedDataView(ctx, publishedCatalog, collectionID)
			if err != nil {
				return nil, err
			}
			versionStatePersisted := durable != nil
			if durable == nil {
				durable = &viewpb.CollectionDataVersionState{CollectionId: collectionID}
			}
			if !versionStatePersisted && published == nil && len(persistedViews) > 0 {
				// Legacy catalogs predate the durable published head and may contain
				// a newer snapshot for an invisible sort/compaction handoff. During
				// this one-time migration, select the newest snapshot whose added
				// membership is loadable; after the head is backfilled, recovery never
				// performs this SegmentMeta-based visibility selection again.
				published = manager.latestLegacyLoadablePersistedView(ctx, persistedViews)
				if published == nil {
					return nil, merr.WrapErrDataIntegrityMsg(
						"legacy DataView snapshots for collection %d have no loadable migration head",
						collectionID,
					)
				}
				if published.GetCollectionId() != collectionID ||
					published.GetDataVersion() == nil ||
					published.GetDataVersion().GetStreamingVersion() <= 0 ||
					published.GetDataVersion().GetCompactVersion() < 0 {
					return nil, merr.WrapErrDataIntegrityMsg(
						"invalid legacy DataView migration snapshot for collection %d",
						collectionID,
					)
				}
				durable = proto.Clone(durable).(*viewpb.CollectionDataVersionState)
				durable.PublishedDataVersion = cloneDataVersion(published.GetDataVersion())
				if published.GetDataVersion().GetStreamingVersion() > durable.GetAllocatedStreamingVersion() {
					durable.AllocatedStreamingVersion = published.GetDataVersion().GetStreamingVersion()
				}
				if err := publishedCatalog.SavePublishedDataView(ctx, durable, cloneDataViewWithoutDeleteTimetick(published)); err != nil {
					return nil, merr.Wrapf(err, "backfill published DataView head for collection %d", collectionID)
				}
				versionStatePersisted = true
			}
			if published != nil {
				manager.retainRecoveredDataViewsThrough(collectionID, published.GetDataVersion())
				manager.recoverCollectionFromDataViews(collectionID, []*viewpb.DataViewOfCollection{published})
				retained, _ := manager.recoveredDataViews(collectionID)
				manager.updateRetainedMembership(collectionID, retained)
			} else if versionStatePersisted {
				manager.retainRecoveredDataViewsThrough(collectionID, nil)
				manager.updateRetainedMembership(collectionID, nil)
				manager.recoverCollectionFromDataViews(collectionID, nil)
			}
			state := manager.getOrCreateState(collectionID)
			state.mu.Lock()
			state.versionState = proto.Clone(durable).(*viewpb.CollectionDataVersionState)
			state.versionStatePersisted = versionStatePersisted
			state.persistedAllocated = durable.GetAllocatedStreamingVersion()
			state.mu.Unlock()
		}
	}
	return manager, nil
}

func (m *dataViewManager) InitializeCollection(ctx context.Context, initialization CollectionInitialization) (*viewpb.DataVersion, error) {
	state := m.getOrCreateState(initialization.CollectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped {
		return nil, nil
	}

	if state.published != nil {
		state.published = m.withDeleteTimetick(ctx, state.published)
		return dataVersionFromView(state.published), nil
	}

	if catalog, ok := m.catalog.(publishedDataViewCatalog); ok {
		durable, err := catalog.GetDataViewVersionState(ctx, initialization.CollectionID)
		if err != nil {
			return nil, err
		}
		if durable != nil {
			if err := m.recoverPublicationStateLocked(ctx, state, catalog); err != nil {
				return nil, err
			}
			if state.published != nil {
				return dataVersionFromView(state.published), nil
			}
			if durable.GetPublishedDataVersion() == nil {
				persistedViews, err := catalog.ListDataViews(ctx, initialization.CollectionID)
				if err != nil {
					return nil, err
				}
				if len(persistedViews) > 0 {
					return nil, merr.WrapErrServiceNotReadyMsg(
						"collection %d has DataView snapshots but no durable published head",
						initialization.CollectionID,
					)
				}
			}
		} else {
			persistedViews, err := catalog.ListDataViews(ctx, initialization.CollectionID)
			if err != nil {
				return nil, err
			}
			if latestPersisted := latestDataView(persistedViews); latestPersisted != nil {
				state.published = canonicalDataViewClone(latestPersisted)
				return dataVersionFromView(state.published), nil
			}
			state.versionState = &viewpb.CollectionDataVersionState{CollectionId: initialization.CollectionID}
			state.publicationRecovered = true
		}
	} else {
		persistedViews, err := m.catalog.ListDataViews(ctx, initialization.CollectionID)
		if err != nil {
			return nil, err
		}
		latestPersisted := latestDataView(persistedViews)
		if latestPersisted != nil {
			state.published = canonicalDataViewClone(latestPersisted)
			return dataVersionFromView(state.published), nil
		}
	}

	view := buildEmptyDataView(initialization.CollectionID, initialization.VChannels)
	view.DataVersion = &viewpb.DataVersion{StreamingVersion: 1}
	toPersist := cloneDataViewWithoutDeleteTimetick(view)
	if catalog, ok := m.catalog.(publishedDataViewCatalog); ok {
		if state.versionState == nil {
			state.versionState = &viewpb.CollectionDataVersionState{CollectionId: initialization.CollectionID}
		}
		if err := m.persistPublishedLocked(ctx, state, catalog, toPersist); err != nil {
			return nil, err
		}
		state.publicationRecovered = true
	} else {
		if err := m.catalog.SaveDataView(ctx, toPersist); err != nil {
			return nil, err
		}
		m.invalidateRetainedMembership(initialization.CollectionID)
		state.published = canonicalDataViewClone(toPersist)
		state.published = m.withDeleteTimetick(ctx, state.published)
	}
	return dataVersionFromView(state.published), nil
}

func (m *dataViewManager) MarkCollectionTerminal(ctx context.Context, collectionID int64) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.published = nil
	state.dropped = true
	return nil
}

func (m *dataViewManager) FinalizeDropCollection(ctx context.Context, collectionID int64) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.dropped {
		return nil
	}
	if len(state.refs) != 0 {
		return merr.WrapErrServiceNotReadyMsg("data view references for terminal collection %d have not drained", collectionID)
	}
	if err := m.catalog.DropDataViews(ctx, collectionID); err != nil {
		return err
	}
	m.updateRetainedMembership(collectionID, nil)
	return nil
}

func (m *dataViewManager) recoverFromDataViews(dataViews []*viewpb.DataViewOfCollection) {
	viewsByCollection := make(map[int64][]*viewpb.DataViewOfCollection)
	recoveredViews := make(map[int64][]*viewpb.DataViewOfCollection)
	for _, view := range dataViews {
		if view == nil {
			continue
		}
		collectionID := view.GetCollectionId()
		viewsByCollection[collectionID] = append(viewsByCollection[collectionID], view)
		recoveredViews[collectionID] = append(recoveredViews[collectionID], canonicalDataViewClone(view))
	}
	m.mu.Lock()
	m.recoveredAll = true
	m.recoveredViews = recoveredViews
	m.mu.Unlock()

	collectionIDs := make([]int64, 0, len(viewsByCollection))
	for collectionID := range viewsByCollection {
		collectionIDs = append(collectionIDs, collectionID)
	}
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })
	for _, collectionID := range collectionIDs {
		m.recoverCollectionFromDataViews(collectionID, viewsByCollection[collectionID])
		m.updateRetainedMembership(collectionID, viewsByCollection[collectionID])
	}
}

func (m *dataViewManager) recoveredDataViews(collectionID int64) ([]*viewpb.DataViewOfCollection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.recoveredAll {
		return nil, false
	}
	return cloneDataViews(m.recoveredViews[collectionID]), true
}

func (m *dataViewManager) rememberRecoveredDataView(view *viewpb.DataViewOfCollection) {
	if view == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.recoveredAll {
		return
	}
	collectionID := view.GetCollectionId()
	version := view.GetDataVersion()
	views := m.recoveredViews[collectionID]
	for idx, recovered := range views {
		if compareDataVersion(recovered.GetDataVersion(), version) == 0 {
			views[idx] = canonicalDataViewClone(view)
			m.recoveredViews[collectionID] = views
			return
		}
	}
	m.recoveredViews[collectionID] = append(views, canonicalDataViewClone(view))
}

func (m *dataViewManager) retainRecoveredDataViewsThrough(collectionID int64, head *viewpb.DataVersion) {
	m.mu.Lock()
	defer m.mu.Unlock()
	views := m.recoveredViews[collectionID]
	retained := views[:0]
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), head) <= 0 {
			retained = append(retained, view)
		}
	}
	m.recoveredViews[collectionID] = retained
}

func (m *dataViewManager) recoverCollectionFromDataViews(collectionID int64, persistedViews []*viewpb.DataViewOfCollection) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.dropped = false
	state.published = canonicalDataViewClone(latestDataView(persistedViews))
}

func (m *dataViewManager) Get(
	ctx context.Context,
	collectionID int64,
	version qviews.DataVersion,
) (DataViewRef, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.dropped {
		return nil, unavailableDataViewError(collectionID, version)
	}
	if catalog, ok := m.catalog.(publishedDataViewCatalog); ok {
		if err := m.recoverPublicationStateLocked(ctx, state, catalog); err != nil {
			return nil, err
		}
	}
	versionProto := version.IntoProto()
	if state.published == nil || compareDataVersion(versionProto, state.published.GetDataVersion()) > 0 {
		return nil, unavailableDataViewError(collectionID, version)
	}
	if compareDataVersion(state.published.GetDataVersion(), versionProto) == 0 {
		return newDataViewRef(state, newDataView(state.published)), nil
	}

	views, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), versionProto) == 0 {
			return newDataViewRef(state, newDataView(view)), nil
		}
	}
	return nil, unavailableDataViewError(collectionID, version)
}

func (m *dataViewManager) LatestPublished(ctx context.Context, collectionID int64) (DataViewRef, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.dropped {
		return nil, unavailableLatestDataViewError(collectionID)
	}
	if catalog, ok := m.catalog.(publishedDataViewCatalog); ok {
		if err := m.recoverPublicationStateLocked(ctx, state, catalog); err != nil {
			return nil, err
		}
	}
	if state.published == nil {
		return nil, unavailableLatestDataViewError(collectionID)
	}
	return newDataViewRef(state, newDataView(state.published)), nil
}

func (m *dataViewManager) snapshot(ctx context.Context, collectionIDs []int64) []*viewpb.DataViewOfCollection {
	states := make([]*collectionDataViewState, 0, len(collectionIDs))
	if len(collectionIDs) == 0 {
		states = m.listStates()
	} else {
		for _, collectionID := range collectionIDs {
			state := m.getState(collectionID)
			if state != nil {
				states = append(states, state)
			}
		}
	}
	sort.Slice(states, func(i, j int) bool { return states[i].collectionID < states[j].collectionID })

	views := make([]*viewpb.DataViewOfCollection, 0, len(states))
	for _, state := range states {
		state.mu.RLock()
		if state.dropped || state.published == nil {
			state.mu.RUnlock()
			continue
		}
		views = append(views, m.withDeleteTimetick(ctx, state.published))
		state.mu.RUnlock()
	}
	return views
}

type segmentSnapshot map[int64]*SegmentInfo

func (s segmentSnapshot) Get(segmentID int64) (*SegmentInfo, bool) {
	info, ok := s[segmentID]
	return info, ok
}

func (s segmentSnapshot) Range(yield func(segmentID int64, info *SegmentInfo) bool) {
	for segmentID, info := range s {
		if !yield(segmentID, info) {
			return
		}
	}
}

// SegmentSnapshot looks up arbitrary segment metadata without requiring the
// segments to belong to the latest visible DataViews.
func (m *dataViewManager) SegmentSnapshot(ctx context.Context, segmentIDs []int64) SegmentSnapshot {
	return newSegmentSnapshot(segmentIDs, m.getSegments(ctx, segmentIDs))
}

func (m *dataViewManager) getSegments(ctx context.Context, segmentIDs []int64) map[int64]*Segment {
	segments := make(map[int64]*Segment, len(segmentIDs))
	if len(segmentIDs) == 0 {
		return segments
	}
	for _, segment := range m.segments.GetSegments(ctx, segmentIDs) {
		if segment != nil {
			segments[segment.GetID()] = segment
		}
	}
	return segments
}

func newSegmentSnapshot(segmentIDs []int64, segmentsByID map[int64]*Segment) SegmentSnapshot {
	segments := make(segmentSnapshot, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segment := segmentsByID[segmentID]
		if segment == nil {
			continue
		}
		segments[segmentID] = &SegmentInfo{
			SegmentID:   segment.GetID(),
			PartitionID: segment.GetPartitionID(),
			MemSize:     segment.GetMemSize(),
			RowNum:      segment.GetNumOfRows(),
		}
	}
	return segments
}

// setDataViewDeleteTimeticks derives each shard's minimum transform-start
// timetick from the prefetched segments. Empty shards or any missing member use
// zero so consumers do not advance beyond unknown metadata.
func setDataViewDeleteTimeticks(views []*viewpb.DataViewOfCollection, segments map[int64]*Segment) {
	for _, view := range views {
		for _, shard := range view.GetShards() {
			minTs := uint64(math.MaxUint64)
			hasSegment := false
			missingSegment := false
			for _, partition := range shard.GetPartitions() {
				for _, segmentID := range partition.GetSegmentIds() {
					hasSegment = true
					segment := segments[segmentID]
					if segment == nil {
						missingSegment = true
						continue
					}
					if ts := segmentTransformStartAfterTimetick(segment); ts < minTs {
						minTs = ts
					}
				}
			}
			if !hasSegment || missingSegment {
				shard.TransformStartAfterTimetick = 0
			} else {
				shard.TransformStartAfterTimetick = minTs
			}
		}
	}
}

func (m *dataViewManager) ShardTimeTicks(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewShardTimeTick, error) {
	views := m.snapshot(ctx, collectionIDs)
	timeticks := make([]*viewpb.DataViewShardTimeTick, 0)
	for _, view := range views {
		timeticks = append(timeticks, dataViewTimeTicks(view)...)
	}
	return timeticks, nil
}

func (m *dataViewManager) IsSegmentReferenced(ctx context.Context, collectionID int64, segmentID int64) (bool, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	m.retainedMu.RLock()
	segments := m.retained[collectionID]
	_, referenced := segments[segmentID]
	m.retainedMu.RUnlock()
	if segments != nil {
		return referenced, nil
	}
	var publishedVersion *viewpb.DataVersion
	if catalog, ok := m.catalog.(publishedDataViewCatalog); ok {
		if err := m.recoverPublicationStateLocked(ctx, state, catalog); err != nil {
			return true, err
		}
		publishedVersion = cloneDataVersion(state.versionState.GetPublishedDataVersion())
	}
	views, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return true, err
	}
	retained := views[:0]
	for _, view := range views {
		version := view.GetDataVersion()
		_, hasLiveRef := state.refs[qviews.FromProtoDataVersion(version)]
		isPublishedHistory := !state.versionStatePersisted ||
			(publishedVersion != nil && compareDataVersion(version, publishedVersion) <= 0)
		if isPublishedHistory || hasLiveRef {
			retained = append(retained, view)
		}
	}
	m.updateRetainedMembership(collectionID, retained)
	m.retainedMu.RLock()
	_, referenced = m.retained[collectionID][segmentID]
	m.retainedMu.RUnlock()
	return referenced, nil
}

func (m *dataViewManager) GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if retainLatest < 1 {
		retainLatest = 1
	}
	views, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return err
	}
	var publishedVersion *viewpb.DataVersion
	versionStatePersisted := false
	if publishedCatalog, ok := m.catalog.(publishedDataViewCatalog); ok {
		versionState, err := publishedCatalog.GetDataViewVersionState(ctx, collectionID)
		if err != nil {
			return err
		}
		versionStatePersisted = versionState != nil
		if versionState != nil && versionState.GetPublishedDataVersion() != nil {
			publishedVersion = cloneDataVersion(versionState.GetPublishedDataVersion())
		}
	}
	sort.Slice(views, func(i, j int) bool {
		return compareDataVersion(views[i].GetDataVersion(), views[j].GetDataVersion()) > 0
	})
	protectedSet := make(map[string]struct{})
	for version, count := range state.refs {
		if count > 0 {
			protectedSet[dataVersionKey(version.IntoProto())] = struct{}{}
		}
	}
	if publishedVersion != nil {
		// The version-state record is the authority after recovery. A newer
		// snapshot can only be an unpublished orphan and must not displace the
		// published head in the retention window.
		protectedSet[dataVersionKey(publishedVersion)] = struct{}{}
	}
	foundPublished := publishedVersion == nil
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), publishedVersion) == 0 {
			foundPublished = true
			break
		}
	}
	if !foundPublished {
		return merr.WrapErrDataIntegrityMsg(
			"published data view snapshot is missing for collection %d at version %d/%d",
			collectionID,
			publishedVersion.GetStreamingVersion(),
			publishedVersion.GetCompactVersion(),
		)
	}
	authoritativeCount := 0
	for _, view := range views {
		version := view.GetDataVersion()
		isPublishedHistory := (!versionStatePersisted && publishedVersion == nil) ||
			(publishedVersion != nil && compareDataVersion(version, publishedVersion) <= 0)
		if isPublishedHistory && authoritativeCount < retainLatest {
			authoritativeCount++
			continue
		}
		if _, ok := protectedSet[dataVersionKey(version)]; ok {
			continue
		}
		if err := m.catalog.DropDataView(ctx, collectionID, version); err != nil {
			return err
		}
	}
	remaining, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return err
	}
	m.updateRetainedMembership(collectionID, remaining)
	return nil
}

func (m *dataViewManager) updateRetainedMembership(collectionID int64, views []*viewpb.DataViewOfCollection) {
	segments := make(map[int64]int)
	for _, view := range views {
		for _, partition := range dataViewPartitions(view) {
			for _, segmentID := range partition.GetSegmentIds() {
				segments[segmentID]++
			}
		}
	}
	m.retainedMu.Lock()
	m.retained[collectionID] = segments
	m.retainedMu.Unlock()
}

func (m *dataViewManager) invalidateRetainedMembership(collectionID int64) {
	m.retainedMu.Lock()
	delete(m.retained, collectionID)
	m.retainedMu.Unlock()
}

func (m *dataViewManager) getState(collectionID int64) *collectionDataViewState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.states[collectionID]
}

func (m *dataViewManager) getOrCreateState(collectionID int64) *collectionDataViewState {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.states[collectionID]
	if state == nil {
		state = &collectionDataViewState{
			collectionID:      collectionID,
			refs:              make(map[qviews.DataVersion]int),
			pendingAssigned:   make(map[int64]struct{}),
			readyPublications: make(map[int64]PublishedMutation),
		}
		m.states[collectionID] = state
	}
	return state
}

func newDataViewRef(state *collectionDataViewState, view *DataView) DataViewRef {
	version := view.Version()
	state.refs[version]++
	return &dataViewRef{
		view: view,
		release: func() {
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.refs[version] <= 1 {
				delete(state.refs, version)
				return
			}
			state.refs[version]--
		},
	}
}

func unavailableDataViewError(collectionID int64, version qviews.DataVersion) error {
	return &dataViewUnavailableError{collectionID: collectionID, version: version}
}

type dataViewUnavailableError struct {
	collectionID int64
	version      qviews.DataVersion
	latest       bool
}

func (e *dataViewUnavailableError) Error() string {
	if e.latest {
		return fmt.Sprintf("latest published data view of collection %d is no longer available", e.collectionID)
	}
	return fmt.Sprintf("data view %s of collection %d is no longer available", e.version.String(), e.collectionID)
}

func (*dataViewUnavailableError) Unwrap() error { return merr.ErrServiceNotReady }

// IsUnavailableDataViewError reports whether err means the exact requested
// DataView is absent or the collection has already become terminal. Other
// service-not-ready failures, such as a catalog outage, are not matched.
func IsUnavailableDataViewError(err error) bool {
	var target *dataViewUnavailableError
	return errors.As(err, &target)
}

// NewUnavailableDataViewError reports that an exact immutable DataView cannot
// be referenced. It is exposed for reference-manager implementations and tests;
// callers should branch with IsUnavailableDataViewError.
func NewUnavailableDataViewError(collectionID int64, version qviews.DataVersion) error {
	return unavailableDataViewError(collectionID, version)
}

func unavailableLatestDataViewError(collectionID int64) error {
	return &dataViewUnavailableError{collectionID: collectionID, latest: true}
}

func (m *dataViewManager) listStates() []*collectionDataViewState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make([]*collectionDataViewState, 0, len(m.states))
	for _, state := range m.states {
		states = append(states, state)
	}
	return states
}

func (m *dataViewManager) latestLegacyLoadablePersistedView(
	ctx context.Context,
	views []*viewpb.DataViewOfCollection,
) *viewpb.DataViewOfCollection {
	ordered := cloneDataViews(views)
	sort.Slice(ordered, func(i, j int) bool {
		return compareDataVersion(ordered[i].GetDataVersion(), ordered[j].GetDataVersion()) < 0
	})
	var latest *viewpb.DataViewOfCollection
	historicalSegments := make(map[int64]struct{})
	for _, view := range ordered {
		loadable := true
		for segmentID := range dataViewSegmentIDSet(view) {
			segment := m.segments.GetSegment(ctx, segmentID)
			_, appearedBefore := historicalSegments[segmentID]
			if !isLegacyMigratableMembership(segment, appearedBefore) {
				loadable = false
				break
			}
		}
		if loadable {
			latest = view
		}
		for segmentID := range dataViewSegmentIDSet(view) {
			historicalSegments[segmentID] = struct{}{}
		}
	}
	return m.withDeleteTimetick(ctx, latest)
}

func isLegacyMigratableMembership(segment *Segment, appearedBefore bool) bool {
	if segment == nil || segment.GetIsInvisible() || segment.GetIsImporting() || segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if segment.GetState() == commonpb.SegmentState_Flushed {
		return true
	}
	return appearedBefore && segment.GetState() == commonpb.SegmentState_Dropped
}

func (m *dataViewManager) withDeleteTimetick(ctx context.Context, view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	clone := canonicalDataViewClone(view)
	if clone == nil {
		return nil
	}
	for _, shard := range clone.GetShards() {
		minTs := uint64(math.MaxUint64)
		hasSegment := false
		for _, partition := range shard.GetPartitions() {
			for _, segmentID := range partition.GetSegmentIds() {
				hasSegment = true
				segment := m.segments.GetSegment(ctx, segmentID)
				ts := segmentTransformStartAfterTimetick(segment)
				if ts < minTs {
					minTs = ts
				}
			}
		}
		if hasSegment {
			shard.TransformStartAfterTimetick = minTs
		} else {
			shard.TransformStartAfterTimetick = 0
		}
	}
	return clone
}

func dataViewTimeTicks(view *viewpb.DataViewOfCollection) []*viewpb.DataViewShardTimeTick {
	if view == nil {
		return nil
	}
	timeticks := make([]*viewpb.DataViewShardTimeTick, 0, len(view.GetShards()))
	for _, shard := range view.GetShards() {
		timeticks = append(timeticks, &viewpb.DataViewShardTimeTick{
			Vchannel:                    shard.GetVchannel(),
			TransformStartAfterTimetick: shard.GetTransformStartAfterTimetick(),
		})
	}
	return timeticks
}

func segmentTransformStartAfterTimetick(segment *Segment) uint64 {
	if segment == nil {
		return 0
	}
	if ts := segment.GetTransformStartAfterTimetick(); ts != 0 {
		return ts
	}
	if ts := segment.GetCommitTimestamp(); ts != 0 {
		return ts
	}
	if segment.GetStartPosition() != nil {
		return segment.GetStartPosition().GetTimestamp()
	}
	return 0
}

func latestDataView(views []*viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	var latest *viewpb.DataViewOfCollection
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), latest.GetDataVersion()) > 0 {
			latest = view
		}
	}
	return canonicalDataViewClone(latest)
}

func compareDataVersion(left, right *viewpb.DataVersion) int {
	leftStreaming, leftCompact := int64(0), int64(0)
	if left != nil {
		leftStreaming = left.GetStreamingVersion()
		leftCompact = left.GetCompactVersion()
	}
	rightStreaming, rightCompact := int64(0), int64(0)
	if right != nil {
		rightStreaming = right.GetStreamingVersion()
		rightCompact = right.GetCompactVersion()
	}
	if leftStreaming != rightStreaming {
		if leftStreaming > rightStreaming {
			return 1
		}
		return -1
	}
	if leftCompact != rightCompact {
		if leftCompact > rightCompact {
			return 1
		}
		return -1
	}
	return 0
}

func removeSegmentFromDataView(view *viewpb.DataViewOfCollection, segmentID int64) bool {
	return removeSegmentsByPredicate(view, func(id int64, partitionID int64, vchannel string) bool {
		return id == segmentID
	})
}

func removeSegmentsByPredicate(view *viewpb.DataViewOfCollection, predicate func(segmentID int64, partitionID int64, vchannel string) bool) bool {
	changed := false
	for _, shard := range view.GetShards() {
		partitions := shard.Partitions[:0]
		for _, partition := range shard.GetPartitions() {
			segmentIDs := partition.SegmentIds[:0]
			for _, segmentID := range partition.GetSegmentIds() {
				if predicate(segmentID, partition.GetPartitionId(), shard.GetVchannel()) {
					changed = true
					continue
				}
				segmentIDs = append(segmentIDs, segmentID)
			}
			partition.SegmentIds = segmentIDs
			if len(partition.GetSegmentIds()) > 0 {
				partitions = append(partitions, partition)
			}
		}
		shard.Partitions = partitions
	}
	shards := view.Shards[:0]
	for _, shard := range view.GetShards() {
		if len(shard.GetPartitions()) > 0 {
			shards = append(shards, shard)
		}
	}
	view.Shards = shards
	return changed
}

func findOrCreateDataViewShard(view *viewpb.DataViewOfCollection, vchannel string) *viewpb.DataViewOfShard {
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard
		}
	}
	shard := &viewpb.DataViewOfShard{Vchannel: vchannel}
	view.Shards = append(view.Shards, shard)
	return shard
}

func findOrCreateDataViewPartition(shard *viewpb.DataViewOfShard, partitionID int64) *viewpb.DataViewOfPartition {
	for _, partition := range shard.GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return partition
		}
	}
	partition := &viewpb.DataViewOfPartition{PartitionId: partitionID}
	shard.Partitions = append(shard.Partitions, partition)
	return partition
}

func dataViewPartitions(view *viewpb.DataViewOfCollection) []*viewpb.DataViewOfPartition {
	if view == nil {
		return nil
	}
	partitions := make([]*viewpb.DataViewOfPartition, 0)
	for _, shard := range view.GetShards() {
		partitions = append(partitions, shard.GetPartitions()...)
	}
	return partitions
}

func canonicalDataViewClone(view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	if view == nil {
		return nil
	}
	clone := proto.Clone(view).(*viewpb.DataViewOfCollection)
	canonicalizeDataView(clone)
	return clone
}

func cloneDataViews(views []*viewpb.DataViewOfCollection) []*viewpb.DataViewOfCollection {
	if len(views) == 0 {
		return nil
	}
	clones := make([]*viewpb.DataViewOfCollection, 0, len(views))
	for _, view := range views {
		if view == nil {
			continue
		}
		clones = append(clones, canonicalDataViewClone(view))
	}
	return clones
}

func cloneDataVersion(version *viewpb.DataVersion) *viewpb.DataVersion {
	if version == nil {
		return nil
	}
	return proto.Clone(version).(*viewpb.DataVersion)
}

func dataVersionFromView(view *viewpb.DataViewOfCollection) *viewpb.DataVersion {
	if view == nil {
		return nil
	}
	return cloneDataVersion(view.GetDataVersion())
}

func cloneDataViewWithoutDeleteTimetick(view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	clone := canonicalDataViewClone(view)
	if clone == nil {
		return nil
	}
	for _, shard := range clone.GetShards() {
		shard.TransformStartAfterTimetick = 0
	}
	return clone
}

func canonicalizeDataView(view *viewpb.DataViewOfCollection) {
	if view == nil {
		return
	}
	sort.Slice(view.Shards, func(i, j int) bool {
		return view.Shards[i].GetVchannel() < view.Shards[j].GetVchannel()
	})
	for _, shard := range view.GetShards() {
		sort.Slice(shard.Partitions, func(i, j int) bool {
			return shard.Partitions[i].GetPartitionId() < shard.Partitions[j].GetPartitionId()
		})
		for _, partition := range shard.GetPartitions() {
			sort.Slice(partition.SegmentIds, func(i, j int) bool {
				return partition.SegmentIds[i] < partition.SegmentIds[j]
			})
			partition.SegmentIds = dedupSortedInt64s(partition.SegmentIds)
		}
	}
}

func buildEmptyDataView(collectionID int64, vchannels []string) *viewpb.DataViewOfCollection {
	view := &viewpb.DataViewOfCollection{
		CollectionId: collectionID,
		DataVersion:  &viewpb.DataVersion{},
	}
	if len(vchannels) == 0 {
		return view
	}
	seen := make(map[string]struct{}, len(vchannels))
	for _, vchannel := range vchannels {
		if _, ok := seen[vchannel]; ok {
			continue
		}
		seen[vchannel] = struct{}{}
		view.Shards = append(view.Shards, &viewpb.DataViewOfShard{Vchannel: vchannel})
	}
	canonicalizeDataView(view)
	return view
}

func dedupSortedInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return values
	}
	write := 1
	for read := 1; read < len(values); read++ {
		if values[read] == values[write-1] {
			continue
		}
		values[write] = values[read]
		write++
	}
	return values[:write]
}

func isDataViewMembershipEqual(left, right *viewpb.DataViewOfCollection) bool {
	leftClone := cloneDataViewWithoutDeleteTimetick(left)
	rightClone := cloneDataViewWithoutDeleteTimetick(right)
	if leftClone != nil {
		leftClone.DataVersion = nil
	}
	if rightClone != nil {
		rightClone.DataVersion = nil
	}
	if isDataViewEmpty(leftClone) && isDataViewEmpty(rightClone) {
		return true
	}
	return proto.Equal(leftClone, rightClone)
}

func isDataViewEmpty(view *viewpb.DataViewOfCollection) bool {
	return view == nil || len(view.GetShards()) == 0
}

func dataViewSegmentIDSet(view *viewpb.DataViewOfCollection) map[int64]struct{} {
	segments := make(map[int64]struct{})
	for _, partition := range dataViewPartitions(view) {
		for _, segmentID := range partition.GetSegmentIds() {
			segments[segmentID] = struct{}{}
		}
	}
	return segments
}

func dataViewContainsSegment(view *viewpb.DataViewOfCollection, target int64) bool {
	for _, partition := range dataViewPartitions(view) {
		for _, segmentID := range partition.GetSegmentIds() {
			if segmentID == target {
				return true
			}
		}
	}
	return false
}

func dataVersionKey(version *viewpb.DataVersion) string {
	return fmt.Sprintf("%d/%d", version.GetStreamingVersion(), version.GetCompactVersion())
}
