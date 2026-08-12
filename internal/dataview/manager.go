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
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type Catalog interface {
	ListDataViews(ctx context.Context, collectionID int64) ([]*viewpb.DataViewOfCollection, error)
	DropDataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) error
	DropDataViews(ctx context.Context, collectionID int64) error
	SaveDataViewVersionState(ctx context.Context, state *viewpb.CollectionDataVersionState) error
	GetDataViewVersionState(ctx context.Context, collectionID int64) (*viewpb.CollectionDataVersionState, error)
	SavePublishedDataView(ctx context.Context, state *viewpb.CollectionDataVersionState, view *viewpb.DataViewOfCollection) error
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

func NewManager(catalog Catalog, segments SegmentStore) Manager {
	return &dataViewManager{
		catalog:  catalog,
		segments: segments,
		states:   make(map[int64]*collectionDataViewState),
		retained: make(map[int64]map[int64]int),
	}
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

func (m *dataViewManager) listStates() []*collectionDataViewState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make([]*collectionDataViewState, 0, len(m.states))
	for _, state := range m.states {
		states = append(states, state)
	}
	return states
}
