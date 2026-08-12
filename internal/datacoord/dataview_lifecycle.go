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

package datacoord

import (
	"context"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type dataViewDropMarkerCatalog interface {
	MarkDataViewCollectionDropped(ctx context.Context, collectionID int64) error
	ListDroppedDataViewCollections(ctx context.Context) ([]int64, error)
	UnmarkDataViewCollectionDropped(ctx context.Context, collectionID int64) error
}

type dataViewLifecycleDataViews interface {
	dataview.ReferenceManager
	GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error
	OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error)
}

type dataViewDropFinalizer interface {
	FinalizeDropCollection(ctx context.Context, collectionID int64) error
}

func (m *dataViewLifecycle) Get(ctx context.Context, collectionID int64, version qviews.DataVersion) (dataview.DataViewRef, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	// Hold the collection lifecycle lock through acquisition so DropCollection's
	// durable terminal marker is a strict fence: either this Get owns its ref
	// first, or a marked/absent collection cannot acquire a new ref afterward.
	if state.terminal || (m.collectionExists != nil && !m.collectionExists(collectionID)) {
		return nil, dataview.NewUnavailableDataViewError(collectionID, version)
	}
	return m.dataViews.Get(ctx, collectionID, version)
}

type dataViewLifecycleState struct {
	mu       sync.Mutex
	terminal bool
}

type dataViewLifecycle struct {
	mu               sync.Mutex
	states           map[int64]*dataViewLifecycleState
	dataViews        dataViewLifecycleDataViews
	catalog          dataViewDropMarkerCatalog
	collectionExists func(int64) bool
}

func (m *dataViewLifecycle) dataViewProvider() balancer.DataViewProvider {
	provider, _ := m.dataViews.(balancer.DataViewProvider)
	return provider
}

func (m *dataViewLifecycle) DataViewSnapshot(ctx context.Context) *balancer.DataViewSnapshot {
	provider := m.dataViewProvider()
	if provider == nil {
		return balancer.NewDataViewSnapshot(0, nil, nil)
	}
	return provider.DataViewSnapshot(ctx)
}

func (m *dataViewLifecycle) DataViewSnapshotForCollections(ctx context.Context, ids map[int64]struct{}) *balancer.DataViewSnapshot {
	provider := m.dataViewProvider()
	if provider == nil {
		return balancer.NewDataViewSnapshot(0, nil, nil)
	}
	return provider.DataViewSnapshotForCollections(ctx, ids)
}

func (m *dataViewLifecycle) SegmentSnapshot(ctx context.Context, ids []int64) balancer.SegmentSnapshot {
	provider := m.dataViewProvider()
	if provider == nil {
		return nil
	}
	return provider.SegmentSnapshot(ctx, ids)
}

func (m *dataViewLifecycle) DataViewSnapshotRefForCollections(ctx context.Context, ids map[int64]struct{}) (balancer.DataViewSnapshotRef, error) {
	provider, ok := m.dataViews.(interface {
		DataViewSnapshotRefForCollections(context.Context, map[int64]struct{}) (balancer.DataViewSnapshotRef, error)
	})
	collectionIDs := make([]int64, 0)
	if ids == nil {
		m.mu.Lock()
		for id := range m.states {
			collectionIDs = append(collectionIDs, id)
		}
		m.mu.Unlock()
	} else {
		for id := range ids {
			collectionIDs = append(collectionIDs, id)
		}
	}
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })
	states := make([]*dataViewLifecycleState, 0, len(collectionIDs))
	for _, id := range collectionIDs {
		state := m.getOrCreateState(id)
		state.mu.Lock()
		if state.terminal {
			state.mu.Unlock()
			for _, held := range states {
				held.mu.Unlock()
			}
			return nil, dataview.NewUnavailableDataViewError(id, qviews.DataVersion{})
		}
		states = append(states, state)
	}
	defer func() {
		for _, state := range states {
			state.mu.Unlock()
		}
	}()
	if ok {
		return provider.DataViewSnapshotRefForCollections(ctx, ids)
	}
	base := m.dataViewProvider()
	if base == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("data view provider is not initialized")
	}
	return &legacyDataViewSnapshotRef{snapshot: base.DataViewSnapshotForCollections(ctx, ids)}, nil
}

type legacyDataViewSnapshotRef struct {
	snapshot *balancer.DataViewSnapshot
}

func (r *legacyDataViewSnapshotRef) Snapshot() *balancer.DataViewSnapshot { return r.snapshot }
func (*legacyDataViewSnapshotRef) Release()                               {}

func recoverDataViewLifecycle(
	ctx context.Context,
	catalog dataViewDropMarkerCatalog,
	dataViews dataViewLifecycleDataViews,
	collectionExists func(int64) bool,
) (*dataViewLifecycle, error) {
	manager := &dataViewLifecycle{
		states:           make(map[int64]*dataViewLifecycleState),
		dataViews:        dataViews,
		catalog:          catalog,
		collectionExists: collectionExists,
	}
	droppedCollections, err := catalog.ListDroppedDataViewCollections(ctx)
	if err != nil {
		return nil, err
	}
	for _, collectionID := range droppedCollections {
		manager.states[collectionID] = &dataViewLifecycleState{
			terminal: true,
		}
	}
	return manager, nil
}

func (m *dataViewLifecycle) getOrCreateState(collectionID int64) *dataViewLifecycleState {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[collectionID]
	if state == nil {
		state = &dataViewLifecycleState{}
		m.states[collectionID] = state
	}
	return state
}

func (m *dataViewLifecycle) GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.terminal {
		return nil
	}
	return m.dataViews.GarbageCollect(ctx, collectionID, retainLatest)
}

func (m *dataViewLifecycle) DropCollection(ctx context.Context, collectionID int64) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if err := m.catalog.MarkDataViewCollectionDropped(ctx, collectionID); err != nil {
		return err
	}
	state.terminal = true
	_, err := m.dataViews.OnDropCollection(ctx, collectionID)
	return err
}

func (m *dataViewLifecycle) FinalizeDropCollection(ctx context.Context, collectionID int64) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if finalizer, ok := m.dataViews.(dataViewDropFinalizer); ok {
		if err := finalizer.FinalizeDropCollection(ctx, collectionID); err != nil {
			return err
		}
	}
	return m.catalog.UnmarkDataViewCollectionDropped(ctx, collectionID)
}

func (m *dataViewLifecycle) IsTerminal(collectionID int64) bool {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.terminal
}
