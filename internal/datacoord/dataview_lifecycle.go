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
)

type dataViewDropMarkerCatalog interface {
	MarkDataViewCollectionDropped(ctx context.Context, collectionID int64) error
	ListDroppedDataViewCollections(ctx context.Context) ([]int64, error)
	UnmarkDataViewCollectionDropped(ctx context.Context, collectionID int64) error
}

type dataViewLifecycleDataViews interface {
	dataview.ReferenceManager
	DataViewSnapshotRefForCollections(context.Context, map[int64]struct{}) (balancer.DataViewSnapshotRef, error)
	SegmentSnapshot(context.Context, []int64) balancer.SegmentSnapshot
	GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error
	MarkCollectionTerminal(ctx context.Context, collectionID int64) error
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
	// first, or a marked collection cannot acquire a new ref afterward. The
	// DataCoord collection cache is populated lazily and is not an existence
	// authority for newly created collections.
	if state.terminal {
		return nil, dataview.NewUnavailableDataViewError(collectionID, version)
	}
	return m.dataViews.Get(ctx, collectionID, version)
}

type dataViewLifecycleState struct {
	mu       sync.Mutex
	terminal bool
}

type dataViewLifecycle struct {
	mu        sync.Mutex
	states    map[int64]*dataViewLifecycleState
	dataViews dataViewLifecycleDataViews
	catalog   dataViewDropMarkerCatalog
}

func (m *dataViewLifecycle) SegmentSnapshot(ctx context.Context, ids []int64) balancer.SegmentSnapshot {
	return m.dataViews.SegmentSnapshot(ctx, ids)
}

func (m *dataViewLifecycle) DataViewSnapshotRefForCollections(ctx context.Context, ids map[int64]struct{}) (balancer.DataViewSnapshotRef, error) {
	collectionIDs := make([]int64, 0)
	statesByID := make(map[int64]*dataViewLifecycleState)
	holdLifecycleLock := ids == nil
	m.mu.Lock()
	if ids == nil {
		for id, state := range m.states {
			collectionIDs = append(collectionIDs, id)
			statesByID[id] = state
		}
	} else {
		for id := range ids {
			collectionIDs = append(collectionIDs, id)
			state := m.states[id]
			if state == nil {
				state = &dataViewLifecycleState{}
				m.states[id] = state
			}
			statesByID[id] = state
		}
	}
	if !holdLifecycleLock {
		m.mu.Unlock()
	}
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })
	states := make([]*dataViewLifecycleState, 0, len(collectionIDs))
	for _, id := range collectionIDs {
		state := statesByID[id]
		state.mu.Lock()
		if state.terminal {
			state.mu.Unlock()
			for _, held := range states {
				held.mu.Unlock()
			}
			if holdLifecycleLock {
				m.mu.Unlock()
			}
			return nil, dataview.NewUnavailableDataViewError(id, qviews.DataVersion{})
		}
		states = append(states, state)
	}
	defer func() {
		for _, state := range states {
			state.mu.Unlock()
		}
		if holdLifecycleLock {
			m.mu.Unlock()
		}
	}()
	return m.dataViews.DataViewSnapshotRefForCollections(ctx, ids)
}

func recoverDataViewLifecycle(
	ctx context.Context,
	catalog dataViewDropMarkerCatalog,
	dataViews dataViewLifecycleDataViews,
) (*dataViewLifecycle, error) {
	manager := &dataViewLifecycle{
		states:    make(map[int64]*dataViewLifecycleState),
		dataViews: dataViews,
		catalog:   catalog,
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
	return m.dataViews.MarkCollectionTerminal(ctx, collectionID)
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
