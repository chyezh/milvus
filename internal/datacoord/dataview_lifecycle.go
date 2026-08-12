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
	"sync"

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type dataViewDropMarkerCatalog interface {
	MarkDataViewCollectionDropped(ctx context.Context, collectionID int64) error
	ListDroppedDataViewCollections(ctx context.Context) ([]int64, error)
	UnmarkDataViewCollectionDropped(ctx context.Context, collectionID int64) error
}

type dataViewLifecycleDataViews interface {
	dataview.ReferenceManager
	GarbageCollect(ctx context.Context, collectionID int64, protected []*viewpb.DataVersion, retainLatest int) error
	OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error)
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
	return m.dataViews.GarbageCollect(ctx, collectionID, nil, retainLatest)
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
	return m.catalog.UnmarkDataViewCollectionDropped(ctx, collectionID)
}

func (m *dataViewLifecycle) IsTerminal(collectionID int64) bool {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.terminal
}
