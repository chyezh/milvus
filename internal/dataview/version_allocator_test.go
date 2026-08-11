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
	"sort"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestAssignFlushVersionFirstAndRepeat(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	first, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, proto.Equal(&viewpb.DataVersion{StreamingVersion: 1}, first))
	require.True(t, proto.Equal(first, store.segments[100].GetSealedAtDataVersion()))
	require.Equal(t, 1, store.assignmentSaves)
	require.Equal(t, int64(1), catalog.versionStates[1].GetAllocatedStreamingVersion())

	repeated, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, proto.Equal(first, repeated))
	require.Equal(t, 1, store.assignmentSaves)
}

func TestAssignFlushVersionLostResponseRetry(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	assigned, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)

	recovered := NewManager(catalog, store)
	retried, err := recovered.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, proto.Equal(assigned, retried))
	require.Equal(t, 1, store.assignmentSaves)
}

func TestAssignFlushVersionConcurrentSegments(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	const segmentCount = 32
	for i := 0; i < segmentCount; i++ {
		segmentID := int64(100 + i)
		store.segments[segmentID] = newDataViewTestSegment(1, 10, segmentID, "ch-1", 1000)
	}

	versions := make(chan int64, segmentCount)
	errs := make(chan error, segmentCount)
	var wg sync.WaitGroup
	for i := 0; i < segmentCount; i++ {
		segmentID := int64(100 + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			version, err := manager.AssignFlushVersion(ctx, 1, segmentID)
			if err != nil {
				errs <- err
				return
			}
			versions <- version.GetStreamingVersion()
		}()
	}
	wg.Wait()
	close(errs)
	close(versions)
	for err := range errs {
		require.NoError(t, err)
	}

	got := make([]int, 0, segmentCount)
	for version := range versions {
		got = append(got, int(version))
	}
	sort.Ints(got)
	for i := 0; i < segmentCount; i++ {
		require.Equal(t, i+1, got[i])
	}
	require.Equal(t, int64(segmentCount), catalog.versionStates[1].GetAllocatedStreamingVersion())
}

func TestAssignFlushVersionStateSaveFailureRepairsOnRetry(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	catalog.saveVersionErrOnce = errors.New("state save failed")

	_, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.Error(t, err)
	require.Equal(t, int64(1), store.segments[100].GetSealedAtDataVersion().GetStreamingVersion())
	require.Nil(t, catalog.versionStates[1])
	require.Equal(t, 1, store.assignmentSaves)

	recovered := NewManager(catalog, store)
	retried, err := recovered.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), retried.GetStreamingVersion())
	require.Equal(t, int64(1), catalog.versionStates[1].GetAllocatedStreamingVersion())
	require.Equal(t, 1, store.assignmentSaves)
}

func TestAssignFlushVersionRecoveryIncludesLegacySnapshotVersion(t *testing.T) {
	ctx := context.Background()
	_, catalog, store := newTestDataViewManager()
	catalog.views = []*viewpb.DataViewOfCollection{newTestDataView(1, 6, 2)}
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	recovered, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	assigned, err := recovered.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)
	require.Equal(t, int64(7), assigned.GetStreamingVersion())
	require.Equal(t, int64(7), catalog.versionStates[1].GetAllocatedStreamingVersion())
}

func TestAssignFlushVersionRecoveryUsesMaximumPersistedAssignment(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	catalog.versionStates = map[int64]*viewpb.CollectionDataVersionState{
		1: {
			CollectionId:              1,
			AllocatedStreamingVersion: 4,
			PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 3, CompactVersion: 2},
		},
	}
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[100].SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 7}
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1000)

	assigned, err := manager.AssignFlushVersion(ctx, 1, 101)
	require.NoError(t, err)
	require.Equal(t, int64(8), assigned.GetStreamingVersion())
	require.Equal(t, int64(8), catalog.versionStates[1].GetAllocatedStreamingVersion())
	require.Equal(t, int64(3), catalog.versionStates[1].GetPublishedDataVersion().GetStreamingVersion())
	require.Equal(t, int64(2), catalog.versionStates[1].GetPublishedDataVersion().GetCompactVersion())
}
