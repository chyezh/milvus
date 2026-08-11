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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDataViewRefProtectsExactImmutableView(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)

	firstVersion, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)
	firstRef, err := manager.Get(ctx, 1, qviews.FromProtoDataVersion(firstVersion))
	require.NoError(t, err)
	t.Cleanup(firstRef.Deref)

	_, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{101}})
	require.NoError(t, err)

	view := firstRef.DataView()
	require.Equal(t, int64(1), view.CollectionID())
	require.Equal(t, qviews.DataVersion{StreamingVersion: 1}, view.Version())
	require.Equal(t, []int64{100}, view.SegmentIDs("ch-1", 10))

	segmentIDs := view.SegmentIDs("ch-1", 10)
	segmentIDs[0] = 999
	require.Equal(t, []int64{100}, view.SegmentIDs("ch-1", 10))
}

func TestDataViewRefDerefIsIdempotent(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)
	domainVersion := qviews.FromProtoDataVersion(version)
	ref, err := manager.Get(ctx, 1, domainVersion)
	require.NoError(t, err)
	require.Equal(t, 1, dataViewReferenceCount(t, manager, 1, domainVersion))

	ref.Deref()
	ref.Deref()
	require.Zero(t, dataViewReferenceCount(t, manager, 1, domainVersion))
}

func TestDataViewRefLatestPublishedProtectsExactView(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)
	domainVersion := qviews.FromProtoDataVersion(version)
	ref, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	require.Equal(t, 1, dataViewReferenceCount(t, manager, 1, domainVersion))

	_, err = manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{101}})
	require.NoError(t, err)
	require.Equal(t, domainVersion, ref.DataView().Version())
	require.Equal(t, []int64{100}, ref.DataView().SegmentIDs("ch-1", 10))
}

func TestDataViewRefSurvivesTerminalCollection(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)
	domainVersion := qviews.FromProtoDataVersion(version)
	ref, err := manager.Get(ctx, 1, domainVersion)
	require.NoError(t, err)

	_, err = manager.OnDropCollection(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []int64{100}, ref.DataView().SegmentIDs("ch-1", 10))

	_, err = manager.Get(ctx, 1, domainVersion)
	requireUnavailableDataViewError(t, err)
	_, err = manager.LatestPublished(ctx, 1)
	requireUnavailableDataViewError(t, err)

	ref.Deref()
	require.Zero(t, dataViewReferenceCount(t, manager, 1, domainVersion))
}

func TestDataViewRefRejectsMissingView(t *testing.T) {
	ctx := context.Background()
	manager, _, _ := newTestDataViewManager()

	_, err := manager.Get(ctx, 1, qviews.DataVersion{StreamingVersion: 1})
	requireUnavailableDataViewError(t, err)
	_, err = manager.LatestPublished(ctx, 1)
	requireUnavailableDataViewError(t, err)
}

func dataViewReferenceCount(
	t *testing.T,
	manager *dataViewManager,
	collectionID int64,
	version qviews.DataVersion,
) int {
	t.Helper()
	state := manager.getState(collectionID)
	require.NotNil(t, state)
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.refs[version]
}

func requireUnavailableDataViewError(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	require.True(t, merr.IsRetryableErr(err))
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}
