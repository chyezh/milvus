package viewresource

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestManagerAcquireWaitsForPreparedRuntime(t *testing.T) {
	registry := NewRegistry(NoopGrowingSegmentPreparer{}, NoopBM25Provider{})
	manager := NewManager(registry)

	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		ReplicaId:    2,
		Vchannel:     "ch",
		Version: &viewpb.QueryViewVersion{
			DataVersion:  version.IntoProto(),
			QueryVersion: 3,
		},
		Settings: &viewpb.QueryViewSettings{RequiredFields: []int64{100}},
	}
	key := qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: 2, VChannel: "ch"},
		QueryViewVersion: qviews.QueryViewVersion{DataVersion: version, QueryVersion: 3},
	}

	ready := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:     key,
		Meta:    meta,
		OnReady: func() { close(ready) },
		OnUnrecoverable: func() {
			t.Error("unexpected unrecoverable callback")
		},
	})

	select {
	case <-ready:
		t.Fatal("manager reported ready before registry prepared the runtime")
	case <-time.After(50 * time.Millisecond):
	}

	observer := registry.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", version, meta.GetSettings()))
	require.NotNil(t, observer)
	t.Cleanup(observer.Close)

	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager ready callback")
	}

	dropped := make(chan struct{})
	manager.Release(snview.ReleaseResource{
		Key:       key,
		OnDropped: func() { close(dropped) },
	})

	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager dropped callback")
	}

	manager.UpdateMinDataVersion(snview.UpdateMinDataVersionResource{
		CollectionID:   1,
		VChannel:       "ch",
		MinDataVersion: qviews.DataVersion{StreamingVersion: 11},
	})
	runtime, runtimeReady, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 3},
	})
	require.Error(t, err)
	require.False(t, runtimeReady)
	require.Nil(t, runtime)
}

func TestManagerAcquireWaitsForDeleteApplyFrontier(t *testing.T) {
	registry := NewRegistry(nil, NoopBM25Provider{})
	manager := NewManager(registry)

	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		ReplicaId:    2,
		Vchannel:     "ch",
		Version: &viewpb.QueryViewVersion{
			DataVersion:  version.IntoProto(),
			QueryVersion: 3,
		},
		DeleteApplyStartAfterTimetick: 30,
	}
	key := qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: 2, VChannel: "ch"},
		QueryViewVersion: qviews.QueryViewVersion{DataVersion: version, QueryVersion: 3},
	}

	ready := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:     key,
		Meta:    meta,
		OnReady: func() { close(ready) },
		OnUnrecoverable: func() {
			t.Error("unexpected unrecoverable callback")
		},
	})

	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   20,
		BaseTransformTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})
	select {
	case <-ready:
		t.Fatal("manager reported ready before delete apply frontier caught up")
	case <-time.After(50 * time.Millisecond):
	}

	require.True(t, observer.ObserveMessage(context.Background(), newTestDeleteMessage(t, "ch", 30)))
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager ready callback")
	}
}

func TestManagerReleaseBeforeReadyCompletesAcquireAsUnrecoverable(t *testing.T) {
	registry := NewRegistry(NoopGrowingSegmentPreparer{}, NoopBM25Provider{})
	manager := NewManager(registry)

	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		ReplicaId:    2,
		Vchannel:     "ch",
		Version: &viewpb.QueryViewVersion{
			DataVersion:  version.IntoProto(),
			QueryVersion: 3,
		},
	}
	key := qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: 2, VChannel: "ch"},
		QueryViewVersion: qviews.QueryViewVersion{DataVersion: version, QueryVersion: 3},
	}

	ready := make(chan struct{})
	unrecoverable := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:             key,
		Meta:            meta,
		OnReady:         func() { close(ready) },
		OnUnrecoverable: func() { close(unrecoverable) },
	})

	dropped := make(chan struct{})
	manager.Release(snview.ReleaseResource{
		Key:       key,
		OnDropped: func() { close(dropped) },
	})
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager dropped callback")
	}
	select {
	case <-unrecoverable:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager unrecoverable callback")
	}

	observer := registry.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", version, meta.GetSettings()))
	require.NotNil(t, observer)
	t.Cleanup(observer.Close)

	select {
	case <-ready:
		t.Fatal("manager reported ready after release")
	case <-time.After(100 * time.Millisecond):
	}

	registry.ReleaseLoad(1, "ch")
	runtime, runtimeReady, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 3},
	})
	require.NoError(t, err)
	require.False(t, runtimeReady)
	require.Nil(t, runtime)
}

func TestManagerEvictWakesWaitingOldViewAsUnrecoverable(t *testing.T) {
	registry := NewRegistry(NoopGrowingSegmentPreparer{}, NoopBM25Provider{})
	manager := NewManager(registry)

	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		ReplicaId:    2,
		Vchannel:     "ch",
		Version: &viewpb.QueryViewVersion{
			DataVersion:  version.IntoProto(),
			QueryVersion: 3,
		},
	}
	key := qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: 2, VChannel: "ch"},
		QueryViewVersion: qviews.QueryViewVersion{DataVersion: version, QueryVersion: 3},
	}

	unrecoverable := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:  key,
		Meta: meta,
		OnReady: func() {
			t.Error("unexpected ready callback")
		},
		OnUnrecoverable: func() { close(unrecoverable) },
	})

	manager.UpdateMinDataVersion(snview.UpdateMinDataVersionResource{
		CollectionID:   1,
		VChannel:       "ch",
		MinDataVersion: qviews.DataVersion{StreamingVersion: 11},
	})

	select {
	case <-unrecoverable:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unrecoverable callback")
	}
}
