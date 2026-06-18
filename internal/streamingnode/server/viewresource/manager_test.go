package viewresource

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type blockedCatchupBM25Builder struct {
	runtime *BM25Runtime
}

func (b *blockedCatchupBM25Builder) BuildInitial(context.Context, LoadResourceDescriptor) (*BM25Runtime, error) {
	if b.runtime == nil {
		b.runtime = &BM25Runtime{}
	}
	return b.runtime, nil
}

func TestBM25RuntimeCatchupError(t *testing.T) {
	runtime := &BM25Runtime{}
	err := errors.New("catchup failed")
	runtime.MarkCatchupFailed(err)
	<-runtime.CatchupDone()
	require.ErrorIs(t, runtime.CatchupError(), err)

	runtime.MarkCatchupDone()
	require.ErrorIs(t, runtime.CatchupError(), err)
}

func TestManagerAcquireWaitsForWALTriggeredRuntime(t *testing.T) {
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, NoopIDFOracleRuntimeBuilder{})
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)

	observer := manager.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", version, meta.GetSettings()))
	require.NotNil(t, observer)

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
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
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager ready callback")
	}
}

func TestManagerDuplicateAlterAfterAcquireDoesNotRestoreInitRef(t *testing.T) {
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, NoopIDFOracleRuntimeBuilder{})
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)
	view := testAlterLoadConfigView(1, "ch", version, meta.GetSettings())

	observer := manager.OnAlterLoadConfig(view)
	require.NotNil(t, observer)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
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
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager ready callback")
	}

	require.Nil(t, manager.OnAlterLoadConfig(view))

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

	runtime, runtimeReady, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 3},
	})
	require.NoError(t, err)
	require.False(t, runtimeReady)
	require.Nil(t, runtime)
}

func TestManagerNewAlterAfterAcquireDoesNotRestoreInitRef(t *testing.T) {
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, NoopIDFOracleRuntimeBuilder{})
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	nextVersion := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)

	observer := manager.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", version, meta.GetSettings()))
	require.NotNil(t, observer)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	ready := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:             key,
		Meta:            meta,
		OnReady:         func() { close(ready) },
		OnUnrecoverable: func() { t.Error("unexpected unrecoverable callback") },
	})
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager ready callback")
	}

	nextObserver := manager.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", nextVersion, meta.GetSettings()))
	require.NotNil(t, nextObserver)
	t.Cleanup(nextObserver.Close)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
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

	runtime, runtimeReady, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: nextVersion, QueryVersion: 4},
	})
	require.NoError(t, err)
	require.False(t, runtimeReady)
	require.Nil(t, runtime)
}

func TestManagerAcquireWithoutWALTriggeredRuntimeIsUnrecoverable(t *testing.T) {
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, NoopIDFOracleRuntimeBuilder{})
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)

	unrecoverable := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:  key,
		Meta: meta,
		OnReady: func() {
			t.Error("unexpected ready callback")
		},
		OnUnrecoverable: func() { close(unrecoverable) },
	})

	select {
	case <-unrecoverable:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager unrecoverable callback")
	}
}

func TestManagerReleaseBeforeReadyCompletesAcquireAsUnrecoverable(t *testing.T) {
	blocking := &cancelAwareIDFOracleRuntimeBuilder{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, blocking)
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)

	observer := manager.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", version, meta.GetSettings()))
	require.NotNil(t, observer)
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight load")
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
	select {
	case <-ready:
		t.Fatal("manager reported ready after release")
	default:
	}
}

func TestManagerReleaseAndReacquireSameKeyDoesNotCompleteStaleAcquireAsReady(t *testing.T) {
	bm25 := &blockedCatchupBM25Builder{}
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, bm25)
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)
	holdMeta, holdKey := testQueryViewMetaAndKey(1, 2, "ch", version, 4)

	observer := manager.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", version, meta.GetSettings()))
	require.NotNil(t, observer)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	holdReady := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:             holdKey,
		Meta:            holdMeta,
		OnReady:         func() { close(holdReady) },
		OnUnrecoverable: func() { t.Error("unexpected hold unrecoverable callback") },
	})

	staleReady := make(chan struct{}, 1)
	staleUnrecoverable := make(chan struct{}, 1)
	manager.Acquire(snview.AcquireResource{
		Key:  key,
		Meta: meta,
		OnReady: func() {
			staleReady <- struct{}{}
		},
		OnUnrecoverable: func() {
			staleUnrecoverable <- struct{}{}
		},
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

	newReady := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:             key,
		Meta:            meta,
		OnReady:         func() { close(newReady) },
		OnUnrecoverable: func() { t.Error("unexpected reacquire unrecoverable callback") },
	})

	bm25.runtime.MarkCatchupDone()

	select {
	case <-newReady:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reacquire ready callback")
	}
	select {
	case <-holdReady:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for hold ready callback")
	}
	select {
	case <-staleReady:
		t.Fatal("stale acquire reported ready after same key was reacquired")
	default:
	}
	select {
	case <-staleUnrecoverable:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stale acquire unrecoverable callback")
	}
}

func TestManagerUnknownReleaseDoesNotDropInitRefResources(t *testing.T) {
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, NoopIDFOracleRuntimeBuilder{})
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)

	observer := manager.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", version, meta.GetSettings()))
	require.NotNil(t, observer)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
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

	runtime, runtimeReady, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 3},
	})
	require.NoError(t, err)
	require.True(t, runtimeReady)
	require.NotNil(t, runtime)
}

func TestManagerCloseRejectsNewLoadAndAcquire(t *testing.T) {
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, NoopIDFOracleRuntimeBuilder{})
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)

	manager.Close()
	require.Nil(t, manager.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", version, meta.GetSettings())))

	unrecoverable := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:  key,
		Meta: meta,
		OnReady: func() {
			t.Error("unexpected ready callback")
		},
		OnUnrecoverable: func() { close(unrecoverable) },
	})
	select {
	case <-unrecoverable:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager unrecoverable callback")
	}
}

func TestManagerClosePanicsWithQueryViewRefs(t *testing.T) {
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, NoopIDFOracleRuntimeBuilder{})
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)

	observer := manager.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", version, meta.GetSettings()))
	require.NotNil(t, observer)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
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
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager ready callback")
	}

	require.PanicsWithError(t, "query resource manager closed with 1 query view references", manager.Close)
}

func testQueryViewMetaAndKey(
	collectionID int64,
	replicaID int64,
	vchannel string,
	dataVersion qviews.DataVersion,
	queryVersion int64,
) (*viewpb.QueryViewMeta, qviews.QueryViewKey) {
	meta := &viewpb.QueryViewMeta{
		CollectionId: collectionID,
		ReplicaId:    replicaID,
		Vchannel:     vchannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  dataVersion.IntoProto(),
			QueryVersion: queryVersion,
		},
	}
	key := qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: replicaID, VChannel: vchannel},
		QueryViewVersion: qviews.QueryViewVersion{DataVersion: dataVersion, QueryVersion: queryVersion},
	}
	return meta, key
}
