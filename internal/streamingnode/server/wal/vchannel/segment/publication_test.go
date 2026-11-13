package segment

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestFinalCommitWaitsForOrderedApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	view := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		SegmentId: 1, Vchannel: "v1", CheckpointTimeTick: 10,
		State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
	}, nil, runtimeConfig{lifecycle: &testSegmentLifecycle{}, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}})
	entered, release := make(chan struct{}), make(chan struct{})
	source := walevent.NewSource(7, 8, 1<<20, func(ctx context.Context, event walevent.Event) error {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return view.ApplyStoragePublished(ctx, event.Payload.(walevent.StoragePublished).Segments[0])
	})
	defer source.Close()
	view.runtime.PublishStorage = func(ctx context.Context, payload walevent.StoragePublished) error {
		_, err := source.Publish(ctx, payload)
		return err
	}
	view.mu.Lock()
	task := view.newCommitL1SegmentTaskLocked(10)
	view.mu.Unlock()
	finished := make(chan error, 1)
	go func() { finished <- task.Execute(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.False(t, view.finalCommitDone.Load())
	require.Nil(t, view.AssignmentMeta().SealedAtDataVersion)
	close(release)
	require.NoError(t, <-finished)
	require.True(t, view.finalCommitDone.Load())
	require.NotNil(t, view.AssignmentMeta().SealedAtDataVersion)
}

func TestReadOnlySegmentUsesPublishedStorageWithoutWriteTasks(t *testing.T) {
	ctx := context.Background()
	meta := &streamingpb.SegmentAssignmentMeta{
		SegmentId: 1, Vchannel: "v1", CheckpointTimeTick: 5,
		State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		Stat:  &streamingpb.SegmentAssignmentStat{},
	}
	view := NewSegmentViewFromMetaWithConfig(meta, nil, ViewConfig{ReadOnly: true, Owner: testSegmentOwner{}})
	for _, tt := range []uint64{10, 20} {
		raw := newObserveTestInsert(t, tt, []*messagespb.PartitionSegmentAssignment{newObserveTestAssignment(1, 3, 4)})
		batches, err := BuildInsertBatches(raw)
		require.NoError(t, err)
		released := false
		owner := message.NewOwnedImmutableMessage(raw, func() { released = true })
		handle := owner.Clone()
		require.True(t, view.ObserveInsert(ctx, handle, batches[1]))
		handle.Release()
		owner.Release()
		require.True(t, released, "RO retains query data, not persistence ACK handles")
	}
	require.Empty(t, view.pending.entries)
	require.Empty(t, view.pendingFlushChunks)
	require.Empty(t, view.pendingTasks)
	require.False(t, view.RequestPersistThrough(20))
	require.False(t, view.RequestPersistThroughUnderPressure(20))
	view.ResumePendingRecovery()
	before, ok := view.VisibleSnapshot("v1", qviews.DataVersion{})
	require.True(t, ok)
	require.Len(t, before.Data.InsertMessages, 2)
	published := proto.CloneOf(meta)
	published.CheckpointTimeTick = 10
	published.PersistedStorage = &streamingpb.L1SegmentPersistedStorage{ManifestPath: "published"}
	require.NoError(t, view.ApplyStoragePublished(ctx, published))
	after, ok := view.VisibleSnapshot("v1", qviews.DataVersion{})
	require.True(t, ok)
	require.Len(t, after.Data.InsertMessages, 1)
	require.EqualValues(t, 20, after.Data.InsertMessages[0].TimeTick())
	require.Equal(t, "published", after.Data.PersistedStorage.ManifestPath)
	require.EqualValues(t, 20, view.AssignmentMeta().CheckpointTimeTick, "durability cannot roll back the observation watermark")
	require.NoError(t, view.ApplyStoragePublished(ctx, published))
	require.NoError(t, view.ApplyStoragePublished(ctx, meta), "stale publication is harmless")
	wrong := proto.CloneOf(published)
	wrong.SegmentId = 2
	require.ErrorIs(t, view.ApplyStoragePublished(ctx, wrong), merr.ErrServiceInternal)
	flush := message.NewOwnedImmutableMessage(newObserveTestInsert(t, 20, nil), nil)
	flushHandle := flush.Clone()
	require.True(t, view.Flush(ctx, flushHandle))
	flushHandle.Release()
	flush.Release()
	require.False(t, view.EnsureFinalCommit(), "RO waits for the primary's final commit")
	published.CheckpointTimeTick = 20
	published.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED
	published.SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 1}
	require.NoError(t, view.ApplyStoragePublished(ctx, published))
	require.Empty(t, view.readOnlyInserts)
	require.True(t, view.EnsureFinalCommit())
	require.Empty(t, view.pendingTasks)
}

func TestReadOnlyCreateDoesNotScheduleRegistration(t *testing.T) {
	raw := message.CreateTestCreateSegmentMessage(t, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	view := NewSegmentViewFromCreateSegmentMessageWithConfig(message.MustAsImmutableCreateSegmentMessageV2(raw), nil, ViewConfig{ReadOnly: true})
	released := false
	owner := message.NewOwnedImmutableMessage(raw, func() { released = true })
	handle := message.MustAsOwnedImmutableCreateSegmentMessageV2(owner).Clone()
	require.True(t, view.ObserveCreateSegmentMessageV2(context.Background(), handle))
	handle.Release()
	owner.Release()
	require.True(t, released)
	require.Empty(t, view.pendingDataHandles)
	require.Empty(t, view.pendingTasks)
}
