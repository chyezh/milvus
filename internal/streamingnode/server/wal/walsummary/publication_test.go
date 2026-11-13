package walsummary

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSummaryPublicationIsTheDurableFrontier(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManagerWithStore(t)
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10, 1, 1))
	persist(t, m)
	require.EqualValues(t, 10, m.LastAcked())
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20, 1, 2))
	m.RequestFlushThrough(20)
	require.NoError(t, m.pendingSealed[0].task.Execute(ctx))
	require.EqualValues(t, 20, m.Manifest().GetCoverage().GetEndTimeTick())
	require.EqualValues(t, 10, m.PublishedManifest().GetCoverage().GetEndTimeTick())
	require.EqualValues(t, 10, m.LastAcked(), "a later chunk cannot use the previous manifest's publication")
	failure := merr.WrapErrServiceUnavailable("publisher unavailable")
	m.cfg.Runtime.PublishStorage = func(context.Context, walevent.StoragePublished) error { return failure }
	require.ErrorIs(t, m.manifestTask.Execute(ctx), failure)
	require.EqualValues(t, 10, m.LastAcked())
	m.cfg.Runtime.PublishStorage = func(_ context.Context, event walevent.StoragePublished) error {
		require.EqualValues(t, 10, m.LastAcked())
		return m.ApplyStoragePublished(event.Summary)
	}
	require.NoError(t, m.manifestTask.Execute(ctx))
	require.EqualValues(t, 20, m.LastAcked())
	copy := m.PublishedManifest()
	copy.Coverage.EndTimeTick = 0
	require.EqualValues(t, 20, m.PublishedManifest().GetCoverage().GetEndTimeTick())
}

func TestReadOnlySummaryPublicationPreservesTailAndReaders(t *testing.T) {
	ctx := context.Background()
	primary, store := newTestManagerWithStore(t)
	ro := newTestManager(t, store, 1)
	ro.cfg.ReadOnly = true
	ro.cfg.FlushMaxBytes = 1
	for _, tt := range []uint64{10, 20} {
		msg := newTestDeleteMessage(t, "v1", tt, 1, int64(tt))
		primary.ObserveMessage(ctx, msg)
		ro.ObserveMessage(ctx, msg)
		if tt == 10 {
			persist(t, primary)
		}
	}
	ro.RequestFlushThrough(20)
	ro.Run(ctx, time.Millisecond, func() bool { return true })
	require.NoError(t, ro.GCOnce(ctx))
	require.False(t, ro.HasPendingWork())
	require.Empty(t, ro.cfg.Runtime.Scheduler.(*recordingScheduler).tasks)
	require.Len(t, ro.pending, 2)
	require.NoError(t, ro.ApplyStoragePublished(primary.PublishedManifest()))
	require.Len(t, ro.pending, 1)
	require.EqualValues(t, 20, ro.pending[0].timeTick)
	require.Equal(t, ro.pending[0].size, ro.pendingBytes)
	require.NotEmpty(t, ro.pendingTransforms)
	entries, err := ro.ReadTransformEntries(ctx, "v1", 0, 20)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	chunk := ro.chunkIndex.chunks[0]
	persist(t, primary)
	require.NoError(t, ro.ApplyStoragePublished(primary.PublishedManifest()))
	require.Same(t, chunk, ro.chunkIndex.chunks[0], "publication preserves cached chunk identity")
	require.Empty(t, ro.pending)
	require.Zero(t, ro.pendingBytes)
	require.Empty(t, ro.pendingTransforms)
	retired := primary.PublishedManifest()
	retired.Chunks = retired.Chunks[1:]
	retired.TransformFastForwardTimeTick = map[string]uint64{"v1": 10}
	require.NoError(t, ro.ApplyStoragePublished(retired))
	require.False(t, chunk.retired)
	require.Len(t, ro.chunkIndex.chunks, 2)
	require.Len(t, chunk.GetVchannels(), 1, "an existing reader keeps immutable metadata")
	// A local old View can start a new lazy read after the writer retired its
	// history. Keeping only existing reader snapshots would silently skip it.
	entries, err = ro.ReadTransformEntries(ctx, "v1", 0, 20)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.EqualValues(t, 10, entries[0].GetTimeTick())
	require.Len(t, ro.PublishedManifest().Chunks, 1, "local retention must not rewrite the authoritative publication")
	require.NoError(t, ro.ApplyStoragePublished(retired))
	require.Len(t, ro.chunkIndex.chunks, 2, "repeated publication does not duplicate retained chunks")
	entries, err = ro.ReadTransformEntries(ctx, "v1", 10, 20)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.EqualValues(t, 20, entries[0].GetTimeTick())
	regressed := proto.CloneOf(retired)
	regressed.Chunks = nil
	regressed.Coverage.EndTimeTick = 10
	require.ErrorIs(t, ro.ApplyStoragePublished(regressed), ErrStoreCorrupted)
	require.ErrorIs(t, ro.ApplyStoragePublished(&streamingpb.PChannelSummaryManifest{
		Coverage: &streamingpb.SummaryCoverage{StartTimeTick: 20, EndTimeTick: 10},
	}), ErrStoreCorrupted)
	ro.terminalErr = merr.WrapErrServiceUnavailable("closed")
	require.ErrorIs(t, ro.ApplyStoragePublished(retired), ro.terminalErr)
}
