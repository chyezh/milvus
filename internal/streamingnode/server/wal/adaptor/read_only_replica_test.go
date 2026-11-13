package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func readOnlyReplicaBaseline() (recovery.ReadOnlyConfig, *streamingpb.WALReplicaSnapshot) {
	checkpoint := &streamingpb.WALCheckpoint{MessageId: message.MustMarshalMessageID(walimplstest.NewTestMessageID(1)), TimeTick: 1, LogGeneration: 1}
	return recovery.ReadOnlyConfig{Channel: types.PChannelInfo{Name: "ro-replica", Term: 7}, Modules: vchannel.PChannelManagerConfig{Logger: mlog.With()}},
		&streamingpb.WALReplicaSnapshot{Cursor: &streamingpb.WALReplicaCursor{Term: 7}, Checkpoint: checkpoint, Observed: proto.CloneOf(checkpoint)}
}

func TestReadOnlyReplicaHasNoWriterCapabilities(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	cfg, snapshot := readOnlyReplicaBaseline()
	ctx, cancel := context.WithCancel(t.Context())
	resolutions := 0
	remoteMVCC := &viewpb.QueryPlanMVCC{GrowingTimetick: 2, TransformingTimetick: 1}
	r, err := newReadOnlyReplica(ctx, cfg, snapshot, nil, func(_ context.Context, req *viewpb.GetQueryPlanRequest, vchannel string) (*viewpb.QueryPlanMVCC, error) {
		resolutions++
		require.Equal(t, "v", vchannel)
		require.Equal(t, commonpb.ConsistencyLevel_Strong, req.GetConsistencyLevel())
		return remoteMVCC, nil
	}, recovery.WithNodeScheduler(scheduler))
	require.NoError(t, err)
	defer r.Close()
	cancel()
	require.True(t, r.IsAvailable(), "opening/transport context is not the local query lifetime")
	require.Equal(t, types.AccessModeRO, r.Channel().AccessMode)
	require.Equal(t, message.WALNameTest, r.WALName())
	require.Equal(t, types.ROWALMetrics{ChannelInfo: r.Channel()}, r.Metrics())
	fixed := &viewpb.QueryPlanMVCC{GrowingTimetick: 1, TransformingTimetick: 1}
	mvcc, err := r.resolveMVCC(t.Context(), &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: fixed}}, "v")
	require.NoError(t, err)
	require.Same(t, fixed, mvcc)
	require.Zero(t, resolutions, "a fixed snapshot can use local data without contacting the primary")
	mvcc, err = r.resolveMVCC(t.Context(), &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong}}, "v")
	require.NoError(t, err)
	require.Same(t, remoteMVCC, mvcc)
	require.Equal(t, 1, resolutions)
	_, err = r.Append(t.Context(), nil)
	require.True(t, status.AsStreamingError(err).IsFenced())
	called := false
	r.AppendAsync(t.Context(), nil, func(result *wal.AppendResult, err error) {
		called = true
		require.Nil(t, result)
		require.True(t, status.AsStreamingError(err).IsFenced())
	})
	require.True(t, called)
	_, err = r.Read(t.Context(), wal.ReadOption{})
	require.True(t, status.AsStreamingError(err).IsFenced())
	_, err = r.GetLatestMVCCTimestamp(t.Context(), "v")
	require.True(t, status.AsStreamingError(err).IsFenced())
	_, err = r.GetReplicateCheckpoint()
	require.True(t, status.AsStreamingError(err).IsFenced())
	require.Empty(t, r.GetSalvageCheckpoint())
	require.NotNil(t, r.TransformLog())
	_, err = r.GetMVCCTimestamp(t.Context(), &viewpb.GetMVCCTimestampRequest{})
	require.True(t, viewerror.AsViewError(err).IsNotPrimary())
	_, err = r.GetQueryPlan(t.Context(), nil)
	require.Error(t, err)
	// The same Apply endpoint accepts the source's query intent without any
	// catalog writer, and a canceled subscription leaves that runtime intact.
	require.NoError(t, r.storage.Apply(t.Context(), walevent.Event{Cursor: walevent.Cursor{Term: 7, Seq: 1}, Payload: walevent.QueryViewIntentBatch{}}))
	runtime := r.storage.VChannelManager()
	require.ErrorIs(t, r.storage.SubscribeAndApply(ctx, nil, cfg.Channel), context.Canceled)
	require.Same(t, runtime, r.storage.VChannelManager())
	require.True(t, r.IsAvailable())
	watch, err := r.AcquireReplicaWatchStream(t.Context())
	require.NoError(t, err)
	defer watch.Close()
	r.Close()
	r.Close()
	require.False(t, r.IsAvailable())
	select {
	case <-watch.Done():
	case <-time.After(time.Second):
		t.Fatal("closed RO runtime retained its directory stream")
	}
	_, err = r.AcquireReplicaWatchStream(t.Context())
	require.True(t, status.AsStreamingError(err).IsOnShutdown())
	select {
	case <-r.Unavailable():
	default:
		t.Fatal("closed replica is still available")
	}
	_, err = r.GetQueryPlan(t.Context(), nil)
	require.True(t, viewerror.AsViewError(err).IsOnShutdown())
}

func TestReadOnlyReplicaRejectsFailedBootstrap(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	cfg, snapshot := readOnlyReplicaBaseline()
	snapshot.Cursor.Term++
	r, err := newReadOnlyReplica(t.Context(), cfg, snapshot, nil, nil, recovery.WithNodeScheduler(scheduler))
	require.Error(t, err)
	require.Nil(t, r)
	snapshot.Cursor.Term--
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r, err = newReadOnlyReplica(ctx, cfg, snapshot, nil, nil, recovery.WithNodeScheduler(scheduler))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, r)
}
