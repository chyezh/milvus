package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type bootstrapSnapshotStorage struct{ recovery.RecoveryStorage }

func (*bootstrapSnapshotStorage) CutReplicaSnapshot(context.Context) (*streamingpb.WALReplicaSnapshot, *walevent.Reader, error) {
	panic("mockey")
}

type bootstrapRawWAL struct{ walimpls.WALImpls }

func (*bootstrapRawWAL) Channel() types.PChannelInfo { panic("mockey") }

type bootstrapReplayScanner struct{ wal.Scanner }

func (*bootstrapReplayScanner) Chan() <-chan message.ImmutableMessage { panic("mockey") }
func (*bootstrapReplayScanner) Error() error                          { panic("mockey") }
func (*bootstrapReplayScanner) Close() error                          { panic("mockey") }

func bootstrapCheckpoint(term int64, tick uint64) *streamingpb.WALCheckpoint {
	return &streamingpb.WALCheckpoint{Term: term, TimeTick: tick, MessageId: message.MustMarshalMessageID(walimplstest.NewTestMessageID(int64(tick)))}
}

func protectedBootstrapSource(t *testing.T) (*walAdaptorImpl, *viewpb.StreamingWorkNode, chan message.ImmutableMessage) {
	t.Helper()
	r := newReplicaObjectRetentionForTest(t)
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(r.rootPath))
	catalog := r.catalog.(metastore.StreamingNodeCataLog)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(cm))
	require.NoError(t, catalog.SaveRecoverySnapshot(t.Context(), "p", &metastore.WALRecoverySnapshot{ConsumeCheckpoint: bootstrapCheckpoint(5, 10)}))
	mockey.Mock((*bootstrapRawWAL).Channel).Return(types.PChannelInfo{Name: "p", Term: 5, AccessMode: types.AccessModeRW}).Build()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{
		lifetime: typeutil.NewLifetime(), availableCtx: ctx, availableCancel: cancel, roWALImpls: &bootstrapRawWAL{},
	}, param: &interceptors.InterceptorBuildParam{RecoveryStorage: &bootstrapSnapshotStorage{}}}
	source := walevent.NewSource(5, 32, 1<<20, func(context.Context, walevent.Event) error { return nil })
	t.Cleanup(source.Close)
	cursor, err := source.Publish(t.Context(), walevent.CheckpointPublished{Checkpoint: bootstrapCheckpoint(5, 10)})
	require.NoError(t, err)
	up, down := replicaRetentionView(1), replicaRetentionView(2)
	down.Meta.State = viewpb.QueryViewState_QueryViewStateDown
	image := &streamingpb.WALReplicaSnapshot{
		Cursor: cursor.IntoProto(), Checkpoint: bootstrapCheckpoint(5, 10), Observed: bootstrapCheckpoint(5, 20),
		QueryViews: []*viewpb.QueryViewOfShard{up, down}, Segments: []*streamingpb.SegmentAssignmentMeta{{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{DeltaBinlog: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "lazy-object"}}}}},
		}},
	}
	mockey.Mock((*bootstrapSnapshotStorage).CutReplicaSnapshot).To(func(_ *bootstrapSnapshotStorage, ctx context.Context) (*streamingpb.WALReplicaSnapshot, *walevent.Reader, error) {
		reader, err := source.Cut(ctx, func(walevent.Cursor) error { return nil })
		return proto.CloneOf(image), reader, err
	}).Build()
	ch := make(chan message.ImmutableMessage, 8)
	mockey.Mock((*bootstrapReplayScanner).Chan).To(func(*bootstrapReplayScanner) <-chan message.ImmutableMessage { return ch }).Build()
	mockey.Mock((*bootstrapReplayScanner).Error).Return(nil).Build()
	mockey.Mock((*bootstrapReplayScanner).Close).Return(nil).Build()
	mockey.Mock((*roWALAdaptorImpl).Read).Return(&bootstrapReplayScanner{}, nil).Build()
	return w, r.runtime, ch
}

func bootstrapReplayMessage(t *testing.T, tick uint64) message.ImmutableMessage {
	id := walimplstest.NewTestMessageID(int64(tick))
	return recoveryTestMessage(t, message.MessageTypeTimeTick, tick).WithLastConfirmed(id).IntoImmutableMessage(id)
}

func TestProtectedBootstrapPinsReplayAndTransfersObjects(t *testing.T) {
	mockey.PatchConvey("transport closure releases replay, never installed query dependencies", t, func() {
		w, runtime, ch := protectedBootstrapSource(t)
		ctx := t.Context()
		transfer, err := w.OpenReplicaBootstrap(ctx, nil, runtime)
		require.NoError(t, err)
		defer transfer.Close()
		require.Error(t, transfer.Complete(ctx), "partial replay cannot prepare the receiver")
		catalog := resource.Resource().StreamingNodeCatalog().(replicaBootstrapCatalog)
		require.NoError(t, resource.Resource().StreamingNodeCatalog().SaveRecoverySnapshot(ctx, "p", &metastore.WALRecoverySnapshot{ConsumeCheckpoint: bootstrapCheckpoint(5, 30)}))
		bound, err := catalog.AuthorizeWALTruncation(ctx, "p", 5, bootstrapCheckpoint(5, 30))
		require.NoError(t, err)
		require.EqualValues(t, 10, bound.TimeTick)
		ch <- bootstrapReplayMessage(t, 9)
		ch <- bootstrapReplayMessage(t, 15)
		ch <- bootstrapReplayMessage(t, 15)
		ch <- bootstrapReplayMessage(t, 20)
		for _, tick := range []uint64{15, 15, 20} {
			msg, err := transfer.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, tick, msg.TimeTick())
		}
		msg, err := transfer.Next(ctx)
		require.NoError(t, err)
		require.Nil(t, msg)
		require.NoError(t, transfer.Complete(ctx))
		require.NoError(t, catalog.InstallWALReplicaRuntime(ctx, runtime, transfer.Snapshot().Cursor))
		transfer.Close()
		transfer.Close()
		require.ErrorIs(t, transfer.Complete(ctx), context.Canceled)
		bound, err = catalog.AuthorizeWALTruncation(ctx, "p", 5, bootstrapCheckpoint(5, 30))
		require.NoError(t, err)
		require.EqualValues(t, 30, bound.TimeTick)
		operation, err := catalog.BeginSharedWALObjectDeletion(ctx, "lazy-object")
		require.NoError(t, err)
		require.Nil(t, operation)
		require.NoError(t, catalog.ReleaseWALReplicaRuntime(ctx, runtime))
		require.NoError(t, catalog.SweepReleasedSharedWALRoots(ctx, 10))
		operation, err = catalog.BeginSharedWALObjectDeletion(ctx, "lazy-object")
		require.NoError(t, err)
		require.NotNil(t, operation)
	})
}

func TestProtectedBootstrapRejectsObsoleteSourceAndWrongReplay(t *testing.T) {
	for _, failure := range []string{"term", "closing", "regression", "overshoot", "different-physical-cut", "early-eof", "canceled-call", "close-blocked-reader"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("only the exact completed source prefix may install", t, func() {
				w, runtime, ch := protectedBootstrapSource(t)
				transfer, err := w.OpenReplicaBootstrap(t.Context(), nil, runtime)
				require.NoError(t, err)
				defer transfer.Close()
				switch failure {
				case "term", "closing":
					ch <- bootstrapReplayMessage(t, 20)
					_, err = transfer.Next(t.Context())
					require.NoError(t, err)
					if failure == "term" {
						require.NoError(t, resource.Resource().StreamingNodeCatalog().SaveRecoverySnapshot(t.Context(), "p", &metastore.WALRecoverySnapshot{ConsumeCheckpoint: bootstrapCheckpoint(6, 30)}))
					} else {
						w.lifetime.SetState(typeutil.LifetimeStateStopped)
					}
					require.Error(t, transfer.Complete(t.Context()))
				case "regression":
					ch <- bootstrapReplayMessage(t, 15)
					_, err = transfer.Next(t.Context())
					require.NoError(t, err)
					ch <- bootstrapReplayMessage(t, 14)
					_, err = transfer.Next(t.Context())
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
				case "overshoot":
					ch <- bootstrapReplayMessage(t, 21)
					_, err = transfer.Next(t.Context())
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
				case "different-physical-cut":
					ch <- recoveryTestMessage(t, message.MessageTypeTimeTick, 20).IntoImmutableMessage(walimplstest.NewTestMessageID(20))
					_, err = transfer.Next(t.Context())
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
				case "early-eof":
					close(ch)
					_, err = transfer.Next(t.Context())
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				case "canceled-call":
					ctx, cancel := context.WithCancel(t.Context())
					cancel()
					_, err = transfer.Next(ctx)
					require.ErrorIs(t, err, context.Canceled)
				case "close-blocked-reader":
					done := make(chan error, 1)
					go func() { _, err := transfer.Next(t.Context()); done <- err }()
					transfer.Close()
					select {
					case err := <-done:
						require.ErrorIs(t, err, context.Canceled)
					case <-time.After(time.Second):
						t.Fatal("Close must cancel and join blocked replay")
					}
				}
			})
		})
	}
}
