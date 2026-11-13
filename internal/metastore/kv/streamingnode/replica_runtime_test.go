package streamingnode

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func retentionRuntime() *viewpb.StreamingWorkNode {
	return &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 10, RuntimeId: 20, WalReplicaId: 2, WalReplicaRevision: 3, WalTerm: 7, ReadOnly: true}
}

func TestWALReplicaRuntimeOwnsObjectsBeyondBootstrapAndSourceLifetime(t *testing.T) {
	c, writer, store := retentionCatalogs(t)
	ctx := t.Context()
	runtime := retentionRuntime()
	cursor := &streamingpb.WALReplicaCursor{Term: 7, Seq: 9}
	root := &streamingpb.WALObjectRetentionRoot{RootId: "bootstrap", ObjectKeys: []string{"object"}, Runtime: runtime}
	require.Error(t, c.RetainWALObjects(ctx, "p", root))
	require.Error(t, c.PrepareWALReplicaRuntime(ctx, runtime, cursor))
	require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
	require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
	require.NoError(t, c.RetainWALObjects(ctx, "p", root))
	require.Error(t, c.InstallWALReplicaRuntime(ctx, runtime, cursor))
	require.NoError(t, c.PrepareWALReplicaRuntime(ctx, runtime, cursor))
	require.Error(t, c.InstallWALReplicaRuntime(ctx, runtime, &streamingpb.WALReplicaCursor{Term: 7, Seq: 10}))
	require.NoError(t, c.InstallWALReplicaRuntime(ctx, runtime, cursor))
	require.NoError(t, c.PrepareWALReplicaRuntime(ctx, runtime, cursor), "late prepare retry cannot regress installed state")
	require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
	active, err := c.ListWALReplicaRuntimes(ctx, "p")
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_INSTALLED, active[0].State)
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 8})))
	intent, err := writer.BeginWALObjectDeletion(ctx, "p", 8, "object")
	require.NoError(t, err)
	require.Nil(t, intent, "source replacement cannot release a follower's lazy query resources")
	require.Error(t, c.InstallWALReplicaRuntime(ctx, runtime, cursor), "a new install cannot finish against an obsolete source")
	require.NoError(t, c.RetainWALObjects(ctx, "p", &streamingpb.WALObjectRetentionRoot{RootId: "other", ObjectKeys: []string{"other"}}))
	require.NoError(t, c.ReleaseWALReplicaRuntime(ctx, runtime))
	require.NoError(t, c.ReleaseWALReplicaRuntime(ctx, runtime))
	require.Error(t, c.RetainWALObjects(ctx, "p", root))
	require.Error(t, c.CreateWALReplicaRuntime(ctx, runtime))
	require.Error(t, c.PrepareWALReplicaRuntime(ctx, runtime, cursor))
	active, err = c.ListWALReplicaRuntimes(ctx, "p")
	require.NoError(t, err)
	require.Empty(t, active)
	roots, err := c.ListWALObjectRetentionRoots(ctx, "p")
	require.NoError(t, err)
	require.Len(t, roots, 1)
	require.Equal(t, "other", roots[0].RootId)
	intent, err = writer.BeginWALObjectDeletion(ctx, "p", 8, "object")
	require.NoError(t, err)
	require.NotNil(t, intent)
	runtime.RuntimeId++
	require.NoError(t, c.ReleaseWALReplicaRuntime(ctx, runtime), "release also fences a delayed create after an ambiguous open")
	require.Error(t, c.CreateWALReplicaRuntime(ctx, runtime))
}

func TestWALReplicaRuntimeReleaseAndTakeoverRaces(t *testing.T) {
	for _, operation := range []string{"retain", "prepare", "install"} {
		t.Run(operation, func(t *testing.T) {
			mockey.PatchConvey("late work cannot resurrect a released runtime or obsolete source", t, func() {
				c, other, store := retentionCatalogs(t)
				runtime := retentionRuntime()
				cursor := &streamingpb.WALReplicaCursor{Term: 7}
				require.NoError(t, c.CreateWALReplicaRuntime(t.Context(), runtime))
				if operation == "install" {
					require.NoError(t, c.PrepareWALReplicaRuntime(t.Context(), runtime, cursor))
				}
				var original func(*catalog, context.Context, string, string, map[string]string, []string, ...predicates.Predicate) error
				once := false
				mockey.Mock((*catalog).commitRetention).Origin(&original).To(func(actual *catalog, ctx context.Context, channel, epoch string, saves map[string]string, removes []string, guards ...predicates.Predicate) error {
					if actual == c && !once {
						once = true
						if operation == "install" {
							require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 8})))
						} else {
							require.NoError(t, other.ReleaseWALReplicaRuntime(ctx, runtime))
						}
					}
					return original(actual, ctx, channel, epoch, saves, removes, guards...)
				}).Build()
				switch operation {
				case "retain":
					require.Error(t, c.RetainWALObjects(t.Context(), "p", &streamingpb.WALObjectRetentionRoot{RootId: "r", ObjectKeys: []string{"o"}, Runtime: runtime}))
				case "prepare":
					require.Error(t, c.PrepareWALReplicaRuntime(t.Context(), runtime, cursor))
				case "install":
					require.Error(t, c.InstallWALReplicaRuntime(t.Context(), runtime, cursor))
				}
				require.True(t, once)
			})
		})
	}
}

func TestWALReplicaRuntimeIdentityAndCursorValidation(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	runtime := retentionRuntime()
	for _, change := range []func(*viewpb.StreamingWorkNode){
		func(r *viewpb.StreamingWorkNode) { r.Pchannel = "" }, func(r *viewpb.StreamingWorkNode) { r.NodeId = 0 },
		func(r *viewpb.StreamingWorkNode) { r.RuntimeId = 0 }, func(r *viewpb.StreamingWorkNode) { r.WalReplicaId = 0 },
		func(r *viewpb.StreamingWorkNode) { r.WalReplicaRevision = 0 }, func(r *viewpb.StreamingWorkNode) { r.WalTerm = 0 },
		func(r *viewpb.StreamingWorkNode) { r.ReadOnly = false },
	} {
		invalid := proto.CloneOf(runtime)
		change(invalid)
		require.ErrorIs(t, c.CreateWALReplicaRuntime(t.Context(), invalid), merr.ErrServiceInternal)
		require.Error(t, c.checkRuntimeRetention(t.Context(), "p", invalid))
	}
	require.NoError(t, c.CreateWALReplicaRuntime(t.Context(), runtime))
	require.Error(t, c.checkRuntimeRetention(t.Context(), "other", runtime))
	require.Error(t, c.PrepareWALReplicaRuntime(t.Context(), runtime, nil))
	require.Error(t, c.PrepareWALReplicaRuntime(t.Context(), runtime, &streamingpb.WALReplicaCursor{Term: 8}))
	conflict := proto.CloneOf(runtime)
	conflict.WalReplicaRevision++
	require.ErrorIs(t, c.CreateWALReplicaRuntime(t.Context(), conflict), merr.ErrDataIntegrity)
	for _, raw := range []string{"malformed", string(mustRuntimeBytes(t, &streamingpb.WALReplicaRuntimeRetention{Runtime: runtime, State: streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_PREPARED}))} {
		require.NoError(t, store.Save(t.Context(), replicaRuntimeKey(runtime), raw))
		require.ErrorIs(t, c.CreateWALReplicaRuntime(t.Context(), runtime), merr.ErrDataIntegrity)
		require.ErrorIs(t, c.checkRuntimeRetention(t.Context(), "p", runtime), merr.ErrDataIntegrity)
	}
	require.NoError(t, store.Remove(t.Context(), replicaRuntimeKey(runtime)))
	require.NoError(t, c.CreateWALReplicaRuntime(t.Context(), runtime))
	require.NoError(t, store.Save(t.Context(), buildConsumeCheckpointKey("p"), "malformed"))
	require.ErrorIs(t, c.PrepareWALReplicaRuntime(t.Context(), runtime, &streamingpb.WALReplicaCursor{Term: 7}), merr.ErrDataIntegrity)
	runtime.Pchannel = "\xff"
	require.ErrorIs(t, c.CreateWALReplicaRuntime(t.Context(), runtime), merr.ErrDataIntegrity)
}

func mustRuntimeBytes(t *testing.T, state *streamingpb.WALReplicaRuntimeRetention) []byte {
	t.Helper()
	value, err := proto.Marshal(state)
	require.NoError(t, err)
	return value
}

func TestWALReplicaRuntimeActiveBudgetAndCorruption(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	runtime := retentionRuntime()
	for i := range maxReplicaRuntimeObligations {
		runtime.RuntimeId = uint64(i + 1)
		require.NoError(t, c.CreateWALReplicaRuntime(t.Context(), runtime))
	}
	runtime.RuntimeId++
	require.ErrorIs(t, c.CreateWALReplicaRuntime(t.Context(), runtime), merr.ErrServiceUnavailable)
	key := retentionPrefix("p") + "replica-runtime-active/extra"
	record := &streamingpb.WALReplicaRuntimeRetention{Runtime: runtime}
	require.NoError(t, store.Save(t.Context(), key, string(mustRuntimeBytes(t, record))))
	_, err := c.ListWALReplicaRuntimes(t.Context(), "p")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, store.RemoveWithPrefix(t.Context(), retentionPrefix("p")+"replica-runtime-active/"))
	for _, value := range []string{"malformed", string(mustRuntimeBytes(t, &streamingpb.WALReplicaRuntimeRetention{})), string(mustRuntimeBytes(t, &streamingpb.WALReplicaRuntimeRetention{Runtime: runtime, State: streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED}))} {
		require.NoError(t, store.Save(t.Context(), key, value))
		_, err := c.ListWALReplicaRuntimes(t.Context(), "p")
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
}

func TestWALReplicaRuntimeReleaseCanResumeAfterCleanupFailure(t *testing.T) {
	mockey.PatchConvey("failed root cleanup does not reopen admission", t, func() {
		c, _, _ := retentionCatalogs(t)
		runtime := retentionRuntime()
		require.Error(t, c.ReleaseWALReplicaRuntime(t.Context(), nil))
		require.NoError(t, c.CreateWALReplicaRuntime(t.Context(), runtime))
		root := &streamingpb.WALObjectRetentionRoot{RootId: "r", ObjectKeys: []string{"o"}, Runtime: runtime}
		require.NoError(t, c.RetainWALObjects(t.Context(), "p", root))
		patch := mockey.Mock((*catalog).ListWALObjectRetentionRoots).Return(nil, merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, c.ReleaseWALReplicaRuntime(t.Context(), runtime), merr.ErrServiceUnavailable)
		patch.UnPatch()
		require.Error(t, c.RetainWALObjects(t.Context(), "p", root))
		patch = mockey.Mock((*catalog).ReleaseWALObjects).Return(merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, c.ReleaseWALReplicaRuntime(t.Context(), runtime), merr.ErrServiceUnavailable)
		patch.UnPatch()
		require.NoError(t, c.ReleaseWALReplicaRuntime(t.Context(), runtime))
		roots, err := c.ListWALObjectRetentionRoots(t.Context(), "p")
		require.NoError(t, err)
		require.Empty(t, roots)
	})
}

func TestWALReplicaRuntimeReadFailuresPreserveState(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	runtime := retentionRuntime()
	for _, key := range []string{retentionPrefix("p") + "epoch", replicaRuntimeKey(runtime), buildConsumeCheckpointKey("p")} {
		mockey.PatchConvey("failed reads cannot authorize a candidate", t, func() {
			require.NoError(t, c.CreateWALReplicaRuntime(t.Context(), runtime))
			var original func(context.Context, string) (string, error)
			mockey.Mock(mockey.GetMethod(store, "Load")).Origin(&original).To(func(ctx context.Context, actual string) (string, error) {
				if actual == key {
					return "", merr.ErrServiceUnavailable
				}
				return original(ctx, actual)
			}).Build()
			require.ErrorIs(t, c.PrepareWALReplicaRuntime(t.Context(), runtime, &streamingpb.WALReplicaCursor{Term: 7}), merr.ErrServiceUnavailable)
		})
	}
	mockey.PatchConvey("the active budget must be readable before creating a new obligation", t, func() {
		runtime.RuntimeId++
		mockey.Mock((*catalog).ListWALReplicaRuntimes).Return(nil, merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, c.CreateWALReplicaRuntime(t.Context(), runtime), merr.ErrServiceUnavailable)
	})
}
