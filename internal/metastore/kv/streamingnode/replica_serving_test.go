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

func TestLostWALReplicaFencesPublicationBeforeReclaimingObjects(t *testing.T) {
	c, restarted, store := retentionCatalogs(t)
	ctx := t.Context()
	runtime := retentionRuntime()
	cursor := &streamingpb.WALReplicaCursor{Term: runtime.WalTerm, Seq: 10}
	require.ErrorIs(t, c.ValidateWALReplicaServing(ctx, runtime), merr.ErrServiceUnavailable)
	require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
	require.Error(t, c.ValidateWALReplicaServing(ctx, runtime))
	require.NoError(t, c.PrepareWALReplicaRuntime(ctx, runtime, cursor))
	require.Error(t, c.ValidateWALReplicaServing(ctx, runtime))
	require.NoError(t, c.InstallWALReplicaRuntime(ctx, runtime, cursor))
	root := &streamingpb.WALObjectRetentionRoot{RootId: "runtime", Runtime: runtime, ObjectKeys: []string{"query-object"}}
	require.NoError(t, c.RetainWALObjects(ctx, "p", root))
	require.NoError(t, c.RetainSharedWALObjects(ctx, root))
	view := &viewpb.QueryViewMeta{CollectionId: 100, ReplicaId: 1, Vchannel: "p_100v0", Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}}}
	require.NoError(t, c.RetainWALQueryView(ctx, runtime, view))
	require.NoError(t, store.Save(ctx, replicaNodeSessionKey(runtime.NodeId), "live"))
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 8})))
	require.NoError(t, restarted.FenceLostWALReplicaRuntimes(ctx, "p", 1))
	require.NoError(t, c.ValidateWALReplicaServing(ctx, runtime), "new writer term does not revoke a surviving RO")
	retired, err := c.RetireWALQueryView(ctx, view)
	require.NoError(t, err)
	require.False(t, retired)
	require.NoError(t, store.Remove(ctx, replicaNodeSessionKey(runtime.NodeId)))
	require.NoError(t, c.ValidateWALReplicaServing(ctx, runtime), "session loss alone is not resource release")
	require.NoError(t, restarted.FenceLostWALReplicaRuntimes(ctx, "p", 1))
	require.ErrorIs(t, c.ValidateWALReplicaServing(ctx, runtime), merr.ErrServiceUnavailable, "an in-flight result must now be discarded")
	require.NoError(t, c.SweepReleasedWALReplicaRoots(ctx, "p", 1))
	require.NoError(t, c.SweepReleasedSharedWALRoots(ctx, 1))
	intent, err := c.BeginWALObjectDeletion(ctx, "p", 8, "query-object")
	require.NoError(t, err)
	require.NotNil(t, intent)
	shared, err := c.BeginSharedWALObjectDeletion(ctx, "query-object")
	require.NoError(t, err)
	require.NotNil(t, shared)
	retired, err = c.RetireWALQueryView(ctx, view)
	require.NoError(t, err)
	require.True(t, retired)
	require.Error(t, c.RetainWALObjects(ctx, "p", root))
	require.Error(t, c.RetainSharedWALObjects(ctx, root))
	late := proto.CloneOf(runtime)
	late.RuntimeId++
	require.NoError(t, store.Save(ctx, replicaNodeSessionKey(runtime.NodeId), "late session retry"))
	require.Error(t, c.CreateWALReplicaRuntime(ctx, late), "a fenced process cannot mint a new runtime")
	require.NoError(t, c.FenceLostWALReplicaRuntimes(ctx, "p", 1))
}

func TestLostWALReplicaFencingChecksSessionAtCommitAndBoundsWork(t *testing.T) {
	mockey.PatchConvey("a node registering concurrently is not mistaken for a lost process", t, func() {
		c, _, store := retentionCatalogs(t)
		runtime := retentionRuntime()
		require.NoError(t, c.CreateWALReplicaRuntime(t.Context(), runtime))
		var original func(*catalog, context.Context, string, string, map[string]string, []string, ...predicates.Predicate) error
		patch := mockey.Mock((*catalog).commitRetention).Origin(&original).To(func(c *catalog, ctx context.Context, channel, epoch string, saves map[string]string, removals []string, guards ...predicates.Predicate) error {
			require.NoError(t, store.Save(ctx, replicaNodeSessionKey(runtime.NodeId), "registered"))
			return original(c, ctx, channel, epoch, saves, removals, guards...)
		}).Build()
		require.Error(t, c.FenceLostWALReplicaRuntimes(t.Context(), "p", 1))
		patch.UnPatch()
		active, err := c.ListWALReplicaRuntimes(t.Context(), "p")
		require.NoError(t, err)
		require.Len(t, active, 1)
		second := proto.CloneOf(runtime)
		second.NodeId++
		require.NoError(t, c.CreateWALReplicaRuntime(t.Context(), second))
		require.NoError(t, store.Remove(t.Context(), replicaNodeSessionKey(runtime.NodeId)))
		require.NoError(t, c.FenceLostWALReplicaRuntimes(t.Context(), "p", 1))
		active, err = c.ListWALReplicaRuntimes(t.Context(), "p")
		require.NoError(t, err)
		require.Len(t, active, 1)
		require.NoError(t, c.FenceLostWALReplicaRuntimes(t.Context(), "p", 2))
		active, err = c.ListWALReplicaRuntimes(t.Context(), "p")
		require.NoError(t, err)
		require.Empty(t, active)
	})
}

func TestLostWALReplicaFencingPreservesResourcesOnCatalogErrors(t *testing.T) {
	mockey.PatchConvey("catalog errors never authorize deletion", t, func() {
		c, _, store := retentionCatalogs(t)
		ctx := t.Context()
		runtime := retentionRuntime()
		require.ErrorIs(t, c.FenceLostWALReplicaRuntimes(ctx, "p", 0), merr.ErrServiceInternal)
		require.ErrorIs(t, c.ValidateWALReplicaServing(ctx, nil), merr.ErrServiceInternal)
		require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
		patch := mockey.Mock((*catalog).ListWALReplicaRuntimes).Return(nil, merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, c.FenceLostWALReplicaRuntimes(ctx, "p", 1), merr.ErrServiceUnavailable)
		patch.UnPatch()
		for _, key := range []string{replicaNodeSessionKey(runtime.NodeId), retentionPrefix("p") + "epoch", replicaNodeFenceKey(runtime)} {
			var original func(*catalog, context.Context, string) (string, error)
			patch = mockey.Mock((*catalog).loadRetentionValue).Origin(&original).To(func(c *catalog, ctx context.Context, actual string) (string, error) {
				if actual == key {
					return "", merr.ErrServiceUnavailable
				}
				return original(c, ctx, actual)
			}).Build()
			if key == replicaNodeFenceKey(runtime) {
				require.ErrorIs(t, c.CreateWALReplicaRuntime(ctx, runtime), merr.ErrServiceUnavailable)
			} else {
				require.ErrorIs(t, c.FenceLostWALReplicaRuntimes(ctx, "p", 1), merr.ErrServiceUnavailable)
			}
			patch.UnPatch()
		}
		require.NoError(t, store.Save(ctx, replicaRuntimeKey(runtime), "corrupt"))
		require.ErrorIs(t, c.ValidateWALReplicaServing(ctx, runtime), merr.ErrDataIntegrity)
	})
}
