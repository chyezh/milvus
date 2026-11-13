package streamingnode

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSharedWALObjectLifetimeAcrossChannels(t *testing.T) {
	c, restarted, store := retentionCatalogs(t)
	ctx := t.Context()
	first := retentionRuntime()
	second := proto.CloneOf(first)
	second.Pchannel = "other"
	root := &streamingpb.WALObjectRetentionRoot{RootId: "baseline", Runtime: first, ObjectKeys: []string{"shared/b", "shared/a", "shared/a"}}
	require.Error(t, c.RetainSharedWALObjects(ctx, root), "a root needs a registered receiver")
	require.NoError(t, c.CreateWALReplicaRuntime(ctx, first))
	require.NoError(t, c.CreateWALReplicaRuntime(ctx, second))
	require.NoError(t, c.RetainSharedWALObjects(ctx, root))
	require.NoError(t, c.RetainSharedWALObjects(ctx, root))
	require.Len(t, root.ObjectKeys, 3, "catalog must not change caller metadata")
	other := proto.CloneOf(root)
	other.Runtime = second
	require.NoError(t, c.RetainSharedWALObjects(ctx, other))
	roots, err := restarted.ListSharedWALObjectRoots(ctx)
	require.NoError(t, err)
	require.Len(t, roots, 2, "root IDs are scoped by exact runtime, across PChannels")
	for _, stored := range roots {
		require.Equal(t, []string{"shared/a", "shared/b"}, stored.ObjectKeys)
	}
	conflict := proto.CloneOf(root)
	conflict.ObjectKeys = []string{"different"}
	require.ErrorIs(t, c.RetainSharedWALObjects(ctx, conflict), merr.ErrDataIntegrity)
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 8})))
	operation, err := restarted.BeginSharedWALObjectDeletion(ctx, "shared/a")
	require.NoError(t, err)
	require.Nil(t, operation, "source term changes do not release installed dependencies")
	require.NoError(t, c.ReleaseWALReplicaRuntime(ctx, first))
	require.Error(t, c.RetainSharedWALObjects(ctx, root))
	require.NoError(t, restarted.SweepReleasedSharedWALRoots(ctx, 1))
	operation, err = restarted.BeginSharedWALObjectDeletion(ctx, "shared/a")
	require.NoError(t, err)
	require.Nil(t, operation, "the other channel still owns the shared object")
	require.NoError(t, c.ReleaseSharedWALObjects(ctx, second, root.RootId))
	require.NoError(t, c.ReleaseSharedWALObjects(ctx, second, root.RootId))
	require.Error(t, c.RetainSharedWALObjects(ctx, other), "late retry cannot resurrect a released root")
	operation, err = restarted.BeginSharedWALObjectDeletion(ctx, "shared/a")
	require.NoError(t, err)
	require.NotEmpty(t, operation.OperationId)
	resumed, err := c.BeginSharedWALObjectDeletion(ctx, "shared/a")
	require.NoError(t, err)
	require.True(t, proto.Equal(operation, resumed))
	other.RootId = "late"
	require.Error(t, c.RetainSharedWALObjects(ctx, other))
	require.NoError(t, c.CompleteSharedWALObjectDeletion(ctx, operation))
	require.NoError(t, restarted.CompleteSharedWALObjectDeletion(ctx, resumed))
	completed, err := restarted.BeginSharedWALObjectDeletion(ctx, "shared/a")
	require.NoError(t, err)
	require.True(t, completed.Done)
	require.Error(t, c.RetainSharedWALObjects(ctx, other), "completed deletion also fences late capture")
}

func TestSharedWALRetentionRaces(t *testing.T) {
	for _, race := range []string{"delete-before-retain", "retain-before-delete", "release-before-retain", "close-before-retain"} {
		t.Run(race, func(t *testing.T) {
			mockey.PatchConvey("capture and deletion cannot both win", t, func() {
				c, other, _ := retentionCatalogs(t)
				runtime := retentionRuntime()
				require.NoError(t, c.CreateWALReplicaRuntime(t.Context(), runtime))
				root := &streamingpb.WALObjectRetentionRoot{RootId: "r", Runtime: runtime, ObjectKeys: []string{"object"}}
				var original func(*catalog, context.Context, string, string, map[string]string, []string, ...predicates.Predicate) error
				interleaved := false
				mockey.Mock((*catalog).commitRetentionAt).Origin(&original).To(func(actual *catalog, ctx context.Context, key, epoch string, saves map[string]string, removes []string, guards ...predicates.Predicate) error {
					if actual == c && !interleaved {
						interleaved = true
						switch race {
						case "delete-before-retain":
							operation, err := other.BeginSharedWALObjectDeletion(ctx, "object")
							require.NoError(t, err)
							require.NotNil(t, operation)
						case "retain-before-delete":
							require.NoError(t, other.RetainSharedWALObjects(ctx, root))
						case "release-before-retain":
							require.NoError(t, other.ReleaseSharedWALObjects(ctx, runtime, root.RootId))
						case "close-before-retain":
							require.NoError(t, other.ReleaseWALReplicaRuntime(ctx, runtime))
						}
					}
					return original(actual, ctx, key, epoch, saves, removes, guards...)
				}).Build()
				if race == "retain-before-delete" {
					operation, err := c.BeginSharedWALObjectDeletion(t.Context(), "object")
					require.Error(t, err)
					require.Nil(t, operation)
					operation, err = other.BeginSharedWALObjectDeletion(t.Context(), "object")
					require.NoError(t, err)
					require.Nil(t, operation)
				} else {
					require.Error(t, c.RetainSharedWALObjects(t.Context(), root))
					require.Error(t, c.RetainSharedWALObjects(t.Context(), root))
					roots, err := c.ListSharedWALObjectRoots(t.Context())
					require.NoError(t, err)
					require.Empty(t, roots)
				}
				require.True(t, interleaved)
			})
		})
	}
}

func TestSharedWALDeletionRecovery(t *testing.T) {
	c, restarted, store := retentionCatalogs(t)
	ctx := t.Context()
	for _, object := range []string{"a", "b", "c"} {
		_, err := c.BeginSharedWALObjectDeletion(ctx, object)
		require.NoError(t, err)
	}
	for _, count := range []int{2, 1, 0} {
		pending, err := restarted.ListPendingSharedWALObjectDeletions(ctx, 2)
		require.NoError(t, err)
		require.Len(t, pending, count)
		for _, operation := range pending {
			wrong := proto.CloneOf(operation)
			wrong.OperationId = "wrong"
			require.ErrorIs(t, c.CompleteSharedWALObjectDeletion(ctx, wrong), merr.ErrDataIntegrity)
			require.NoError(t, c.CompleteSharedWALObjectDeletion(ctx, operation))
		}
	}
	for _, limit := range []int{0, maxRetentionObjects + 1} {
		_, err := c.ListPendingSharedWALObjectDeletions(ctx, limit)
		require.Error(t, err)
	}
	for _, raw := range []string{"malformed", "", string(mustRuntimeBytes(t, &streamingpb.WALReplicaRuntimeRetention{}))} {
		require.NoError(t, store.Save(ctx, sharedRetentionKey("pending", "bad"), raw))
		_, err := c.ListPendingSharedWALObjectDeletions(ctx, 2)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
	for _, raw := range []string{"malformed", "\x0a\x01x"} {
		require.NoError(t, store.Save(ctx, sharedRetentionKey("deletions", "bad"), raw))
		_, err := c.BeginSharedWALObjectDeletion(ctx, "bad")
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
}

func TestSharedWALRetentionValidationAndMissingOwner(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	ctx := t.Context()
	runtime := retentionRuntime()
	require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
	for _, root := range []*streamingpb.WALObjectRetentionRoot{
		nil,
		{},
		{RootId: "r", ObjectKeys: []string{"o"}},
		{RootId: "r", Runtime: runtime, ObjectKeys: []string{""}},
		{RootId: "r", Runtime: runtime, ObjectKeys: make([]string, maxRetentionObjects+1)},
	} {
		require.Error(t, c.RetainSharedWALObjects(ctx, root))
	}
	require.Error(t, c.ReleaseSharedWALObjects(ctx, nil, "r"))
	require.Error(t, c.ReleaseSharedWALObjects(ctx, runtime, ""))
	require.Error(t, c.SweepReleasedSharedWALRoots(ctx, 0))
	require.Error(t, c.SweepReleasedSharedWALRoots(ctx, maxSharedRoots+1))
	_, err := c.BeginSharedWALObjectDeletion(ctx, "")
	require.Error(t, err)
	require.Error(t, c.CompleteSharedWALObjectDeletion(ctx, nil))
	require.Error(t, c.CompleteSharedWALObjectDeletion(ctx, &streamingpb.WALObjectDeletion{ObjectKey: "o", OperationId: "absent"}))
	root := &streamingpb.WALObjectRetentionRoot{RootId: "r", Runtime: runtime, ObjectKeys: []string{"o"}}
	require.NoError(t, c.RetainSharedWALObjects(ctx, root))
	require.NoError(t, store.Remove(ctx, replicaRuntimeKey(runtime)))
	require.NoError(t, c.SweepReleasedSharedWALRoots(ctx, 1))
	roots, err := c.ListSharedWALObjectRoots(ctx)
	require.NoError(t, err)
	require.Len(t, roots, 1, "missing owner is not proof that readers drained")
	for _, raw := range []string{"malformed", "", "\x0a\x01r\x12\x01o"} {
		require.NoError(t, store.Save(ctx, sharedRootKey("roots", runtime, "r"), raw))
		_, err := c.ListSharedWALObjectRoots(ctx)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		_, err = c.BeginSharedWALObjectDeletion(ctx, "o")
		require.Error(t, err)
	}
}
