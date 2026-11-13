package streamingnode

import (
	"context"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func retentionCatalogs(t *testing.T) (*catalog, *catalog, kv.MetaKv) {
	t.Helper()
	client, _ := kvfactory.GetEtcdAndPath()
	store := etcdkv.NewEtcdKV(client, "wal-object-retention-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")) })
	require.NoError(t, store.Save(t.Context(), buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 7})))
	return NewCataLog(store).(*catalog), NewCataLog(store).(*catalog), store
}

func TestWALObjectRetentionSurvivesOwnershipChange(t *testing.T) {
	writer, reader, store := retentionCatalogs(t)
	ctx := t.Context()
	root := &streamingpb.WALObjectRetentionRoot{RootId: "bootstrap-1", ObjectKeys: []string{"summary/b", "summary/a", "summary/a"}}
	require.NoError(t, reader.RetainWALObjects(ctx, "p", root))
	require.Len(t, root.ObjectKeys, 3, "caller metadata remains owned")
	require.NoError(t, reader.RetainWALObjects(ctx, "p", root))
	roots, err := writer.ListWALObjectRetentionRoots(ctx, "p")
	require.NoError(t, err)
	require.Len(t, roots, 1)
	require.Equal(t, []string{"summary/a", "summary/b"}, roots[0].ObjectKeys)
	intent, err := writer.BeginWALObjectDeletion(ctx, "p", 7, "summary/a")
	require.NoError(t, err)
	require.Nil(t, intent, "a remote bootstrap blocks physical deletion")
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 8})))
	intent, err = writer.BeginWALObjectDeletion(ctx, "p", 7, "summary/a")
	require.Error(t, err)
	require.Nil(t, intent, "old owner cannot authorize new deletion")
	intent, err = writer.BeginWALObjectDeletion(ctx, "p", 8, "summary/a")
	require.NoError(t, err)
	require.Nil(t, intent, "changing writer does not release the remote runtime")
	require.ErrorIs(t, reader.RetainWALObjects(ctx, "p", &streamingpb.WALObjectRetentionRoot{RootId: root.RootId, ObjectKeys: []string{"other"}}), merr.ErrDataIntegrity)
	require.NoError(t, reader.ReleaseWALObjects(ctx, "p", root.RootId))
	require.NoError(t, reader.ReleaseWALObjects(ctx, "p", root.RootId))
	require.Error(t, reader.RetainWALObjects(ctx, "p", root), "late retries cannot resurrect a released root")
	intent, err = writer.BeginWALObjectDeletion(ctx, "p", 8, "summary/a")
	require.NoError(t, err)
	require.NotEmpty(t, intent.OperationId)
	require.False(t, intent.Done)
	pending, err := reader.ListPendingWALObjectDeletions(ctx, "p", 1)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.True(t, proto.Equal(intent, pending[0]))
	// The request may already be in object storage when ownership changes.
	// Its exact key remains ineligible for a new root before and after Done.
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 9})))
	lateRoot := &streamingpb.WALObjectRetentionRoot{RootId: "new-owner", ObjectKeys: []string{"summary/a"}}
	require.Error(t, reader.RetainWALObjects(ctx, "p", lateRoot))
	resumed, err := reader.BeginWALObjectDeletion(ctx, "p", 9, "summary/a")
	require.NoError(t, err)
	require.True(t, proto.Equal(intent, resumed), "new owner resumes the exact durable deletion")
	require.NoError(t, writer.CompleteWALObjectDeletion(ctx, "p", intent))
	require.NoError(t, reader.CompleteWALObjectDeletion(ctx, "p", resumed))
	pending, err = reader.ListPendingWALObjectDeletions(ctx, "p", 1)
	require.NoError(t, err)
	require.Empty(t, pending)
	completed, err := writer.BeginWALObjectDeletion(ctx, "p", 8, "summary/a")
	require.NoError(t, err)
	require.True(t, completed.Done)
	require.Error(t, reader.RetainWALObjects(ctx, "p", lateRoot))
	require.NoError(t, reader.RetainWALObjects(ctx, "p", &streamingpb.WALObjectRetentionRoot{RootId: "unrelated", ObjectKeys: []string{"summary/c"}}))
}

func TestPendingWALObjectDeletionRecoveryPages(t *testing.T) {
	c, restarted, store := retentionCatalogs(t)
	for _, object := range []string{"a", "b", "c"} {
		_, err := c.BeginWALObjectDeletion(t.Context(), "p", 7, object)
		require.NoError(t, err)
	}
	for _, count := range []int{2, 1, 0} {
		pending, err := restarted.ListPendingWALObjectDeletions(t.Context(), "p", 2)
		require.NoError(t, err)
		require.Len(t, pending, count)
		for _, operation := range pending {
			require.NoError(t, restarted.CompleteWALObjectDeletion(t.Context(), "p", operation))
		}
	}
	_, err := c.ListPendingWALObjectDeletions(t.Context(), "p", 0)
	require.Error(t, err)
	_, err = c.ListPendingWALObjectDeletions(t.Context(), "p", maxRetentionObjects+1)
	require.Error(t, err)
	for _, value := range []string{"bad proto", ""} {
		require.NoError(t, store.Save(t.Context(), retentionKey("p", "pending", "corrupt"), value))
		_, err = c.ListPendingWALObjectDeletions(t.Context(), "p", 2)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
}

func TestWALObjectRetentionSerializesCaptureAndDelete(t *testing.T) {
	for _, interleave := range []string{"delete-before-pin", "pin-before-delete", "release-before-pin", "takeover-before-delete"} {
		t.Run(interleave, func(t *testing.T) {
			mockey.PatchConvey("a competing operation crosses the metadata capture/commit boundary", t, func() {
				first, second, store := retentionCatalogs(t)
				root := &streamingpb.WALObjectRetentionRoot{RootId: "snapshot", ObjectKeys: []string{"chunk"}}
				var original func(*catalog, context.Context, string, string, map[string]string, []string, ...predicates.Predicate) error
				interleaved := false
				mockey.Mock((*catalog).commitRetention).Origin(&original).To(func(c *catalog, ctx context.Context, channel, epoch string, saves map[string]string, removes []string, guards ...predicates.Predicate) error {
					if c == first && !interleaved {
						interleaved = true
						switch interleave {
						case "delete-before-pin":
							intent, err := second.BeginWALObjectDeletion(ctx, channel, 7, "chunk")
							require.NoError(t, err)
							require.NotNil(t, intent)
						case "pin-before-delete":
							require.NoError(t, second.RetainWALObjects(ctx, channel, root))
						case "release-before-pin":
							require.NoError(t, second.ReleaseWALObjects(ctx, channel, root.RootId))
						case "takeover-before-delete":
							require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey(channel), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 8})))
						}
					}
					return original(c, ctx, channel, epoch, saves, removes, guards...)
				}).Build()
				switch interleave {
				case "delete-before-pin", "release-before-pin":
					require.Error(t, first.RetainWALObjects(t.Context(), "p", root))
					require.Error(t, first.RetainWALObjects(t.Context(), "p", root))
					roots, err := first.ListWALObjectRetentionRoots(t.Context(), "p")
					require.NoError(t, err)
					require.Empty(t, roots)
				default:
					intent, err := first.BeginWALObjectDeletion(t.Context(), "p", 7, "chunk")
					require.Error(t, err)
					require.Nil(t, intent)
					intent, err = first.loadObjectDeletion(t.Context(), "p", "chunk")
					require.NoError(t, err)
					require.Nil(t, intent)
				}
				require.True(t, interleaved)
			})
		})
	}
}

func TestWALObjectRetentionRejectsInvalidMetadata(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	ctx := t.Context()
	for _, root := range []*streamingpb.WALObjectRetentionRoot{
		nil,
		{},
		{RootId: "r", ObjectKeys: []string{""}},
		{RootId: "r", ObjectKeys: []string{"o"}, Released: true},
		{RootId: "r", ObjectKeys: []string{strings.Repeat("x", maxRetentionRootBytes)}},
		{RootId: "r", ObjectKeys: make([]string, maxRetentionObjects+1)},
	} {
		require.Error(t, c.RetainWALObjects(ctx, "p", root))
	}
	require.Error(t, c.ReleaseWALObjects(ctx, "p", ""))
	_, err := c.BeginWALObjectDeletion(ctx, "p", 0, "o")
	require.Error(t, err)
	_, err = c.BeginWALObjectDeletion(ctx, "p", 7, "")
	require.Error(t, err)
	require.Error(t, c.CompleteWALObjectDeletion(ctx, "p", nil))
	require.Error(t, c.CompleteWALObjectDeletion(ctx, "p", &streamingpb.WALObjectDeletion{ObjectKey: "o", OperationId: "missing"}))
	for _, corrupt := range []string{"bad proto", string([]byte{0x1a, 0x00}), "\x0a\x01r\x18\x01"} {
		require.NoError(t, store.Save(ctx, retentionKey("p", "roots", "broken"), corrupt))
		_, err := c.BeginWALObjectDeletion(ctx, "p", 7, "o")
		require.Error(t, err, "corrupt obligations must stop deletion")
	}
	require.NoError(t, store.Remove(ctx, retentionKey("p", "roots", "broken")))
	require.NoError(t, store.Save(ctx, retentionKey("p", "deletions", "o"), "bad proto"))
	_, err = c.BeginWALObjectDeletion(ctx, "p", 7, "o")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	wrong, err := proto.Marshal(&streamingpb.WALObjectDeletion{ObjectKey: "other", OperationId: "id"})
	require.NoError(t, err)
	require.NoError(t, store.Save(ctx, retentionKey("p", "deletions", "o"), string(wrong)))
	_, err = c.BeginWALObjectDeletion(ctx, "p", 7, "o")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), "bad proto"))
	_, err = c.BeginWALObjectDeletion(ctx, "p", 7, "other")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestWALObjectRetentionCommitAmbiguity(t *testing.T) {
	for _, outcome := range []string{"applied-error", "rejected-nil", "read-error", "small-budget"} {
		t.Run(outcome, func(t *testing.T) {
			mockey.PatchConvey("no success without durable mutation evidence", t, func() {
				c, _, store := retentionCatalogs(t)
				require.NoError(t, store.Save(t.Context(), retentionPrefix("p")+"epoch", "before"))
				failure := merr.WrapErrServiceUnavailable("injected catalog failure")
				var original func(context.Context, map[string]string, []string, ...predicates.Predicate) error
				mockey.Mock(mockey.GetMethod(store, "MultiSaveAndRemove")).Origin(&original).To(func(ctx context.Context, saves map[string]string, removes []string, guards ...predicates.Predicate) error {
					if outcome == "rejected-nil" {
						return nil
					}
					if err := original(ctx, saves, removes, guards...); err != nil {
						return err
					}
					if outcome == "applied-error" {
						return failure
					}
					return nil
				}).Build()
				if outcome == "read-error" {
					mockey.Mock(mockey.GetMethod(store, "Load")).Return("", failure).Build()
				}
				if outcome == "small-budget" {
					mockey.Mock(mockey.GetMethod(store, "MaxTxnOps")).Return(1).Build()
				}
				err := c.commitRetention(t.Context(), "p", "before", map[string]string{"test": "value"}, nil)
				if outcome == "applied-error" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			})
		})
	}
}

func TestWALObjectRetentionPropagatesCatalogFailures(t *testing.T) {
	for _, step := range []string{"epoch", "released", "root", "deletion", "checkpoint", "list", "lookup", "incomplete-lookup", "no-capacity"} {
		t.Run(step, func(t *testing.T) {
			mockey.PatchConvey("unknown catalog state cannot authorize a pin or deletion", t, func() {
				c, _, store := retentionCatalogs(t)
				failure := merr.WrapErrServiceUnavailable("injected read failure")
				root := &streamingpb.WALObjectRetentionRoot{RootId: "r", ObjectKeys: []string{"o"}}
				key := map[string]string{
					"epoch": retentionPrefix("p") + "epoch", "released": retentionKey("p", "released", "r"),
					"root": retentionKey("p", "roots", "r"), "deletion": retentionKey("p", "deletions", "o"),
					"checkpoint": buildConsumeCheckpointKey("p"),
				}[step]
				var original func(context.Context, string) (string, error)
				mockey.Mock(mockey.GetMethod(store, "Load")).Origin(&original).To(func(ctx context.Context, actual string) (string, error) {
					if actual == key {
						return "", failure
					}
					return original(ctx, actual)
				}).Build()
				if step == "list" {
					mockey.Mock(mockey.GetMethod(store, "WalkWithPrefix")).Return(failure).Build()
				}
				if step == "lookup" {
					mockey.Mock(mockey.GetMethod(store, "MultiLoad")).Return(nil, failure).Build()
				}
				if step == "incomplete-lookup" {
					mockey.Mock(mockey.GetMethod(store, "MultiLoad")).Return(nil, merr.WrapErrIoKeyNotFound("missing")).Build()
				}
				if step == "no-capacity" {
					mockey.Mock(mockey.GetMethod(store, "MaxTxnOps")).Return(0).Build()
				}
				switch step {
				case "deletion":
					require.ErrorIs(t, c.CompleteWALObjectDeletion(t.Context(), "p", &streamingpb.WALObjectDeletion{ObjectKey: "o", OperationId: "id"}), failure)
				case "checkpoint":
					_, err := c.BeginWALObjectDeletion(t.Context(), "p", 7, "o")
					require.ErrorIs(t, err, failure)
				default:
					require.Error(t, c.RetainWALObjects(t.Context(), "p", root))
				}
				if step == "epoch" {
					require.ErrorIs(t, c.ReleaseWALObjects(t.Context(), "p", "r"), failure)
					require.ErrorIs(t, c.CompleteWALObjectDeletion(t.Context(), "p", &streamingpb.WALObjectDeletion{ObjectKey: "o", OperationId: "id"}), failure)
					_, err := c.BeginWALObjectDeletion(t.Context(), "p", 7, "o")
					require.ErrorIs(t, err, failure)
				}
			})
		})
	}
}

func TestWALObjectRetentionBudgetsAndEncoding(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	ctx := t.Context()
	root := &streamingpb.WALObjectRetentionRoot{RootId: "r", ObjectKeys: []string{"o"}}
	key := retentionKey("p", "roots", "r")
	require.NoError(t, store.Save(ctx, key, "bad proto"))
	require.ErrorIs(t, c.RetainWALObjects(ctx, "p", root), merr.ErrDataIntegrity)
	require.NoError(t, store.Save(ctx, key, strings.Repeat("x", maxRetentionRootBytes+1)))
	_, err := c.ListWALObjectRetentionRoots(ctx, "p")
	require.Error(t, err)
	require.NoError(t, store.Remove(ctx, key))
	root.ObjectKeys = []string{"invalid-utf8-\xff"}
	require.ErrorIs(t, c.RetainWALObjects(ctx, "p", root), merr.ErrDataIntegrity)
	_, err = c.BeginWALObjectDeletion(ctx, "p", 7, "invalid-utf8-\xff")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	mockey.PatchConvey("active roots have a bounded total metadata budget", t, func() {
		roots := make([]*streamingpb.WALObjectRetentionRoot, maxRetentionRoots)
		for i := range roots {
			roots[i] = &streamingpb.WALObjectRetentionRoot{RootId: "existing", ObjectKeys: []string{"o"}}
		}
		mockey.Mock((*catalog).ListWALObjectRetentionRoots).Return(roots, nil).Build()
		require.Error(t, c.RetainWALObjects(ctx, "p", &streamingpb.WALObjectRetentionRoot{RootId: "overflow", ObjectKeys: []string{"o"}}))
	})
}
