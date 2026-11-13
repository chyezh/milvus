package streamingnode

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestWALRecoveryRootPublicationRetainsBothImagesUntilCommit(t *testing.T) {
	c, reader, store := retentionCatalogs(t)
	ctx := t.Context()
	a := &streamingpb.WALObjectRetentionRoot{RootId: "a", ObjectKeys: []string{"old"}}
	b := &streamingpb.WALObjectRetentionRoot{RootId: "b", ObjectKeys: []string{"new"}}
	require.Error(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "summary", a.RootId))
	require.Error(t, c.RetainWALRecoveryObjects(ctx, "p", 0, a))
	require.Error(t, c.RetainWALRecoveryObjects(ctx, "p", 6, a))
	require.NoError(t, c.RetainWALRecoveryObjects(ctx, "p", 7, a))
	require.NoError(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "summary", a.RootId))
	require.NoError(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "summary", a.RootId))
	require.NoError(t, c.RetainWALRecoveryObjects(ctx, "p", 7, b))
	for _, object := range []string{"old", "new"} {
		intent, err := reader.BeginWALObjectDeletion(ctx, "p", 7, object)
		require.NoError(t, err)
		require.Nil(t, intent)
	}
	require.NoError(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "summary", b.RootId))
	intent, err := reader.BeginWALObjectDeletion(ctx, "p", 7, "old")
	require.NoError(t, err)
	require.NotNil(t, intent)
	require.Error(t, c.RetainWALRecoveryObjects(ctx, "p", 7, a))
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 8})))
	require.Error(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "summary", b.RootId))
	require.NoError(t, c.CommitWALRecoveryRoot(ctx, "p", 8, "summary", b.RootId))
}

func TestWALRecoveryRootCommitRacesWithReleaseOrTakeover(t *testing.T) {
	for _, outcome := range []string{"release", "takeover", "retain-takeover"} {
		t.Run(outcome, func(t *testing.T) {
			mockey.PatchConvey("the root and owner share one guarded commit", t, func() {
				c, other, store := retentionCatalogs(t)
				root := &streamingpb.WALObjectRetentionRoot{RootId: "r", ObjectKeys: []string{"o"}}
				require.NoError(t, c.RetainWALObjects(t.Context(), "p", root))
				var original func(*catalog, context.Context, string, string, map[string]string, []string, ...predicates.Predicate) error
				changed := false
				mockey.Mock((*catalog).commitRetention).Origin(&original).To(func(actual *catalog, ctx context.Context, channel, epoch string, saves map[string]string, removes []string, guards ...predicates.Predicate) error {
					if actual == c && !changed {
						changed = true
						if outcome == "release" {
							require.NoError(t, other.ReleaseWALObjects(ctx, "p", root.RootId))
						} else {
							require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 8})))
						}
					}
					return original(actual, ctx, channel, epoch, saves, removes, guards...)
				}).Build()
				if outcome == "retain-takeover" {
					require.Error(t, c.RetainWALRecoveryObjects(t.Context(), "p", 7, root))
				} else {
					require.Error(t, c.CommitWALRecoveryRoot(t.Context(), "p", 7, "summary", root.RootId))
				}
				require.True(t, changed)
			})
		})
	}
}

func TestWALRecoveryRootValidationAndReadFailures(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	ctx := t.Context()
	root := &streamingpb.WALObjectRetentionRoot{RootId: "r", ObjectKeys: []string{"o"}}
	require.Error(t, c.CommitWALRecoveryRoot(ctx, "p", 0, "summary", "r"))
	require.Error(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "", "r"))
	require.Error(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "summary", ""))
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), "malformed"))
	require.ErrorIs(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "summary", "r"), merr.ErrDataIntegrity)
	require.ErrorIs(t, c.RetainWALRecoveryObjects(ctx, "p", 7, root), merr.ErrDataIntegrity)
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 7})))
	for _, encoded := range []string{"malformed", "\x0a\x05other"} {
		require.NoError(t, store.Save(ctx, retentionKey("p", "roots", "r"), encoded))
		require.ErrorIs(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "summary", "r"), merr.ErrDataIntegrity)
	}
	for _, failKey := range []string{retentionPrefix("p") + "epoch", buildConsumeCheckpointKey("p"), retentionKey("p", "roots", "r"), retentionKey("p", "recovery-slots", "summary")} {
		mockey.PatchConvey("catalog read failure cannot release a recovery root", t, func() {
			require.NoError(t, store.Remove(ctx, retentionKey("p", "roots", "r")))
			require.NoError(t, c.RetainWALRecoveryObjects(ctx, "p", 7, root))
			var original func(context.Context, string) (string, error)
			mockey.Mock(mockey.GetMethod(store, "Load")).Origin(&original).To(func(ctx context.Context, key string) (string, error) {
				if key == failKey {
					return "", merr.ErrServiceUnavailable
				}
				return original(ctx, key)
			}).Build()
			require.ErrorIs(t, c.CommitWALRecoveryRoot(ctx, "p", 7, "summary", "r"), merr.ErrServiceUnavailable)
			if failKey == buildConsumeCheckpointKey("p") {
				require.ErrorIs(t, c.RetainWALRecoveryObjects(ctx, "p", 7, root), merr.ErrServiceUnavailable)
			}
		})
	}
}
