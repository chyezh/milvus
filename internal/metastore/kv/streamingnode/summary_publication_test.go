package streamingnode

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSummaryPublicationFencesDelayedPublishAndRecovery(t *testing.T) {
	c, other, kv := retentionCatalogs(t)
	ctx := t.Context()
	cp := replayTestCheckpoint(100, 1000)
	require.NoError(t, kv.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
	retain := func(term int64, key string) *streamingpb.WALSummaryPublication {
		root := &streamingpb.WALObjectRetentionRoot{RootId: fmt.Sprintf("summary-recovery/%d/%s", term, key), ObjectKeys: []string{key, "chunk"}}
		require.NoError(t, c.RetainWALRecoveryObjects(ctx, "p", term, root))
		return &streamingpb.WALSummaryPublication{Term: term, ObjectKey: key, RootIds: []string{root.RootId}}
	}
	absent, err := c.LoadWALSummaryPublication(ctx, "p")
	require.NoError(t, err)
	require.Nil(t, absent)
	a := retain(7, "a")
	require.NoError(t, c.PublishWALSummary(ctx, "p", "", a))
	b := retain(7, "b")
	require.NoError(t, other.PublishWALSummary(ctx, "p", "a", b))
	require.Error(t, c.PublishWALSummary(ctx, "p", "", a), "delayed completion cannot replace a successor")
	require.NoError(t, c.PublishWALSummary(ctx, "p", "a", b), "ambiguous commit is idempotent")
	loaded, err := other.LoadWALSummaryPublication(ctx, "p")
	require.NoError(t, err)
	require.True(t, proto.Equal(b, loaded))
	roots, err := c.ListWALObjectRetentionRoots(ctx, "p")
	require.NoError(t, err)
	require.Len(t, roots, 1)
	intent, err := c.BeginWALObjectDeletion(ctx, "p", 7, "b")
	require.NoError(t, err)
	require.Nil(t, intent)
	cp.Term = 8
	require.NoError(t, kv.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
	d := retain(8, "d")
	require.NoError(t, c.PublishWALSummary(ctx, "p", "b", d))
	require.Error(t, other.PublishWALSummary(ctx, "p", "b", b))
	loaded, err = NewCataLog(kv).(*catalog).LoadWALSummaryPublication(ctx, "p")
	require.NoError(t, err)
	require.True(t, proto.Equal(d, loaded))
}

func TestSummaryPublicationCASAgainstTakeoverAndRelease(t *testing.T) {
	for _, takeover := range []bool{false, true} {
		mockey.PatchConvey("publication and dependency release share the same fence", t, func() {
			c, other, kv := retentionCatalogs(t)
			ctx := t.Context()
			cp := replayTestCheckpoint(100, 1000)
			require.NoError(t, kv.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
			root := &streamingpb.WALObjectRetentionRoot{RootId: "summary-recovery/7/a", ObjectKeys: []string{"a"}}
			require.NoError(t, c.RetainWALRecoveryObjects(ctx, "p", 7, root))
			var original func(*catalog, context.Context, string, string, map[string]string, []string, ...predicates.Predicate) error
			mockey.Mock((*catalog).commitRetention).Origin(&original).To(func(cat *catalog, ctx context.Context, channel, epoch string, saves map[string]string, removes []string, guards ...predicates.Predicate) error {
				if cat == c {
					if takeover {
						cp.Term++
						require.NoError(t, kv.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
					} else {
						require.NoError(t, other.ReleaseWALObjects(ctx, "p", root.RootId))
					}
				}
				return original(cat, ctx, channel, epoch, saves, removes, guards...)
			}).Build()
			require.Error(t, c.PublishWALSummary(ctx, "p", "", &streamingpb.WALSummaryPublication{Term: 7, ObjectKey: "a", RootIds: []string{root.RootId}}))
			publication, err := c.LoadWALSummaryPublication(ctx, "p")
			require.NoError(t, err)
			require.Nil(t, publication)
		})
	}
}

func TestSummaryPublicationRejectsMissingOrInvalidDependencies(t *testing.T) {
	c, _, kv := retentionCatalogs(t)
	ctx := t.Context()
	require.NoError(t, kv.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, replayTestCheckpoint(100, 1000))))
	for _, p := range []*streamingpb.WALSummaryPublication{nil, {}, {Term: 7, ObjectKey: "a"}, {Term: 7, ObjectKey: "a", RootIds: make([]string, 129)}} {
		require.Error(t, c.PublishWALSummary(ctx, "p", "", p))
	}
	p := &streamingpb.WALSummaryPublication{Term: 7, ObjectKey: "a", RootIds: []string{"absent"}}
	require.Error(t, c.PublishWALSummary(ctx, "p", "", p))
	for _, id := range []string{"unqualified", "summary-recovery/7/missing-image"} {
		require.NoError(t, c.RetainWALObjects(ctx, "p", &streamingpb.WALObjectRetentionRoot{RootId: id, ObjectKeys: []string{"other"}}))
		p.RootIds = []string{id}
		require.Error(t, c.PublishWALSummary(ctx, "p", "", p))
	}
	for _, value := range []string{"bad", ""} {
		if value == "" {
			encoded, err := proto.Marshal(&streamingpb.WALSummaryPublication{Term: 7})
			require.NoError(t, err)
			value = string(encoded)
		}
		require.NoError(t, kv.Save(ctx, summaryPublicationKey("p"), value))
		_, err := c.LoadWALSummaryPublication(ctx, "p")
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		require.Error(t, c.PublishWALSummary(ctx, "p", "", p))
	}
	require.NoError(t, kv.Remove(ctx, summaryPublicationKey("p")))
	require.NoError(t, kv.Save(ctx, buildConsumeCheckpointKey("p"), "bad"))
	require.Error(t, c.PublishWALSummary(ctx, "p", "", p))
}
