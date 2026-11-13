package walsummary

import (
	"context"
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestImmutableSummaryPublicationSurvivesDelayedPUT(t *testing.T) {
	mockey.PatchConvey("an old PUT arriving after GC cannot replace the committed image", t, func() {
		s, _, retention := retainedTestStore(t)
		ctx := t.Context()
		old := &streamingpb.PChannelSummaryManifest{TransformFastForwardTimeTick: map[string]uint64{"v": 100}}
		latest := &streamingpb.PChannelSummaryManifest{TransformFastForwardTimeTick: map[string]uint64{"v": 200}}
		encoded, err := marshalManifest(old)
		require.NoError(t, err)
		oldKey := s.manifestImageKey(encoded)
		var original func(*storage.LocalChunkManager, context.Context, string, []byte) error
		once := false
		mockey.Mock((*storage.LocalChunkManager).Write).Origin(&original).To(func(cm *storage.LocalChunkManager, ctx context.Context, key string, value []byte) error {
			if key == oldKey && !once {
				once = true
				require.NoError(t, s.WriteManifest(ctx, latest))
				// Model a successful early PUT, then a delayed duplicate arriving
				// after its exact object has already been physically deleted.
				require.NoError(t, original(cm, ctx, key, value))
				removed, err := s.deleteObject(ctx, key)
				require.NoError(t, err)
				require.True(t, removed)
			}
			return original(cm, ctx, key, value)
		}).Build()
		require.Error(t, s.WriteManifest(ctx, old))
		require.True(t, once)
		read, exists, err := s.ReadManifest(ctx)
		require.NoError(t, err)
		require.True(t, exists)
		require.True(t, proto.Equal(latest, read))
		publication, err := retention.LoadWALSummaryPublication(ctx, s.pchannel)
		require.NoError(t, err)
		require.NotEqual(t, oldKey, publication.ObjectKey)
		deleted, finished, err := s.sweepManifestImages(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, 1, deleted)
		require.False(t, finished)
		exists, err = s.chunkManager.Exist(ctx, oldKey)
		require.NoError(t, err)
		require.False(t, exists, "a completed deletion must also remove a delayed duplicate PUT")
		_, finished, err = s.sweepManifestImages(ctx, 10)
		require.NoError(t, err)
		require.True(t, finished)
		_, exists, err = s.ReadManifest(ctx)
		require.NoError(t, err)
		require.True(t, exists)
	})
}

func TestImmutableSummaryPublicationTakeoverAndRecovery(t *testing.T) {
	s, catalog, retention := retainedTestStore(t)
	ctx := t.Context()
	terms, err := s.ListManifestTerms(ctx, math.MaxInt64)
	require.NoError(t, err)
	require.Empty(t, terms)
	_, exists, err := s.ReadManifest(ctx)
	require.NoError(t, err)
	require.False(t, exists)
	m := newTestManager(t, s, 1<<30)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.NoError(t, drainSummary(ctx, m))
	old, err := retention.LoadWALSummaryPublication(ctx, s.pchannel)
	require.NoError(t, err)
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, s.pchannel, &metastore.WALRecoverySnapshot{ConsumeCheckpoint: &streamingpb.WALCheckpoint{Term: 2}}))
	next := NewStore(s.chunkManager, s.pchannel, 2, WithRetentionCatalog(retention))
	recovered := newTestManager(t, next, 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	require.NoError(t, drainSummary(ctx, recovered))
	require.Error(t, s.WriteManifest(ctx, m.Manifest()))
	current, err := retention.LoadWALSummaryPublication(ctx, s.pchannel)
	require.NoError(t, err)
	require.EqualValues(t, 2, current.Term)
	terms, err = next.ListManifestTerms(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, terms)
	_, exists, err = next.ReadManifestOfTerm(ctx, 1)
	require.NoError(t, err)
	require.False(t, exists)
	terms, err = next.ListManifestTerms(ctx, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, terms)
	_, _, err = next.sweepManifestImages(ctx, 10)
	require.NoError(t, err)
	exists, err = s.chunkManager.Exist(ctx, old.ObjectKey)
	require.NoError(t, err)
	require.False(t, exists)
	read, exists, err := next.ReadManifest(ctx)
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, proto.Equal(recovered.Manifest(), read))
}
