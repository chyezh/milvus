package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReadOnlyManagerAppliesStorageToObservedModules(t *testing.T) {
	ctx := context.Background()
	m, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1", ReadOnly: true,
		VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": {Vchannel: "v1"}},
	})
	require.NoError(t, err)
	defer m.Close()
	module := m.Module("v1")
	require.True(t, module.readOnly)
	meta := &streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1", State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING}
	view := segment.NewSegmentViewFromMetaWithConfig(meta, nil, module.segmentViewConfig())
	module.segments[1] = view
	meta.CheckpointTimeTick = 10
	meta.PersistedStorage = &streamingpb.L1SegmentPersistedStorage{ManifestPath: "immutable"}
	require.NoError(t, m.ApplySegmentPublication(ctx, meta))
	snapshot, ok := view.VisibleSnapshot("v1", qviews.DataVersion{})
	require.True(t, ok)
	require.Equal(t, "immutable", snapshot.Data.PersistedStorage.ManifestPath)
	require.NoError(t, m.ApplyVChannelPublication(&streamingpb.VChannelMeta{Vchannel: "v1", TransformMaterializedTimeTick: 20}))
	require.EqualValues(t, 20, module.vchannelView.MaterializedTimeTick())
	require.ErrorIs(t, m.ApplySegmentPublication(ctx, &streamingpb.SegmentAssignmentMeta{SegmentId: 2, Vchannel: "v1"}), merr.ErrServiceInternal)
	require.ErrorIs(t, m.ApplySegmentPublication(ctx, &streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "missing"}), merr.ErrServiceInternal)
	require.ErrorIs(t, m.ApplyVChannelPublication(&streamingpb.VChannelMeta{Vchannel: "missing"}), merr.ErrServiceInternal)
	require.Nil(t, m.Module("missing"), "storage completion cannot recreate a retired vchannel")
}
