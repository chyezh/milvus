package adaptor

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryPlanPrunesSNUsingSelectedViewDataVersion(t *testing.T) {
	sealedAt := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 2}
	patch := mockey.Mock((*queryresource.QueryRuntime).MayHaveVisibleGrowingSegments).To(func(_ *queryresource.QueryRuntime, version qviews.DataVersion, growing uint64, transform uint64, partitions []int64) bool {
		require.Equal(t, uint64(100), growing)
		require.Equal(t, uint64(90), transform)
		require.Equal(t, []int64{100}, partitions)
		return sealedAt.GT(version)
	}).Build()
	defer patch.UnPatch()
	options := queryPlanWorkNodeOptions{runtime: &queryresource.QueryRuntime{}, partitionIDs: []int64{100}, mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}}
	view := &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{Vchannel: "p_1v0", Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 9}}}, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}
	require.True(t, queryPlanIncludesStreamingNode(view, options))
	view.Meta.Version.DataVersion = sealedAt.IntoProto()
	require.False(t, queryPlanIncludesStreamingNode(view, options))
}
