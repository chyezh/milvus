package queryresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestManagerResolveLoadInfoAppliesSettingsAndIndexInfos(t *testing.T) {
	provider := fakeLoadInfoProvider{
		loadInfo: QueryViewLoadInfo{
			Settings: &viewpb.QueryViewSettings{
				RequiredPartitions: []int64{10},
				RequiredFields:     []int64{100, 101},
			},
			IndexInfos: []*indexpb.IndexInfo{
				{CollectionID: 1, FieldID: 101, IndexName: "sparse_inverted"},
			},
		},
	}
	manager := NewManager(Config{LoadInfoProvider: provider})

	view, err := manager.resolveLoadInfo(context.Background(), walview.VChannelWALView{
		CollectionID:    1,
		LoadInfoVersion: &viewpb.QueryViewLoadInfoVersion{Version: 7},
	})
	require.NoError(t, err)
	require.Equal(t, []int64{10}, view.Settings.GetRequiredPartitions())
	require.Equal(t, []int64{100, 101}, view.Settings.GetRequiredFields())
	require.Len(t, view.IndexInfos, 1)
	require.Equal(t, int64(101), view.IndexInfos[0].GetFieldID())
}

type fakeLoadInfoProvider struct {
	loadInfo QueryViewLoadInfo
	err      error
}

func (p fakeLoadInfoProvider) QueryViewLoadInfo(context.Context, int64, *viewpb.QueryViewLoadInfoVersion) (QueryViewLoadInfo, error) {
	return p.loadInfo, p.err
}
