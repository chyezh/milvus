package queryresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestManagerCloseWaitsForRunningBuild(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	dispatcher := NewDispatcher(1)
	defer dispatcher.Close()

	started := make(chan struct{})
	stopped := make(chan struct{})
	manager := NewManager(Config{
		Scheduler:  scheduler,
		Dispatcher: dispatcher,
		Builders: []QueryRuntimeModuleBuilder{blockingQueryRuntimeModuleBuilder{
			started: started,
			stopped: stopped,
		}},
	})
	version := qviews.QueryViewVersion{
		DataVersion:  qviews.DataVersion{StreamingVersion: 1},
		QueryVersion: 1,
	}
	key := qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: 1, VChannel: "v1"},
		QueryViewVersion: version,
	}
	manager.AcquireLocked(snview.AcquireResource{
		Key: key,
		Meta: &viewpb.QueryViewMeta{
			ReplicaId: 1,
			Vchannel:  "v1",
			Version:   version.IntoProto(),
		},
	}, func(*viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
		return walview.VChannelWALView{}, true
	})
	<-started

	manager.Close()
	select {
	case <-stopped:
	default:
		t.Fatal("manager close returned before the running build stopped")
	}
}

func TestManagerResolveLoadInfoAppliesLoadInfoAndIndexInfos(t *testing.T) {
	provider := fakeLoadInfoProvider{
		loadInfo: QueryViewLoadInfo{
			PartitionIDs: []int64{10},
			LoadFields:   loadFields(100, 101),
			IndexInfos: []*indexpb.IndexInfo{
				{CollectionID: 1, FieldID: 101, IndexName: "sparse_inverted"},
			},
		},
	}
	manager := NewManager(Config{LoadInfoProvider: provider})

	view, err := manager.resolveLoadInfo(context.Background(), walview.VChannelWALView{
		CollectionID:    1,
		LoadInfoVersion: 7,
	})
	require.NoError(t, err)
	require.Equal(t, []int64{10}, view.PartitionIDs)
	require.Equal(t, loadFields(100, 101), view.LoadFields)
	require.Len(t, view.IndexInfos, 1)
	require.Equal(t, int64(101), view.IndexInfos[0].GetFieldID())
}

func loadFields(fieldIDs ...int64) []*messagespb.LoadFieldConfig {
	fields := make([]*messagespb.LoadFieldConfig, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		fields = append(fields, &messagespb.LoadFieldConfig{FieldId: fieldID})
	}
	return fields
}

type fakeLoadInfoProvider struct {
	loadInfo QueryViewLoadInfo
	err      error
}

func (p fakeLoadInfoProvider) QueryViewLoadInfo(context.Context, int64, uint64) (QueryViewLoadInfo, error) {
	return p.loadInfo, p.err
}

type blockingQueryRuntimeModuleBuilder struct {
	started chan struct{}
	stopped chan struct{}
}

func (b blockingQueryRuntimeModuleBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return &blockingQueryRuntimeModule{started: b.started, stopped: b.stopped}, nil
}

type blockingQueryRuntimeModule struct {
	started chan struct{}
	stopped chan struct{}
}

func (m *blockingQueryRuntimeModule) Prepare(ctx context.Context, _ walview.VChannelWALView) error {
	close(m.started)
	<-ctx.Done()
	close(m.stopped)
	return ctx.Err()
}

func (*blockingQueryRuntimeModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {}

func (*blockingQueryRuntimeModule) Advance(qviews.DataVersion) {}

func (*blockingQueryRuntimeModule) Close() {}
