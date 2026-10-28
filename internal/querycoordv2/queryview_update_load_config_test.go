package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestQueryViewUpdateLoadConfig(t *testing.T) {
	s, _ := newHandoffServer(t)
	store := s.qviewsRuntime.loadConfigStore
	cfg := store.GetConfig(1).Clone()
	cfg.Replicas = []*loadmgr.ReplicaAssignment{{ReplicaID: 11, ResourceGroup: "a"}, {ReplicaID: 12, ResourceGroup: "a"}, {ReplicaID: 13, ResourceGroup: "b"}}
	cfg.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 100}}
	cfg.RequiredFields = []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}}
	cfg.UserSpecifiedReplicaMode = true
	require.NoError(t, store.Put(t.Context(), cfg))
	var headers []*messagespb.AlterLoadConfigMessageHeader
	for _, p := range []*mockey.Mocker{
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(stubBroadcaster{}, nil).Build(),
		mockey.Mock(utils.AssignReplica).To(func(_ context.Context, _ *meta.Meta, groups []string, replicas int32, _ bool) (map[string]int, error) {
			return utils.ReplicaNumberByResourceGroup(groups, replicas)
		}).Build(),
		mockey.Mock(stubBroadcaster.Broadcast).To(func(_ stubBroadcaster, ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			h := message.MustAsMutableAlterLoadConfigMessageV2(msg).Header()
			headers = append(headers, h)
			return nil, store.Put(ctx, loadmgr.FromAlterLoadConfigMessage(h))
		}).Build(),
	} {
		t.Cleanup(func() { p.UnPatch() })
	}
	require.NoError(t, s.updateLoadConfig(t.Context(), []int64{1}, 3, []string{"a", "b", "b"}))
	require.Len(t, headers, 1)
	require.Equal(t, cfg.LoadFields, headers[0].LoadFields)
	require.Equal(t, cfg.RequiredFields, headers[0].RequiredFields)
	require.Equal(t, cfg.PartitionIDs, headers[0].PartitionIds)
	require.True(t, headers[0].UserSpecifiedReplicaMode)
	require.Equal(t, "b", store.GetConfig(1).Replicas[1].ResourceGroup)
	require.NoError(t, s.updateLoadConfig(t.Context(), []int64{1}, 3, []string{"b", "a", "b"}))
	require.Len(t, headers, 1, "reordered identical multiplicities do not broadcast")

	params := paramtable.Get()
	for key, value := range map[string]string{
		params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key:                "3",
		params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key:               "a,a,b",
		params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key: "false",
	} {
		old := params.GetWithDefault(key, "")
		require.NoError(t, params.Save(key, value))
		t.Cleanup(func() { require.NoError(t, params.Save(key, old)) })
	}
	watcher := &LoadConfigWatcher{s: s, notifier: syncutil.NewAsyncTaskNotifier[struct{}]()}
	require.NoError(t, watcher.applyLoadConfigChanges())
	require.Len(t, headers, 1, "user placement is protected")
	require.NoError(t, params.Save(params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "true"))
	require.NoError(t, watcher.applyLoadConfigChanges())
	require.Len(t, headers, 2)
	require.True(t, store.GetConfig(1).UserSpecifiedReplicaMode, "force does not erase user intent")
	require.NoError(t, watcher.applyLoadConfigChanges())
	require.Len(t, headers, 2, "periodic reconciliation is idempotent")
	require.NoError(t, store.Remove(t.Context(), 1))
	require.NoError(t, s.updateLoadConfig(t.Context(), []int64{1}, 3, []string{"a"}))
	require.Len(t, headers, 2, "released collection is not resurrected")
}

func TestQueryViewLoadConfigWatcherLifecycle(t *testing.T) {
	s, _ := newHandoffServer(t)
	entered := make(chan struct{}, 1)
	patch := mockey.Mock((*Server).updateCollectionLoadConfig).To(func(_ *Server, ctx context.Context, id int64, _ int32, _ []string, defaults bool) error {
		require.EqualValues(t, 1, id)
		require.True(t, defaults)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}).Build()
	defer patch.UnPatch()
	s.watchLoadConfigChanges()
	select {
	case <-entered: // Startup reconciles recovered configurations without an event.
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not reconcile at startup")
	}
	require.Len(t, s.loadConfigWatcher.handlers, 3)
	// A slow broadcast must not block config delivery. Notifications coalesce.
	for range 1000 {
		s.loadConfigWatcher.Trigger()
	}
	stopped := make(chan struct{})
	go func() { s.loadConfigWatcher.Close(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not cancel outstanding work")
	}
}
