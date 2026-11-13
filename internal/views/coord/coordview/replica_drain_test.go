package coordview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	streamingcatalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestReplicaDrainGatesSharedNodesAndLostNodeReports(t *testing.T) {
	for _, unrecoverable := range []bool{false, true} {
		view := buildTestView(2)
		view.Meta.State = viewpb.QueryViewState_QueryViewStateUp
		sm := RecoverCoordQueryViewStateMachine(view)
		sm.requireReplicaDrain = true
		sm.onReplicasDrained() // A stale callback cannot retire an active View.
		require.False(t, sm.replicasDrained)
		if unrecoverable {
			sm.EnterUnrecoverable()
			sm.EnterDropping()
		} else {
			sm.EnterDown()
			sm.ConsumeFlush()
			sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
		}
		flush := sm.ConsumeFlush()
		require.NotNil(t, flush.Drain)
		require.False(t, flush.Empty())
		require.Len(t, flush.Sync, 1)
		require.IsType(t, qviews.StreamingNode{}, flush.Sync[0].WorkNode())
		// A late Ready report must not accidentally resend Dropped to QNs.
		sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
		require.Len(t, sm.ConsumeFlush().Sync, 1)
		sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
		sm.OnQueryNodeLost(qviews.QueryNode{ID: 1})
		sm.OnQueryNodeLost(qviews.QueryNode{ID: 2})
		require.Equal(t, qviews.QueryViewStateDropping, sm.State(), "node loss is not a WAL replica drain proof")
		require.Nil(t, sm.ConsumeFlush().Persist)
		sm.onReplicasDrained()
		require.Equal(t, qviews.QueryViewStateDropped, sm.State())
		require.NotNil(t, sm.ConsumeFlush().Persist)
		sm.onReplicasDrained()
		require.True(t, sm.ConsumeFlush().Empty())
	}
}

func TestReplicaDrainHoldsReferencesAcrossSourceAndCoordinatorRestart(t *testing.T) {
	ctx := t.Context()
	client, _ := kvfactory.GetEtcdAndPath()
	store := etcdkv.NewEtcdKV(client, "qv-replica-drain-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")) })
	catalog := streamingcatalog.NewCataLog(store)
	retention := catalog.(metastore.WALQueryViewRetentionCatalog)
	runtimes := catalog.(metastore.WALReplicaRuntimeCatalog)
	view := buildTestView(1)
	view.Meta.Vchannel = "p_100v0"
	view.Meta.State = viewpb.QueryViewState_QueryViewStateDown
	a := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 1, RuntimeId: 1, WalReplicaId: 1, WalReplicaRevision: 1, WalTerm: 7, ReadOnly: true}
	b := proto.CloneOf(a)
	b.RuntimeId++
	for _, runtime := range []*viewpb.StreamingWorkNode{a, b} {
		require.NoError(t, runtimes.CreateWALReplicaRuntime(ctx, runtime))
		require.NoError(t, retention.RetainWALQueryView(ctx, runtime, view.Meta))
	}
	// Reconstruct the coordinator from Down: it must rediscover the holders
	// before allowing shared QNs or their backing references to disappear.
	for attempt := range 2 {
		nodeScheduler := &capturedDirtyViewTaskScheduler{}
		coordCatalog, transport := newMockCatalog(), newMockSyncer()
		scheduler := newDirtyViewFlushScheduler(coordCatalog, transport, 128, nodeScheduler, retention)
		manager := newShardViewManager(ctx, qviews.NewShardIDFromQVMeta(view.Meta), scheduler, []*viewpb.QueryViewOfShard{view}, nil)
		require.NoError(t, nodeScheduler.snapshot()[0].Execute(ctx)) // recovered Down
		sm := manager.views[qviews.FromProtoQueryViewVersion(view.Meta.Version)]
		released := 0
		sm.releaseLoadInfo = func() { released++ }
		manager.makeOnSyncResponse(sm.Version(), snReport(view, qviews.QueryViewStateDown))(snReport(view, qviews.QueryViewStateDown))
		require.NoError(t, nodeScheduler.snapshot()[1].Execute(ctx))
		drain := nodeScheduler.snapshot()[2].(*queryViewDrainTask)
		require.ErrorAs(t, drain.Execute(ctx), new(*nodescheduler.ScheduleError))
		manager.makeOnSyncResponse(sm.Version(), snReport(view, qviews.QueryViewStateDropped))(snReport(view, qviews.QueryViewStateDropped))
		require.Zero(t, released)
		for _, synced := range transport.syncedViews {
			require.IsType(t, qviews.StreamingNode{}, synced.View.WorkNode(), "no shared QN may be dropped while either copy owns it")
		}
		// A replacement can persist while this old generation waits.
		event := dirtyPersistEvent(manager.shardID, 2)
		scheduler.Submit(event)
		tasks := nodeScheduler.snapshot()
		require.NoError(t, tasks[len(tasks)-1].Execute(ctx))
		require.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, coordCatalog.savedStates()[0])
		if attempt == 0 {
			scheduler.Close()
			retention = streamingcatalog.NewCataLog(store).(metastore.WALQueryViewRetentionCatalog)
			continue
		}
		require.NoError(t, retention.ReleaseWALQueryView(ctx, a, view.Meta))
		require.ErrorAs(t, drain.Execute(ctx), new(*nodescheduler.ScheduleError))
		require.NoError(t, runtimes.ReleaseWALReplicaRuntime(ctx, b))
		require.NoError(t, drain.Execute(ctx))
		require.Empty(t, scheduler.drains)
		tasks = nodeScheduler.snapshot()
		require.NoError(t, tasks[len(tasks)-1].Execute(ctx))
		require.False(t, scheduler.closed)
		require.Zero(t, released, "drain alone does not release shared resources before QN acknowledgment")
		manager.makeOnSyncResponse(sm.Version(), qnReport(view, 1, qviews.QueryViewStateDropped))(qnReport(view, 1, qviews.QueryViewStateDropped))
		tasks = nodeScheduler.snapshot()
		require.NoError(t, tasks[len(tasks)-1].Execute(ctx))
		require.Equal(t, 1, released)
		scheduler.Close()
	}
}

type replicaDrainCatalogStub struct {
	metastore.WALQueryViewRetentionCatalog
}

func (*replicaDrainCatalogStub) RetireWALQueryView(context.Context, *viewpb.QueryViewMeta) (bool, error) {
	return false, nil
}

func TestReplicaDrainTaskRetriesCoalescesAndCancels(t *testing.T) {
	mockey.PatchConvey("drain does not infer success from errors or cancellation", t, func() {
		catalog := &replicaDrainCatalogStub{}
		nodeScheduler := &capturedDirtyViewTaskScheduler{}
		scheduler := newDirtyViewFlushScheduler(newMockCatalog(), newMockSyncer(), 128, nodeScheduler, catalog)
		called := 0
		drain := queryViewDrain{meta: buildTestView(1).Meta, onDrained: func() { called++ }}
		scheduler.scheduleReplicaDrains([]queryViewDrain{drain, drain})
		require.Len(t, nodeScheduler.snapshot(), 1)
		task := nodeScheduler.snapshot()[0].(*queryViewDrainTask)
		mockey.Mock((*replicaDrainCatalogStub).RetireWALQueryView).Return(false, merr.WrapErrServiceUnavailable("catalog unavailable")).Build()
		require.ErrorAs(t, task.Execute(t.Context()), new(*nodescheduler.ScheduleError))
		require.Zero(t, called)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, task.Execute(ctx), context.Canceled)
		flushCtx, stop := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer stop()
		require.ErrorIs(t, scheduler.Flush(flushCtx), context.DeadlineExceeded)
		scheduler.Close()
		scheduler.scheduleReplicaDrains([]queryViewDrain{drain})
		require.Len(t, nodeScheduler.snapshot(), 1)
	})
}

func TestReplicaDrainRegistryRequiresProviders(t *testing.T) {
	_, err := RecoverShardViewRegistryWithWALReplicas(t.Context(), newMockCatalog(), newMockSyncer(), nil, nil)
	require.Error(t, err)
	registry, err := RecoverShardViewRegistryWithWALReplicas(t.Context(), newMockCatalog(), newMockSyncer(), stubDataViewRefProvider{}, &replicaDrainCatalogStub{})
	require.NoError(t, err)
	registry.Close()
}
