package walmanager

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (*controllerTestOpener) Close()                   { panic("mockey") }
func (*controllerTestWAL) Channel() types.PChannelInfo { panic("mockey") }
func (*controllerTestWAL) Metrics() types.WALMetrics   { panic("mockey") }

func TestManagerReplicaPlanOwnsOneLocalLifecycle(t *testing.T) {
	mockey.PatchConvey("plan admission, reads and status use the same owned runtime", t, func() {
		resource.InitForTest(t)
		nodeID := paramtable.GetNodeID()
		paramtable.SetNodeID(2)
		defer paramtable.SetNodeID(nodeID)
		instance := &controllerTestWAL{}
		channel := types.PChannelInfo{Name: "p", Term: 7, AccessMode: types.AccessModeRO}
		mockey.Mock((*controllerTestOpener).Open).Return(instance, nil).Build()
		mockey.Mock((*controllerTestOpener).Close).Return().Build()
		mockey.Mock((*controllerTestWAL).Close).Return().Build()
		mockey.Mock((*controllerTestWAL).IsAvailable).Return(true).Build()
		mockey.Mock((*controllerTestWAL).Channel).Return(channel).Build()
		mockey.Mock((*controllerTestWAL).Metrics).Return(types.ROWALMetrics{ChannelInfo: channel}).Build()
		m := newManager(&controllerTestOpener{}).(*managerImpl)
		defer m.Close()
		req := &streamingpb.ReconcileWALReplicasRequest{Pchannel: "p", Plan: controllerTestPlan()}
		_, err := m.ReconcileWALReplicas(t.Context(), req)
		require.NoError(t, err)
		controller, ok := m.replicas.Get("p")
		require.True(t, ok)
		awaitReplicaState(t, controller, req.Plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		_, err = m.ReconcileWALReplicas(t.Context(), req)
		require.NoError(t, err)
		found, err := m.GetAvailableWAL(channel)
		require.NoError(t, err)
		require.Same(t, instance, wal.Unwrap(found))
		raw, err := m.GetAvailableRawWALByPChannel("p")
		require.NoError(t, err)
		require.Same(t, instance, raw)
		metrics, err := m.Metrics()
		require.NoError(t, err)
		require.Len(t, metrics.WALMetrics, 1)
		require.Error(t, m.Open(t.Context(), channel), "single assignment cannot replace a planned runtime")
		require.Error(t, m.Remove(t.Context(), channel))
		require.NoError(t, m.Remove(t.Context(), types.PChannelInfo{Name: "legacy", Term: 1}))
		_, err = m.ReconcileWALReplicas(t.Context(), &streamingpb.ReconcileWALReplicasRequest{Pchannel: "legacy", Plan: req.Plan})
		require.Error(t, err)
		_, err = m.ReconcileWALReplicas(t.Context(), nil)
		require.Error(t, err)
		_, err = m.ReconcileWALReplicas(t.Context(), &streamingpb.ReconcileWALReplicasRequest{Pchannel: "invalid"})
		require.Error(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = m.ReconcileWALReplicas(ctx, req)
		require.ErrorIs(t, err, context.Canceled)
		m.Close()
		_, err = m.ReconcileWALReplicas(t.Context(), req)
		require.Error(t, err)
	})
}

// Deliberately lacks ReadOnlyReplicaOpener. A raw backend opener must never be
// used as a fallback for a planned RO instance.
type rawReplicaTestOpener struct{ wal.Opener }

func (*rawReplicaTestOpener) Open(context.Context, *wal.OpenOption) (wal.WAL, error) { panic("mockey") }

func TestReadOnlyPlanNeverFallsBackToBackendOpen(t *testing.T) {
	mockey.PatchConvey("unsupported RO stays unavailable without backend write capability", t, func() {
		opened := mockey.Mock((*rawReplicaTestOpener).Open).Return(nil, nil).Build()
		c := newReplicaController(t.Context(), 2, "p", &rawReplicaTestOpener{})
		defer c.close()
		awaitReplicaState(t, c, controllerTestPlan(), streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED)
		require.Zero(t, opened.Times())
		require.Nil(t, c.getWAL())
	})
}
