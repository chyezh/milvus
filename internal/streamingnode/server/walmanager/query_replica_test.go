package walmanager

import (
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQueryReplicaIdentityRejectsOldAndForeignRuntimes(t *testing.T) {
	for _, planned := range []bool{true, false} {
		t.Run(map[bool]string{true: "replica_plan", false: "single_assignment"}[planned], func(t *testing.T) {
			mockey.PatchConvey("queries must use the exact still-installed local runtime", t, func() {
				resource.InitForTest(t)
				nodeID := paramtable.GetNodeID()
				paramtable.SetNodeID(2)
				defer paramtable.SetNodeID(nodeID)
				instance := &controllerTestWAL{}
				channel := types.PChannelInfo{Name: "p", Term: 7, AccessMode: types.AccessModeRO}
				var available atomic.Bool
				available.Store(true)
				mockey.Mock((*controllerTestOpener).Open).Return(instance, nil).Build()
				mockey.Mock((*controllerTestOpener).Close).Return().Build()
				mockey.Mock((*controllerTestWAL).Close).Return().Build()
				mockey.Mock((*controllerTestWAL).IsAvailable).To(func(*controllerTestWAL) bool { return available.Load() }).Build()
				mockey.Mock((*controllerTestWAL).Channel).Return(channel).Build()
				m := newManager(&controllerTestOpener{}).(*managerImpl)
				defer m.Close()
				_, err := m.GetAvailableQueryReplica(&viewpb.StreamingWorkNode{Pchannel: "missing", NodeId: 2, RuntimeId: 1})
				require.Error(t, err)
				if planned {
					plan := controllerTestPlan()
					_, err = m.ReconcileWALReplicas(t.Context(), &streamingpb.ReconcileWALReplicasRequest{Pchannel: "p", Plan: plan})
					require.NoError(t, err)
					controller, _ := m.replicas.Get("p")
					awaitReplicaState(t, controller, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
				} else {
					require.NoError(t, m.Open(t.Context(), channel))
				}
				target, err := m.QueryReplicaIdentity(nopCloseWAL{instance})
				require.NoError(t, err)
				require.EqualValues(t, 2, target.NodeId)
				require.EqualValues(t, 7, target.WalTerm)
				require.True(t, target.ReadOnly)
				require.NotZero(t, target.RuntimeId)
				request := &streamingpb.WALReplicaEndpointRequest{Pchannel: "p", NodeId: 2, ReplicaId: target.WalReplicaId}
				endpoint, endpointErr := m.QueryReplicaEndpoint(request)
				if planned {
					require.NoError(t, endpointErr)
					require.True(t, proto.Equal(target, endpoint))
				} else {
					require.Error(t, endpointErr)
				}
				for _, invalid := range []*streamingpb.WALReplicaEndpointRequest{
					nil,
					{Pchannel: "p", NodeId: 3, ReplicaId: 2},
					{Pchannel: "missing", NodeId: 2, ReplicaId: 2},
					{NodeId: 2, ReplicaId: 2},
					{Pchannel: "p", NodeId: 2, ReplicaId: 99},
				} {
					_, err := m.QueryReplicaEndpoint(invalid)
					require.Error(t, err)
				}
				found, err := m.GetAvailableQueryReplica(target)
				require.NoError(t, err)
				require.Same(t, instance, wal.Unwrap(found))
				for _, mutate := range []func(*viewpb.StreamingWorkNode){
					func(v *viewpb.StreamingWorkNode) { v.NodeId++ },
					func(v *viewpb.StreamingWorkNode) { v.RuntimeId = 0 },
					func(v *viewpb.StreamingWorkNode) { v.RuntimeId++ },
					func(v *viewpb.StreamingWorkNode) { v.WalReplicaId++ },
					func(v *viewpb.StreamingWorkNode) { v.WalReplicaRevision++ },
					func(v *viewpb.StreamingWorkNode) { v.WalTerm++ },
					func(v *viewpb.StreamingWorkNode) { v.ReadOnly = false },
				} {
					foreign := proto.CloneOf(target)
					mutate(foreign)
					_, err = m.GetAvailableQueryReplica(foreign)
					require.Error(t, err)
				}
				_, err = m.QueryReplicaIdentity(&controllerTestWAL{})
				require.Error(t, err)
				available.Store(false)
				_, err = m.QueryReplicaEndpoint(request)
				require.Error(t, err)
				_, err = m.QueryReplicaIdentity(instance)
				require.Error(t, err)
				_, err = m.GetAvailableQueryReplica(target)
				require.Error(t, err)
				available.Store(true)
				if planned {
					controller, _ := m.replicas.Get("p")
					controller.mu.Lock()
					controller.runtimeID = replicaRuntimeIDs.Add(1)
					controller.mu.Unlock()
					_, err = m.GetAvailableQueryReplica(target)
					require.Error(t, err, "same-term replacement invalidates plans from the previous runtime")
					endpoint, err = m.QueryReplicaEndpoint(request)
					require.NoError(t, err)
					require.NotEqual(t, target.RuntimeId, endpoint.RuntimeId)
				}
				m.lifetime.SetState(managerStopped)
				_, err = m.QueryReplicaEndpoint(request)
				require.ErrorIs(t, err, errWALManagerClosed)
				_, err = m.QueryReplicaIdentity(instance)
				require.ErrorIs(t, err, errWALManagerClosed)
				_, err = m.GetAvailableQueryReplica(target)
				require.ErrorIs(t, err, errWALManagerClosed)
			})
		})
	}
}
