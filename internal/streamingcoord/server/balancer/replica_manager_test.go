package balancer_test

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type replicaTestManager struct{ manager.ManagerClient }

func (*replicaTestManager) ReconcileWALReplicas(context.Context, int64, *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
	panic("patched with mockey")
}

func withReplicaPlans(t *testing.T, client manager.ManagerClient, reconcile ...func(context.Context, int64, *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error)) manager.ManagerClient {
	patch := mockey.Mock((*replicaTestManager).ReconcileWALReplicas).To(func(ctx context.Context, nodeID int64, req *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
		if len(reconcile) != 0 {
			return reconcile[0](ctx, nodeID, req)
		}
		return readyReplicaResponse(nodeID, req), nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return &replicaTestManager{ManagerClient: client}
}

func readyReplicaResponse(nodeID int64, req *streamingpb.ReconcileWALReplicasRequest) *streamingpb.ReconcileWALReplicasResponse {
	resp := &streamingpb.ReconcileWALReplicasResponse{PlanRevision: req.Plan.Revision}
	for _, member := range req.Plan.Replicas {
		if member.GetNode().GetServerId() == nodeID {
			resp.Replicas = append(resp.Replicas, &streamingpb.WALReplicaObservation{NodeId: nodeID, ReplicaId: member.ReplicaId, ReplicaRevision: member.ReplicaRevision, SourceTerm: req.Plan.WalTerm, State: streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY})
		}
	}
	for _, member := range req.Plan.RetiringReplicas {
		if member.GetNode().GetServerId() == nodeID {
			resp.Replicas = append(resp.Replicas, &streamingpb.WALReplicaObservation{NodeId: nodeID, ReplicaId: member.ReplicaId, ReplicaRevision: member.ReplicaRevision, State: streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED})
		}
	}
	return resp
}
