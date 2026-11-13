package service

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type planTestManager struct{ walmanager.Manager }

func (*planTestManager) ReconcileWALReplicas(context.Context, *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
	panic("mockey")
}

func TestWALReplicaPlanServiceReportsLocalObservation(t *testing.T) {
	mockey.PatchConvey("plan transport propagates independent local preparation", t, func() {
		response := &streamingpb.ReconcileWALReplicasResponse{PlanRevision: 4, Replicas: []*streamingpb.WALReplicaObservation{{ReplicaId: 2, ReplicaRevision: 1, NodeId: 22, State: streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING}}}
		patch := mockey.Mock((*planTestManager).ReconcileWALReplicas).Return(response, nil).Build()
		service := NewReplicaService(&planTestManager{})
		actual, err := service.ReconcileWALReplicas(t.Context(), &streamingpb.ReconcileWALReplicasRequest{})
		require.NoError(t, err)
		require.Same(t, response, actual)
		patch.UnPatch()
		mockey.Mock((*planTestManager).ReconcileWALReplicas).Return(nil, merr.WrapErrServiceUnavailable("closing")).Build()
		_, err = service.ReconcileWALReplicas(t.Context(), &streamingpb.ReconcileWALReplicasRequest{})
		require.Error(t, err)
		_, err = NewReplicaService(&replicaManager{}).ReconcileWALReplicas(t.Context(), nil)
		require.Error(t, err)
	})
}
