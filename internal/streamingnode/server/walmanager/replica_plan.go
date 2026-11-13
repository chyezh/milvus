package walmanager

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ReplicaPlanManager admits complete persisted plans. The response describes
// this node only; a primary's observation never grants readiness to a follower.
type ReplicaPlanManager interface {
	ReconcileWALReplicas(context.Context, *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error)
}

func (m *managerImpl) ReconcileWALReplicas(ctx context.Context, req *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
	if !m.lifetime.AddIf(isOpenable) {
		return nil, errWALManagerClosed
	}
	defer m.lifetime.Done()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.GetPchannel() == "" {
		return nil, merr.WrapErrServiceInternalMsg("WAL replica plan has no channel")
	}
	if err := validateReplicaPlan(req.GetPlan()); err != nil {
		return nil, err
	}
	// A term has exactly one writer/event-source lifetime. Mixing the legacy
	// single-assignment protocol into an already opened channel cannot safely
	// recreate that source at sequence zero.
	m.assignmentMu.Lock()
	defer m.assignmentMu.Unlock()
	if _, ok := m.wltMap.Get(req.Pchannel); ok {
		return nil, merr.WrapErrServiceUnavailable("WAL channel already belongs to single-assignment management")
	}
	controller, ok := m.replicas.Get(req.Pchannel)
	if !ok {
		controller = newReplicaController(m.openingCtx, paramtable.GetNodeID(), req.Pchannel, m.opener)
		m.replicas.Insert(req.Pchannel, controller)
	}
	return controller.reconcile(req.Plan)
}

func (m *managerImpl) getCurrentWAL(pchannel string) wal.WAL {
	if controller, ok := m.replicas.Get(pchannel); ok {
		return controller.getWAL()
	}
	if lifetime, ok := m.wltMap.Get(pchannel); ok {
		return lifetime.GetWAL()
	}
	return nil
}

var _ ReplicaPlanManager = (*managerImpl)(nil)
