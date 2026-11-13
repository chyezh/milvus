package walmanager

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ wal.QueryReplicaManager = (*managerImpl)(nil)

func (m *managerImpl) QueryReplicaEndpoint(req *streamingpb.WALReplicaEndpointRequest) (*viewpb.StreamingWorkNode, error) {
	if !m.lifetime.AddIf(isGetable) {
		return nil, errWALManagerClosed
	}
	defer m.lifetime.Done()
	if req.GetNodeId() != paramtable.GetNodeID() || req.GetReplicaId() == 0 || req.GetPchannel() == "" {
		return nil, status.NewChannelFenced(req.GetPchannel())
	}
	controller, ok := m.replicas.Get(req.Pchannel)
	if !ok {
		return nil, status.NewChannelNotExist(req.Pchannel)
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.current == nil || !controller.current.IsAvailable() || controller.assignment.GetReplicaId() != req.ReplicaId {
		return nil, status.NewChannelFenced(req.Pchannel)
	}
	return controller.queryIdentityLocked(), nil
}

func (m *managerImpl) QueryReplicaIdentity(instance wal.WAL) (*viewpb.StreamingWorkNode, error) {
	if !m.lifetime.AddIf(isGetable) {
		return nil, errWALManagerClosed
	}
	defer m.lifetime.Done()
	instance = wal.Unwrap(instance)
	channel := instance.Channel()
	if controller, ok := m.replicas.Get(channel.Name); ok {
		controller.mu.Lock()
		defer controller.mu.Unlock()
		if controller.current != instance || !instance.IsAvailable() {
			return nil, status.NewChannelFenced(channel.Name)
		}
		return controller.queryIdentityLocked(), nil
	}
	if m.getCurrentWAL(channel.Name) != instance || !instance.IsAvailable() {
		return nil, status.NewChannelFenced(channel.Name)
	}
	return unplannedQueryIdentity(instance), nil
}

func (m *managerImpl) GetAvailableQueryReplica(target *viewpb.StreamingWorkNode) (wal.WAL, error) {
	if !m.lifetime.AddIf(isGetable) {
		return nil, errWALManagerClosed
	}
	defer m.lifetime.Done()
	if target.GetNodeId() != paramtable.GetNodeID() || target.GetRuntimeId() == 0 {
		return nil, status.NewChannelFenced(target.GetPchannel())
	}
	if controller, ok := m.replicas.Get(target.Pchannel); ok {
		controller.mu.Lock()
		defer controller.mu.Unlock()
		if controller.current == nil || !controller.current.IsAvailable() || !proto.Equal(target, controller.queryIdentityLocked()) {
			return nil, status.NewChannelFenced(target.Pchannel)
		}
		return nopCloseWAL{controller.current}, nil
	}
	instance := m.getCurrentWAL(target.Pchannel)
	if instance == nil || !instance.IsAvailable() || !proto.Equal(target, unplannedQueryIdentity(instance)) {
		return nil, status.NewChannelFenced(target.Pchannel)
	}
	return nopCloseWAL{instance}, nil
}

func (c *replicaController) queryIdentityLocked() *viewpb.StreamingWorkNode {
	channel := c.current.Channel()
	return &viewpb.StreamingWorkNode{
		Pchannel: c.pchannel, NodeId: c.nodeID,
		WalReplicaId: c.assignment.ReplicaId, WalReplicaRevision: c.assignment.ReplicaRevision,
		RuntimeId: c.runtimeID, WalTerm: channel.Term, ReadOnly: channel.AccessMode == types.AccessModeRO,
	}
}

// An unplanned WAL is replaced only with a fresh term. Planned replicas instead
// carry a separate local runtime ID, since RO can rebuild within the same term.
func unplannedQueryIdentity(instance wal.WAL) *viewpb.StreamingWorkNode {
	channel := instance.Channel()
	return &viewpb.StreamingWorkNode{
		Pchannel: channel.Name, NodeId: paramtable.GetNodeID(), WalTerm: channel.Term,
		RuntimeId: uint64(channel.Term), ReadOnly: channel.AccessMode == types.AccessModeRO,
	}
}
