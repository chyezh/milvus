package meta

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Replica is a immutable type for manipulating replica meta info for replica manager.
// Performed a copy-on-write strategy to keep the consistency of the replica manager.
// So only read only operations are allowed on these type.
type Replica struct {
	replicaPB *querypb.Replica
	nodes     typeutil.UniqueSet // a helper field for manipulating replica's Available Nodes slice field.
	// always keep consistent with replicaPB.Nodes.
	outboundNodes typeutil.UniqueSet // a helper field for manipulating replica's Outbound Nodes slice field.
	// always keep consistent with replicaPB.OutboundNodes.
}

// Deprecated: may break the consistency of ReplicaManager, use Spawn instead.
func NewReplica(replica *querypb.Replica, nodes ...typeutil.UniqueSet) *Replica {
	r := proto.Clone(replica).(*querypb.Replica)
	// TODO: nodes is a bad parameter, should be removed in future.
	// keep it for old unittest.
	if len(nodes) > 0 && len(replica.Nodes) == 0 && nodes[0].Len() > 0 {
		r.Nodes = nodes[0].Collect()
	}
	return newReplica(r)
}

// newReplica creates a new replica from pb.
func newReplica(replica *querypb.Replica) *Replica {
	return &Replica{
		replicaPB:     proto.Clone(replica).(*querypb.Replica),
		nodes:         typeutil.NewUniqueSet(replica.Nodes...),
		outboundNodes: typeutil.NewUniqueSet(replica.OutboundNodes...),
	}
}

// GetID returns the id of the replica.
func (replica *Replica) GetID() typeutil.UniqueID {
	return replica.replicaPB.GetID()
}

// GetCollectionID returns the collection id of the replica.
func (replica *Replica) GetCollectionID() typeutil.UniqueID {
	return replica.replicaPB.GetCollectionID()
}

// GetResourceGroup returns the resource group name of the replica.
func (replica *Replica) GetResourceGroup() string {
	return replica.replicaPB.GetResourceGroup()
}

// GetNodes returns the available nodes of the replica.
func (replica *Replica) GetNodes() []int64 {
	return replica.nodes.Collect()
}

// GetOutboundNodes returns the in-used outbound nodes of the replica.
func (replica *Replica) GetOutboundNodes() []int64 {
	return replica.outboundNodes.Collect()
}

// AvailableNodesCount returns the count of available nodes of the replica.
func (replica *Replica) AvailableNodesCount() int {
	return replica.nodes.Len()
}

// Contains checks if the node is in available nodes of the replica.
func (replica *Replica) Contains(node int64) bool {
	return replica.nodes.Contain(node)
}

// ContainOutboundNode checks if the node is in outbound nodes of the replica.
func (replica *Replica) ContainOutboundNode(node int64) bool {
	return replica.outboundNodes.Contain(node)
}

// InUseNode checks if the node is in use by the replica (available or outbound).
func (replica *Replica) InUseNode(node int64) bool {
	return replica.nodes.Contain(node) || replica.outboundNodes.Contain(node)
}

// Deprecated: Warning, break the consistency of ReplicaManager, use `SetAvailableNodesInSameCollectionAndRG` in ReplicaManager instead.
// TODO: removed in future, only for old unittest now.
func (replica *Replica) AddNode(nodes ...int64) {
	// incoming node maybe still in outboundNodes, remove it from outboundNodes first.
	replica.outboundNodes.Remove(nodes...)
	replica.replicaPB.OutboundNodes = replica.outboundNodes.Collect()
	replica.nodes.Insert(nodes...)
	replica.replicaPB.Nodes = replica.nodes.Collect()
}

// copyForWrite returns a mutable replica for write operations.
func (replica *Replica) copyForWrite() *mutableReplica {
	return &mutableReplica{
		&Replica{
			replicaPB:     proto.Clone(replica.replicaPB).(*querypb.Replica),
			nodes:         typeutil.NewUniqueSet(replica.replicaPB.Nodes...),
			outboundNodes: typeutil.NewUniqueSet(replica.replicaPB.OutboundNodes...),
		},
	}
}

// mutableReplica is a mutable type (COW) for manipulating replica meta info for replica manager.
type mutableReplica struct {
	*Replica
}

// AddNode adds the node to available nodes of the replica.
func (replica *mutableReplica) AddNode(nodes ...int64) {
	replica.Replica.AddNode(nodes...)
}

// SetResourceGroup sets the resource group name of the replica.
func (replica *mutableReplica) SetResourceGroup(resourceGroup string) {
	replica.replicaPB.ResourceGroup = resourceGroup
}

// MoveNodeToOutbound moves the node from available nodes to outbound nodes of the replica.
// only used in replica manager.
func (replica *mutableReplica) MoveNodeToOutbound(node ...int64) {
	replica.nodes.Remove(node...)
	replica.replicaPB.Nodes = replica.nodes.Collect()
	replica.outboundNodes.Insert(node...)
	replica.replicaPB.OutboundNodes = replica.outboundNodes.Collect()
}

// RemoveNode removes the node from available nodes and outbound nodes of the replica.
// only used in replica manager.
func (replica *mutableReplica) RemoveNode(nodes ...int64) {
	replica.outboundNodes.Remove(nodes...)
	replica.replicaPB.OutboundNodes = replica.outboundNodes.Collect()
	replica.nodes.Remove(nodes...)
	replica.replicaPB.Nodes = replica.nodes.Collect()
}

// IntoReplica returns the immutable replica, After calling this method, the mutable replica should not be used again.
func (replica *mutableReplica) IntoReplica() *Replica {
	r := replica.Replica
	replica.Replica = nil
	return r
}
