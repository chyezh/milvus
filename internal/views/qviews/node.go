package qviews

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// WorkNodeKey uniquely identifies a work node. Used as map key.
type WorkNodeKey = string

// NodeType identifies the type of a work node.
type NodeType int

const (
	NodeTypeStreamingNode NodeType = iota + 1
	NodeTypeQueryNode
)

// String returns "sn" for StreamingNode and "qn" for QueryNode.
func (t NodeType) String() string {
	switch t {
	case NodeTypeStreamingNode:
		return "sn"
	case NodeTypeQueryNode:
		return "qn"
	default:
		return "unknown"
	}
}

// WorkNode is the enum type for query node and streaming node.
type WorkNode interface {
	fmt.Stringer

	// Key returns a unique identifier for the node, suitable for use as a map key.
	Key() WorkNodeKey

	// NodeType returns the type of this work node.
	NodeType() NodeType

	isWorkNode()
}

// NewQueryNode creates a new query node.
func NewQueryNode(id int64) QueryNode {
	return QueryNode{ID: id}
}

// QueryNode identifies a query node by its node ID.
type QueryNode struct {
	ID int64
}

func (QueryNode) isWorkNode()        {}
func (QueryNode) NodeType() NodeType { return NodeTypeQueryNode }

func (q QueryNode) Key() WorkNodeKey {
	return fmt.Sprintf("%s@%d", q.NodeType(), q.ID)
}

func (q QueryNode) String() string {
	return q.Key()
}

// NewStreamingNodeFromVChannel creates a new streaming node by vchannel.
func NewStreamingNodeFromVChannel(vchannel string) StreamingNode {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	return StreamingNode{PChannel: pchannel}
}

// StreamingNode identifies a streaming node by its bound physical channel.
type StreamingNode struct {
	PChannel           string
	NodeID             int64
	WALReplicaID       uint64
	WALReplicaRevision uint64
	RuntimeID          uint64
	WALTerm            int64
	ReadOnly           bool
}

func StreamingNodeFromProto(node *viewpb.StreamingWorkNode) StreamingNode {
	return StreamingNode{
		PChannel: node.Pchannel, NodeID: node.NodeId, WALReplicaID: node.WalReplicaId,
		WALReplicaRevision: node.WalReplicaRevision, RuntimeID: node.RuntimeId, WALTerm: node.WalTerm, ReadOnly: node.ReadOnly,
	}
}

func (s StreamingNode) IntoProto() *viewpb.StreamingWorkNode {
	return &viewpb.StreamingWorkNode{
		Pchannel: s.PChannel, NodeId: s.NodeID, WalReplicaId: s.WALReplicaID,
		WalReplicaRevision: s.WALReplicaRevision, RuntimeId: s.RuntimeID, WalTerm: s.WALTerm, ReadOnly: s.ReadOnly,
	}
}

func (StreamingNode) isWorkNode()        {}
func (StreamingNode) NodeType() NodeType { return NodeTypeStreamingNode }

func (s StreamingNode) Key() WorkNodeKey {
	if s.NodeID != 0 {
		return fmt.Sprintf("%s@%s/%d/%d/%d/%d", s.NodeType(), s.PChannel, s.NodeID, s.WALReplicaID, s.WALReplicaRevision, s.RuntimeID)
	}
	return fmt.Sprintf("%s@%s", s.NodeType(), s.PChannel)
}

func (s StreamingNode) String() string {
	return s.Key()
}
