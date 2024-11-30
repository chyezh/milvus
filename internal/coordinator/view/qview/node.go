package qview

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// WorkNode is the enum type for query node and streaming node.
type WorkNode struct {
	ID int64 // ID is the node id of streaming or querynode, if the id is -1, it means the node is a streaming node.
}

// IsStreaming returns whether the node is a streaming node.
func (n *WorkNode) IsStreaming() bool {
	return n.ID == -1
}

// QueryNode creates a query node.
func QueryNode(id int64) WorkNode {
	return WorkNode{ID: id}
}

// StreamingNode creates a streaming node.
func StreamingNode() WorkNode {
	return WorkNode{ID: -1}
}

// newNodeSyncStateRecord creates a new node sync state record.
func newNodeSyncStateRecord(qv *viewpb.QueryViewOfShardAtCoord) *nodeSyncStateRecord {
	nodes := make(map[WorkNode]NodeSyncState, len(qv.GetQueryNode())+1)
	nodes[StreamingNode()] = NodeSyncStateNot
	for _, node := range qv.QueryNode {
		nodes[QueryNode(node.GetNodeId())] = NodeSyncStateNot
	}
	return &nodeSyncStateRecord{
		nodes: nodes,
	}
}

// nodeSyncStateRecord records the related node sync opeartion state of a query view.
type nodeSyncStateRecord struct {
	allAcked bool // allAcked indicates whether all nodes are
	nodes    map[WorkNode]NodeSyncState
}

// FindAllUnsyncedNodes finds all the unsynced nodes.
func (r *nodeSyncStateRecord) FindAllUnsyncedNodes(recover bool) []WorkNode {
	if r.allAcked {
		// if all nodes has been acked, so no need to send the sync signal any more.
		return nil
	}
	var nodes []WorkNode
	for node, state := range r.nodes {
		// if the sync is recovering, a in-flight message may be lost, so re-send the message.
		if state == NodeSyncStateNot || (recover && state == NodeSyncStateInFlight) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// MarkNodeInFlight marks the node sync state as on flight.
func (r *nodeSyncStateRecord) MarkNodeInFlight(node WorkNode) {
	r.assertQueryNode(node)
	r.nodes[node] = NodeSyncStateInFlight
}

// MarkNodeAcked marks the node sync state as acked.
func (r *nodeSyncStateRecord) MarkNodeAcked(node WorkNode) {
	r.assertQueryNode(node)
	r.nodes[node] = NodeSyncStateAcked
}

func (r *nodeSyncStateRecord) assertQueryNode(node WorkNode) {
	if _, ok := r.nodes[node]; !ok {
		panic(fmt.Sprintf("node should always be found, %v", node))
	}
}
