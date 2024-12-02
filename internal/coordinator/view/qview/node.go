package qview

import (
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

// newAllWorkNodeSyncRecord indicate to make sync opearation to all worknode.
func newAllWorkNodeSyncRecord(qv *viewpb.QueryViewOfShard) *workNodeSyncRecord {
	nodes := make(map[WorkNode]struct{}, len(qv.GetQueryNode())+1)
	nodes[StreamingNode()] = struct{}{}
	for _, node := range qv.QueryNode {
		nodes[QueryNode(node.GetNodeId())] = struct{}{}
	}
	return &workNodeSyncRecord{nodes: nodes}
}

// newStreamingNodeSyncRecord indicate to make sync operation to streaming node only.
func newStreamingNodeSyncRecord() *workNodeSyncRecord {
	return &workNodeSyncRecord{
		nodes: map[WorkNode]struct{}{StreamingNode(): {}},
	}
}

// workNodeSyncRecord records the related node sync opeartion state of a query view.
type workNodeSyncRecord struct {
	nodes map[WorkNode]struct{}
}

// MarkNodeAcked marks the node sync state as acked.
func (r *workNodeSyncRecord) MarkNodeReady(node WorkNode) {
	delete(r.nodes, node)
}

// IsAllReady returns whether all nodes are ready.
func (r *workNodeSyncRecord) IsAllReady() bool {
	return len(r.nodes) == 0
}
