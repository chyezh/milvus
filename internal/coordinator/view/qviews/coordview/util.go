package coordview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// newAllWorkNodeSyncRecord indicate to make sync opearation to all worknode.
func newAllWorkNodeSyncRecord(qv *viewpb.QueryViewOfShard) *workNodeSyncRecord {
	nodes := make(map[qviews.WorkNode]struct{}, len(qv.GetQueryNode())+1)
	nodes[qviews.StreamingNode()] = struct{}{}
	for _, node := range qv.QueryNode {
		nodes[qviews.QueryNode(node.GetNodeId())] = struct{}{}
	}
	return &workNodeSyncRecord{nodes: nodes}
}

// newStreamingNodeSyncRecord indicate to make sync operation to streaming node only.
func newStreamingNodeSyncRecord() *workNodeSyncRecord {
	return &workNodeSyncRecord{
		nodes: map[qviews.WorkNode]struct{}{qviews.StreamingNode(): {}},
	}
}

// workNodeSyncRecord records the related node sync opeartion state of a query view.
type workNodeSyncRecord struct {
	nodes map[qviews.WorkNode]struct{}
}

// MarkNodeAcked marks the node sync state as acked.
func (r *workNodeSyncRecord) MarkNodeReady(node qviews.WorkNode) {
	delete(r.nodes, node)
}

// IsAllReady returns whether all nodes are ready.
func (r *workNodeSyncRecord) IsAllReady() bool {
	return len(r.nodes) == 0
}
