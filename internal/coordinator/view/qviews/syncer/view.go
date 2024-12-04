package syncer

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var (
	_ qviews.QueryViewAtQueryNode     = (*queryViewAtQuerynodeImpl)(nil)
	_ qviews.QueryViewAtStreamingNode = (*queryViewAtStreamingNodeImpl)(nil)
)

type queryViewAtWorkNodeBase struct {
	inner *viewpb.QueryViewOfShard
}

func (qv *queryViewAtWorkNodeBase) ShardID() qviews.ShardID {
	return qviews.NewShardIDFromQVMeta(qv.inner.Meta)
}

func (qv *queryViewAtWorkNodeBase) State() qviews.QueryViewState {
	return qviews.QueryViewState(qv.inner.Meta.State)
}

func (qv *queryViewAtWorkNodeBase) Version() qviews.QueryViewVersion {
	return qviews.FromProtoQueryViewVersion(qv.inner.Meta.Version)
}

type queryViewAtStreamingNodeImpl struct {
	queryViewAtWorkNodeBase
}

func (qv *queryViewAtStreamingNodeImpl) WorkNode() qviews.WorkNode {
	return qviews.StreamingNode()
}

func (qv *queryViewAtStreamingNodeImpl) ViewOfStreamingNode() *viewpb.QueryViewOfStreamingNode {
	return qv.inner.StreamingNode
}

type queryViewAtQuerynodeImpl struct {
	queryViewAtWorkNodeBase
}

func (qv *queryViewAtQuerynodeImpl) WorkNode() qviews.WorkNode {
	return qviews.QueryNode(qv.NodeID())
}

func (qv *queryViewAtQuerynodeImpl) NodeID() int64 {
	return qv.ViewOfQueryNode().NodeId
}

func (qv *queryViewAtQuerynodeImpl) ViewOfQueryNode() *viewpb.QueryViewOfQueryNode {
	if len(qv.inner.QueryNode) != 1 {
		panic("query view at query node should have only one query node")
	}
	return qv.inner.QueryNode[0]
}
