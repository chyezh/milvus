package syncer

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var (
	_ qviews.BalanceAttrAtStreamingNode = (*balanceAttrAtStreamingNodeImpl)(nil)
	_ qviews.BalanceAttrAtQueryNode     = (*balanceAttributesAtQueryNodeImpl)(nil)
)

type balanceAttributesAtQueryNodeImpl struct {
	nodeID int64
	inner  *viewpb.QueryNodeBalanceAttributes
}

func (qv *balanceAttributesAtQueryNodeImpl) WorkNode() qviews.WorkNode {
	return qviews.QueryNode(qv.nodeID)
}

func (qv *balanceAttributesAtQueryNodeImpl) BalanceAttrOfQueryNode() *viewpb.QueryNodeBalanceAttributes {
	return qv.inner
}

type balanceAttrAtStreamingNodeImpl struct {
	inner *viewpb.StreamingNodeBalanceAttributes
}

func (qv *balanceAttrAtStreamingNodeImpl) WorkNode() qviews.WorkNode {
	return qviews.StreamingNode()
}

func (qv *balanceAttrAtStreamingNodeImpl) BalanceAttrOfStreamingNode() *viewpb.StreamingNodeBalanceAttributes {
	return qv.inner
}
