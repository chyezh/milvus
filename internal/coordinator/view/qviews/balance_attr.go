package qviews

import (
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var (
	_ BalanceAttrAtWorkNode = (*BalanceAttrAtStreamingNode)(nil)
	_ BalanceAttrAtWorkNode = (*BalanceAttributesAtQueryNode)(nil)
)

type BalanceAttributesAtQueryNode struct {
	nodeID int64
	inner  *viewpb.QueryNodeBalanceAttributes
}

func (qv *BalanceAttributesAtQueryNode) WorkNode() WorkNode {
	return QueryNode(qv.nodeID)
}

func (qv *BalanceAttributesAtQueryNode) BalanceAttrOfQueryNode() *viewpb.QueryNodeBalanceAttributes {
	return qv.inner
}

type BalanceAttrAtStreamingNode struct {
	inner *viewpb.StreamingNodeBalanceAttributes
}

func (qv *BalanceAttrAtStreamingNode) WorkNode() WorkNode {
	return StreamingNode()
}

func (qv *BalanceAttrAtStreamingNode) BalanceAttrOfStreamingNode() *viewpb.StreamingNodeBalanceAttributes {
	return qv.inner
}
