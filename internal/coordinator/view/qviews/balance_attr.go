package qviews

import (
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var (
	_ BalanceAttrAtWorkNode = (*BalanceAttrAtStreamingNode)(nil)
	_ BalanceAttrAtWorkNode = (*BalanceAttributesAtQueryNode)(nil)
)

func NewBalanceAttrAtWorkNodeFromProto(n WorkNode, proto *viewpb.SyncQueryViewsResponse) BalanceAttrAtWorkNode {
	switch b := proto.BalanceAttributes.(type) {
	case *viewpb.SyncQueryViewsResponse_QueryNode:
		return &BalanceAttributesAtQueryNode{
			nodeID: n.(QueryNode).ID,
			inner:  b.QueryNode,
		}
	case *viewpb.SyncQueryViewsResponse_StreamingNode:
		return &BalanceAttrAtStreamingNode{
			inner: b.StreamingNode,
		}
	default:
		panic("unknown balance attribute type")
	}
}

type BalanceAttributesAtQueryNode struct {
	nodeID int64
	inner  *viewpb.QueryNodeBalanceAttributes
}

func (qv *BalanceAttributesAtQueryNode) WorkNode() WorkNode {
	return NewQueryNode(qv.nodeID)
}

func (qv *BalanceAttributesAtQueryNode) BalanceAttrOfQueryNode() *viewpb.QueryNodeBalanceAttributes {
	return qv.inner
}

type BalanceAttrAtStreamingNode struct {
	vchannel string
	inner    *viewpb.StreamingNodeBalanceAttributes
}

func (qv *BalanceAttrAtStreamingNode) WorkNode() WorkNode {
	return NewStreamingNode(qv.vchannel)
}

func (qv *BalanceAttrAtStreamingNode) BalanceAttrOfStreamingNode() *viewpb.StreamingNodeBalanceAttributes {
	return qv.inner
}
