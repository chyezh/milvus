package qviews

import "github.com/milvus-io/milvus/internal/proto/viewpb"

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

// QueryViewAtWorkNode represents the query view of a shard at a work node.
type QueryViewAtWorkNode interface {
	ShardID() ShardID

	WorkNode() WorkNode

	State() QueryViewState

	Version() QueryViewVersion
}

// QueryViewAtQueryNode represents the query view of a shard at a query node.
type QueryViewAtQueryNode interface {
	QueryViewAtWorkNode

	NodeID() int64

	ViewOfQueryNode() *viewpb.QueryViewOfQueryNode
}

// QueryViewAtStreamingNode represents the query view of a shard at a streaming node.
type QueryViewAtStreamingNode interface {
	QueryViewAtWorkNode

	ViewOfStreamingNode() *viewpb.QueryViewOfStreamingNode
}

type BalanceAttrAtWorkNode interface {
	WorkNode() WorkNode
}

type BalanceAttrAtQueryNode interface {
	BalanceAttrAtWorkNode

	BalanceAttrOfQueryNode() *viewpb.QueryNodeBalanceAttributes
}

type BalanceAttrAtStreamingNode interface {
	BalanceAttrAtWorkNode

	BalanceAttrOfStreamingNode() *viewpb.StreamingNodeBalanceAttributes
}
