package qviews

// WorkNode is the enum type for query node and streaming node.
type WorkNode struct {
	id int64 // ID is the node id of streaming or querynode, if the id is -1, it means the node is a streaming node.
}

// IsStreaming returns whether the node is a streaming node.
func (n *WorkNode) IsStreaming() bool {
	return n.id == -1
}

// QueryNode creates a query node.
func QueryNode(id int64) WorkNode {
	return WorkNode{id: id}
}

// StreamingNode creates a streaming node.
func StreamingNode() WorkNode {
	return WorkNode{id: -1}
}

type BalanceAttrAtWorkNode interface {
	WorkNode() WorkNode
}
