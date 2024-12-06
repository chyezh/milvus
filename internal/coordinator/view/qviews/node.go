package qviews

// WorkNode is the enum type for query node and streaming node.
type WorkNode interface {
	isWorkNode()
}

// NewQueryNode creates a new query node.
func NewQueryNode(id int64) QueryNode {
	return QueryNode{ID: id}
}

type QueryNode struct {
	ID int64 // ID is the node id of streaming or querynode, if the id is -1, it means the node is a streaming node.
}

func (QueryNode) isWorkNode() {}

// NewStreamingNode creates a new streaming node.
func NewStreamingNode(vchannel string) StreamingNode {
	return StreamingNode{VChannel: vchannel}
}

type StreamingNode struct {
	VChannel string
}

func (StreamingNode) isWorkNode() {}

type BalanceAttrAtWorkNode interface {
	WorkNode() WorkNode
}
