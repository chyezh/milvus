package qviews

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

// WorkNode is the enum type for query node and streaming node.
type WorkNode interface {
	fmt.Stringer

	isWorkNode()
}

// NewQueryNode creates a new query node.
func NewQueryNode(id int64) QueryNode {
	return QueryNode{ID: id}
}

// QueryNode identifies a query node by its node ID.
type QueryNode struct {
	ID int64
}

func (QueryNode) isWorkNode() {}

func (q QueryNode) String() string {
	return fmt.Sprintf("qn@%d", q.ID)
}

// NewStreamingNodeFromVChannel creates a new streaming node by vchannel.
func NewStreamingNodeFromVChannel(vchannel string) StreamingNode {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	return StreamingNode{PChannel: pchannel}
}

// StreamingNode identifies a streaming node by its bound physical channel.
type StreamingNode struct {
	PChannel string
}

func (StreamingNode) isWorkNode() {}

func (s StreamingNode) String() string {
	return fmt.Sprintf("sn@%s", s.PChannel)
}

// BalanceAttrAtWorkNode is the balance attributes reported by a work node.
type BalanceAttrAtWorkNode interface {
	WorkNode() WorkNode
}
