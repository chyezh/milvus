package qviews

import (
	"github.com/gogo/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var (
	_ QueryViewAtWorkNode = (*QueryViewAtQueryNode)(nil)
	_ QueryViewAtWorkNode = (*QueryViewAtStreamingNode)(nil)
)

// QueryViewAtWorkNode represents the query view of a shard at a work node.
type QueryViewAtWorkNode interface {
	IntoProto() *viewpb.QueryViewOfShard

	ShardID() ShardID

	WorkNode() WorkNode

	State() QueryViewState

	Version() QueryViewVersion
}

type queryViewAtWorkNodeBase struct {
	inner *viewpb.QueryViewOfShard
}

func (qv *queryViewAtWorkNodeBase) ShardID() ShardID {
	return NewShardIDFromQVMeta(qv.inner.Meta)
}

func (qv *queryViewAtWorkNodeBase) State() QueryViewState {
	return QueryViewState(qv.inner.Meta.State)
}

func (qv *queryViewAtWorkNodeBase) Version() QueryViewVersion {
	return FromProtoQueryViewVersion(qv.inner.Meta.Version)
}

func (qv *queryViewAtWorkNodeBase) IntoProto() *viewpb.QueryViewOfShard {
	return qv.inner
}

// NewQueryViewAtWorkNodeFromProto creates a new query view at work node from proto.
func NewQueryViewAtWorkNodeFromProto(proto *viewpb.QueryViewOfShard) QueryViewAtWorkNode {
	if len(proto.QueryNode) == 1 {
		return &QueryViewAtQueryNode{
			queryViewAtWorkNodeBase: queryViewAtWorkNodeBase{
				inner: proto,
			},
		}
	} else if proto.StreamingNode != nil {
		return &QueryViewAtStreamingNode{
			queryViewAtWorkNodeBase: queryViewAtWorkNodeBase{
				inner: proto,
			},
		}
	}
	panic("invalid node view proto")
}

// NewQueryViewAtQueryNode creates a new query view at query node.
func NewQueryViewAtStreamingNode(meta *viewpb.QueryViewOfStreamingNode, view *viewpb.QueryViewOfStreamingNode) QueryViewAtWorkNode {
	return &QueryViewAtStreamingNode{
		queryViewAtWorkNodeBase: queryViewAtWorkNodeBase{
			inner: &viewpb.QueryViewOfShard{
				Meta:          proto.Clone(meta).(*viewpb.QueryViewMeta),
				StreamingNode: proto.Clone(view).(*viewpb.QueryViewOfStreamingNode),
			},
		},
	}
}

// QueryViewAtStreamingNode represents the query view of a shard at a streaming node.
type QueryViewAtStreamingNode struct {
	queryViewAtWorkNodeBase
}

func (qv *QueryViewAtStreamingNode) WorkNode() WorkNode {
	return StreamingNode()
}

func (qv *QueryViewAtStreamingNode) ViewOfStreamingNode() *viewpb.QueryViewOfStreamingNode {
	return qv.inner.StreamingNode
}

// NewQueryViewAtQueryNode creates a new query view at query node.
func NewQueryViewAtQueryNode(meta *viewpb.QueryViewOfStreamingNode, view *viewpb.QueryViewOfQueryNode) QueryViewAtWorkNode {
	return &QueryViewAtStreamingNode{
		queryViewAtWorkNodeBase: queryViewAtWorkNodeBase{
			inner: &viewpb.QueryViewOfShard{
				Meta: proto.Clone(meta).(*viewpb.QueryViewMeta),
				QueryNode: []*viewpb.QueryViewOfQueryNode{
					proto.Clone(view).(*viewpb.QueryViewOfQueryNode),
				},
			},
		},
	}
}

// QueryViewAtQueryNode represents the query view of a shard at a query node.
type QueryViewAtQueryNode struct {
	queryViewAtWorkNodeBase
}

func (qv *QueryViewAtQueryNode) WorkNode() WorkNode {
	return QueryNode(qv.NodeID())
}

func (qv *QueryViewAtQueryNode) NodeID() int64 {
	return qv.ViewOfQueryNode().NodeId
}

func (qv *QueryViewAtQueryNode) ViewOfQueryNode() *viewpb.QueryViewOfQueryNode {
	if len(qv.inner.QueryNode) != 1 {
		panic("query view at query node should have only one query node")
	}
	return qv.inner.QueryNode[0]
}
