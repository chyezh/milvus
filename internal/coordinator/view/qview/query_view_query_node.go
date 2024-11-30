package qview

import "github.com/milvus-io/milvus/internal/proto/viewpb"

// QueryViewOfShardAtQueryNode is the view of query node.
type QueryViewOfShardAtQueryNode struct {
	inner *viewpb.QueryViewOfShard
}

// WorkNode returns the work node of the query view.
func (qv *QueryViewOfShardAtQueryNode) WorkNode() WorkNode {
	return QueryNode(qv.NodeID())
}

// Acknowledge acknowledges the query view of the query node.
func (qv *QueryViewOfShardAtQueryNode) Acknowledge(q *QueryViewOfShardAtCoord) {
	panic("implement me")
}

// NodeID returns the node id of the query node.
func (qv *QueryViewOfShardAtQueryNode) NodeID() int64 {
	return qv.inner.GetQueryNode()[0].GetNodeId()
}

// State returns the state of the query view.
func (qv *QueryViewOfShardAtQueryNode) State() QueryViewState {
	return QueryViewState(qv.inner.GetMeta().GetState())
}

// Version return the version of the view of query node.
func (qv *QueryViewOfShardAtQueryNode) Version() QueryViewVersion {
	v := qv.inner.GetMeta().GetVersion()
	return QueryViewVersion{
		DataVersion:  v.GetDataVersion(),
		QueryVersion: v.GetQueryVersion(),
	}
}
