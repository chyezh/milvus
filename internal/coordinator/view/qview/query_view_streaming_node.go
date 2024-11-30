package qview

import "github.com/milvus-io/milvus/internal/proto/viewpb"

type QueryViewOfShardAtStreamingNode struct {
	inner *viewpb.QueryViewOfShard
}

// WorkNode returns the work node of the query view.
func (qv *QueryViewOfShardAtStreamingNode) WorkNode() WorkNode {
	return StreamingNode()
}

// State returns the state of the query view.
func (qv *QueryViewOfShardAtStreamingNode) State() QueryViewState {
	return QueryViewState(qv.inner.GetMeta().GetState())
}

// Version return the version of the query view.
func (qv *QueryViewOfShardAtStreamingNode) Version() QueryViewVersion {
	v := qv.inner.GetMeta().GetVersion()
	return QueryViewVersion{
		DataVersion:  v.GetDataVersion(),
		QueryVersion: v.GetQueryVersion(),
	}
}
