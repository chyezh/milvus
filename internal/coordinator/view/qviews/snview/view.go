package snview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// queryViewAtStreamingNode is the view of streaming node.
type queryViewAtStreamingNode struct {
	inner *viewpb.QueryViewOfShard
}

// WorkNode returns the work node of the query view.
func (qv *queryViewAtStreamingNode) WorkNode() qviews.WorkNode {
	return qviews.StreamingNode()
}

// State returns the state of the query view.
func (qv *queryViewAtStreamingNode) State() qviews.QueryViewState {
	return qviews.QueryViewState(qv.inner.GetMeta().GetState())
}

// Version return the version of the query view.
func (qv *queryViewAtStreamingNode) Version() qviews.QueryViewVersion {
	v := qv.inner.GetMeta().GetVersion()
	return qviews.QueryViewVersion{
		DataVersion:  v.GetDataVersion(),
		QueryVersion: v.GetQueryVersion(),
	}
}
