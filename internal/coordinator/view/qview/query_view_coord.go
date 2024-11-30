package qview

import "github.com/milvus-io/milvus/internal/proto/viewpb"

// QueryView is the representation of a query view of one shard.
type QueryViewOfShardAtCoord struct {
	inner      *viewpb.QueryViewOfShard
	syncRecord *nodeSyncStateRecord // syncRecord is a record map to make record sync opeartion of nodes, help to achieve the 2PC.
}

// ApplyNodeStateView applies the node state view to the coord query view.
func (qv *QueryViewOfShardAtCoord) ApplyNodeStateView(incomingQV interface{}) {
	if viewAtQueryNode, ok := incomingQV.(*QueryViewOfShardAtQueryNode); ok {
		qv.applyQueryNodeView(viewAtQueryNode)
		return
	}
	if viewAtStreamingNode, ok := incomingQV.(*QueryViewOfShardAtStreamingNode); ok {
		qv.applyStreamingNodeView(viewAtStreamingNode)
		return
	}
	panic("invalid incoming query view type")
}

// ApplyQueryNodeView applies the query node view to the coord query view.
func (qv *QueryViewOfShardAtCoord) applyQueryNodeView(viewAtQueryNode *QueryViewOfShardAtQueryNode) {
	// Only preparing state query view can apply query node view.
	if qv.State() != QueryViewStatePreparing {
		panic("invalid state transition")
	}
	// The version must be matched.
	if !qv.Version().EQ(viewAtQueryNode.Version()) {
		panic("version of query view not match")
	}

	for idx, node := range qv.inner.QueryNode {
		if node.GetNodeId() == viewAtQueryNode.NodeID() {
			qv.inner.QueryNode[idx] = viewAtQueryNode.inner.GetQueryNode()[0]
			// Make a state transition, if all nodes are ready, the query view can be transited into ready.
			// else into down state.
			qv.transitionWhenPreparing()
			return
		}
	}
	panic("query node not found in query view")
}

// applyStreamingNodeView applies the streaming node view to the coord query view.
func (qv *QueryViewOfShardAtCoord) applyStreamingNodeView(viewAtStreamingNode *QueryViewOfShardAtStreamingNode) {
	if qv.State() != QueryViewStatePreparing {
		panic("invalid state transition")
	}
	if !qv.Version().EQ(viewAtStreamingNode.Version()) {
		panic("version of query view not match")
	}
	qv.inner.StreamingNode = viewAtStreamingNode.inner.GetStreamingNode()
	qv.transitionWhenPreparing()
}

// Up transits the query view state into up.
func (qv *QueryViewOfShardAtCoord) Up() *QueryViewOfShardAtCoordTxn {
	if qv.State() != QueryViewStateReady {
		panic("invalid state transition")
	}
	return newQueryViewOfShardAtCoordTxn(qv.inner, QueryViewStateUp)
}

// Drop transits the query view state into dropping.
func (qv *QueryViewOfShardAtCoord) Down() *QueryViewOfShardAtCoordTxn {
	if qv.State() == QueryViewStateDropping {
		panic("invalid state transition")
	}
	return newQueryViewOfShardAtCoordTxn(qv.inner, QueryViewStateDropping)
}

// transitionWhenPreparing transits the query view state when it is preparing.
func (qv *QueryViewOfShardAtCoord) transitionWhenPreparing() bool {
	if qv.transisitionWithNodeWhenPreparing(QueryViewNodeState(qv.inner.GetStreamingNode().GetState())) {
		return true
	}
	for _, node := range qv.inner.GetQueryNode() {
		if qv.transisitionWithNodeWhenPreparing(QueryViewNodeState(node.GetState())) {
			return true
		}
	}
	// All node are ready, transit the state into ready.
	qv.inner.Shard.State = viewpb.QueryViewState(QueryViewStateReady)
	return false
}

// transisitionWithNodeWhenPreparing transits the query view state with the node state when it is preparing.
func (qv *QueryViewOfShardAtCoord) transisitionWithNodeWhenPreparing(s QueryViewNodeState) bool {
	switch s {
	case QueryViewNodeStatePreparing:
		return false
	case QueryViewNodeStateUnrecoverable:
		qv.inner.Shard.State = viewpb.QueryViewState(QueryViewStateDown)
		return true
	case QueryViewNodeStateReady:
	default:
		panic("found inconsistent state")
	}
	return false
}

// State returns the state of the query view.
func (qv *QueryViewOfShardAtCoord) State() QueryViewState {
	return QueryViewState(qv.inner.GetShard().GetState())
}

// Version return the version of the query view.
func (qv *QueryViewOfShardAtCoord) Version() QueryViewVersion {
	v := qv.inner.GetShard().GetVersion()
	return QueryViewVersion{
		DataVersion:  v.GetDataVersion(),
		QueryVersion: v.GetQueryVersion(),
	}
}

// newQueryViewOfShardAtCoordTxn creates a new query view transaction.
func newQueryViewOfShardAtCoordTxn(inner *viewpb.QueryViewOfShardAtCoord, targetState QueryViewState) *QueryViewOfShardAtCoordTxn {
	oldState := inner.GetShard().GetState()
	inner.Shard.State = viewpb.QueryViewState(targetState)
	return &QueryViewOfShardAtCoordTxn{
		QueryViewOfShardAtCoord: &QueryViewOfShardAtCoord{inner: inner},
		oldState:                oldState,
	}
}

// QueryViewOfShardAtCoordTxn is the transaction of query view of one shard.
type QueryViewOfShardAtCoordTxn struct {
	*QueryViewOfShardAtCoord
	oldState viewpb.QueryViewState
}

// IntoProto converts the query view into proto representation.
func (qv *QueryViewOfShardAtCoordTxn) IntoProto() *viewpb.QueryViewOfShardAtCoord {
	return qv.inner
}

// Commit commits the modification.
// Because current query view is no concurency, so no need to make commit opeartion here, just a hint function.
func (qv *QueryViewOfShardAtCoordTxn) Commit() {
}

// Rollback rollbacks the modification.
func (qv *QueryViewOfShardAtCoordTxn) Rollback() {
	qv.inner.Shard.State = qv.oldState
}
