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

// applyQueryNodeView applies the query node view to the coord query view.
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
		if node.NodeId == viewAtQueryNode.NodeID() {
			qv.inner.QueryNode[idx] = viewAtQueryNode.inner.QueryNode[0]
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
	qv.inner.StreamingNode = viewAtStreamingNode.inner.StreamingNode
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
func (qv *QueryViewOfShardAtCoord) transitionWhenPreparing() {
	if qv.transisitionWithNodeWhenPreparing(QueryViewNodeState(qv.inner.StreamingNode.State)) {
		return
	}
	for _, node := range qv.inner.QueryNode {
		if qv.transisitionWithNodeWhenPreparing(QueryViewNodeState(node.State)) {
			return
		}
	}
	// All node are ready, transit the state into ready.
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateReady)
	return
}

// transisitionWithNodeWhenPreparing transits the query view state with the node state when it is preparing.
func (qv *QueryViewOfShardAtCoord) transisitionWithNodeWhenPreparing(s QueryViewNodeState) bool {
	switch s {
	case QueryViewNodeStatePreparing:
		return false
	case QueryViewNodeStateUnrecoverable:
		qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateDropping)
		return true
	case QueryViewNodeStateReady:
		return false
	default:
		panic("found inconsistent state")
	}
}

// State returns the state of the query view.
func (qv *QueryViewOfShardAtCoord) State() QueryViewState {
	return QueryViewState(qv.inner.Meta.State)
}

// Version return the version of the query view.
func (qv *QueryViewOfShardAtCoord) Version() QueryViewVersion {
	v := qv.inner.Meta.Version
	return QueryViewVersion{
		DataVersion:  v.DataVersion,
		QueryVersion: v.QueryVersion,
	}
}

// newQueryViewOfShardAtCoordTxn creates a new query view transaction.
func newQueryViewOfShardAtCoordTxn(inner *viewpb.QueryViewOfShard, targetState QueryViewState) *QueryViewOfShardAtCoordTxn {
	oldState := inner.Meta.State
	inner.Meta.State = viewpb.QueryViewState(targetState)
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
func (qv *QueryViewOfShardAtCoordTxn) IntoProto() *viewpb.QueryViewOfShard {
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
