package qview

import "github.com/milvus-io/milvus/internal/proto/viewpb"

// newQueryViewOfShardAtCoord creates a new query view of shard at coord.
func newQueryViewOfShardAtCoord(qv *viewpb.QueryViewOfShard) *QueryViewOfShardAtCoord {
	return &QueryViewOfShardAtCoord{
		inner:      qv,
		syncRecord: newAllWorkNodeSyncRecord(qv), // The incoming pv must be a preparing state.
		// So we need to make a new broadcast to all worknode.
		waitPersist: true,
	}
}

// QueryView is the representation of a query view of one shard.
type QueryViewOfShardAtCoord struct {
	inner       *viewpb.QueryViewOfShard
	syncRecord  *workNodeSyncRecord // syncRecord is a record map to make record sync opeartion of worknode, help to achieve the 2PC.
	waitPersist bool                // needPersistent indicates whether the query view need to be persistented.
}

// ApplyNodeStateView applies the node state view to the coord query view.
func (qv *QueryViewOfShardAtCoord) ApplyNodeStateView(incomingQV QueryViewOfShardAtWorkNode) {
	// The version must be matched.
	if !qv.Version().EQ(incomingQV.Version()) {
		panic("version of query view not match")
	}
	// Only preparing state query view can apply query node view.
	if qv.State() != QueryViewStatePreparing {
		panic("invalid state transition")
	}

	// Update the view of related node parts.
	switch incomingQV := incomingQV.(type) {
	case *QueryViewOfShardAtQueryNode:
		qv.applyQueryNodeView(incomingQV)
	case *QueryViewOfShardAtStreamingNode:
		qv.applyStreamingNodeView(incomingQV)
	default:
		panic("invalid incoming query view type")
	}

	// Do a state transition
	qv.transitWhenPreparing(incomingQV)
}

// applyQueryNodeView applies the query node view to the coord query view.
func (qv *QueryViewOfShardAtCoord) applyQueryNodeView(viewAtQueryNode *QueryViewOfShardAtQueryNode) {
	for idx, node := range qv.inner.QueryNode {
		if node.NodeId == viewAtQueryNode.NodeID() {
			qv.inner.QueryNode[idx] = viewAtQueryNode.inner.QueryNode[0]
			return
		}
	}
	panic("query node not found in query view")
}

// applyStreamingNodeView applies the streaming node view to the coord query view.
func (qv *QueryViewOfShardAtCoord) applyStreamingNodeView(viewAtStreamingNode *QueryViewOfShardAtStreamingNode) {
	qv.inner.StreamingNode = viewAtStreamingNode.inner.StreamingNode
}

// transitWhenPreparing transits the query view state when it is preparing.
func (qv *QueryViewOfShardAtCoord) transitWhenPreparing(incomingQV QueryViewOfShardAtWorkNode) {
	// Check the state of the query view.
	switch incomingQV.State() {
	case QueryViewStatePreparing:
		qv.syncRecord.MarkNodeAcked(incomingQV.WorkNode())
	case QueryViewStateUnrecoverable:
		qv.dropView()
	case QueryViewStateReady:
		qv.syncRecord.MarkNodeReady(incomingQV.WorkNode())
		if qv.syncRecord.IsAllReady() {
			// If all nodes are ready, then transit the state into ready.
			qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateReady)
			// When the state is transited into ready, we need to sent a notification to streaming node to enable the view.
			qv.syncRecord = newStreamingNodeSyncRecord()
		}
	case QueryViewStateUp:
		// If the node is marked as up (streaming node), then transit the state into up directly.
		qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateUp)
	default:
		panic("found inconsistent state")
	}
}

// dropView marks the query view as dropping.
func (qv *QueryViewOfShardAtCoord) dropView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateDropping)
	// When the state is transited into dropping, we need to sent a broadcast to all nodes to notify them dropping these view.
	// Empty the sync record to redo the sync signal.
	qv.syncRecord = newAllWorkNodeSyncRecord(qv.inner)
}

// Up transits the query view state into up.
func (qv *QueryViewOfShardAtCoord) Up() {
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
