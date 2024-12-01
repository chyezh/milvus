package qview

import "github.com/milvus-io/milvus/internal/proto/viewpb"

// newQueryViewOfShardAtCoord creates a new query view of shard at coord.
func newQueryViewOfShardAtCoord(qv *viewpb.QueryViewOfShard) *QueryViewOfShardAtCoord {
	return &QueryViewOfShardAtCoord{
		inner:      qv,
		syncRecord: newAllWorkNodeSyncRecord(qv), // The incoming pv must be a preparing state.
	}
}

// QueryView is the representation of a query view of one shard.
type QueryViewOfShardAtCoord struct {
	inner      *viewpb.QueryViewOfShard
	syncRecord *workNodeSyncRecord // syncRecord is a record map to make record sync opeartion of worknode, help to achieve the 2PC.
}

// ApplyNodeStateView applies the node state view to the coord query view.
func (qv *QueryViewOfShardAtCoord) ApplyNodeStateView(incomingQV QueryViewOfShardAtWorkNode) {
	// The version must be matched.
	if !qv.Version().EQ(incomingQV.Version()) {
		panic("version of query view not match")
	}

	switch qv.State() {
	case QueryViewStatePreparing:
		qv.applyNodeStateViewAtPreparing(incomingQV)
	case QueryViewStateReady:
		if incomingQV.State() == QueryViewStateUp {
			qv.upView()
		}
	case QueryViewStateDown:
		if incomingQV.State() == QueryViewStateDown {
			qv.dropView()
		}
	case QueryViewStateDropping:
		if incomingQV.State() == QueryViewStateDropped {
			qv.syncRecord.MarkNodeReady(incomingQV.WorkNode())
			// If all nodes are ready, then transit the state into ready.
			if qv.syncRecord.IsAllReady() {
				qv.deleteView()
			}
		}
	}
}

func (qv *QueryViewOfShardAtCoord) applyNodeStateViewAtPreparing(incomingQV QueryViewOfShardAtWorkNode) {
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

func (qv *QueryViewOfShardAtCoord) transitWhenDropping(incomingQV QueryViewOfShardAtWorkNode) {
	switch incomingQV.State() {
	case QueryViewStatePreparing:
		qv.syncRecord.MarkNodeAcked(incomingQV.WorkNode())
	}
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
	case QueryViewStateReady:
		qv.syncRecord.MarkNodeReady(incomingQV.WorkNode())
		// If all nodes are ready, then transit the state into ready.
		if qv.syncRecord.IsAllReady() {
			qv.readyView()
		}
	case QueryViewStateDropping:
		qv.dropView()
	case QueryViewStateUp:
		// If the node is marked as up (streaming node), then transit the state into up directly.
		qv.upView()
	default:
		panic("found inconsistent state")
	}
}

// Up transits the query view state into up.
func (qv *QueryViewOfShardAtCoord) Up() {
	if qv.State() != QueryViewStateReady {
		panic("invalid state transition")
	}
	qv.upView()
}

// Down transits the query view state into down.
func (qv *QueryViewOfShardAtCoord) Down() {
	if qv.State() == QueryViewStateUp {
		panic("invalid state transition")
	}
	qv.downView()
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

// readyView marks the query view as ready.
func (qv *QueryViewOfShardAtCoord) readyView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateReady)
	// When the state is transited into ready, we need to sent a notification to streaming node to enable the view.
	qv.syncRecord = newStreamingNodeSyncRecord()
}

// upView marks the query view as up.
func (qv *QueryViewOfShardAtCoord) upView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateUp)
	qv.syncRecord = nil
}

// downView marks the query view as down.
func (qv *QueryViewOfShardAtCoord) downView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateDown)
	qv.syncRecord = newStreamingNodeSyncRecord()
}

// dropView marks the query view as dropping.
func (qv *QueryViewOfShardAtCoord) dropView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateDropping)
	// When the state is transited into dropping, we need to sent a broadcast to all nodes to notify them drop these view.
	qv.syncRecord = newAllWorkNodeSyncRecord(qv.inner)
}

func (qv *QueryViewOfShardAtCoord) deleteView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateDropped)
	qv.syncRecord = nil
}
