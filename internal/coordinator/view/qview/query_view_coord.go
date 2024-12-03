package qview

import "github.com/milvus-io/milvus/internal/proto/viewpb"

// newQueryViewOfShardAtCoord creates a new query view of shard at coord.
func newQueryViewOfShardAtCoord(qv *viewpb.QueryViewOfShard) *QueryViewAtCoord {
	return &QueryViewAtCoord{
		inner:      qv,
		syncRecord: newAllWorkNodeSyncRecord(qv), // The incoming pv must be a preparing state.
	}
}

// QueryViewAtCoord is the representation of a query view of one shard on coord.
type QueryViewAtCoord struct {
	inner      *viewpb.QueryViewOfShard
	syncRecord *workNodeSyncRecord // syncRecord is a record map to make record sync opeartion of worknode, help to achieve the 2PC.
}

// ApplyViewFromWorkNode applies the node state view to the coord query view from the worknode.
// Return true if the view need to be sync with other.
func (qv *QueryViewAtCoord) ApplyViewFromWorkNode(incomingQV QueryViewOfShardAtWorkNode) *StateTransition {
	// The version must be matched.
	if !qv.Version().EQ(incomingQV.Version()) {
		panic("version of query view not match")
	}
	previousState := qv.State()

	switch previousState {
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
	case QueryViewStateUp, QueryViewStateDropped, QueryViewStateUnrecoverable:
		// Some state on coord cannot be changed by the worknode.
		// Can noly be changed by the coord itself, see `Down` and `DropView` interface.
	default:
		panic("invalid query view state")
	}

	currentState := qv.State()
	if previousState == currentState {
		return nil
	}
	return &StateTransition{
		From: previousState,
		To:   currentState,
	}
}

func (qv *QueryViewAtCoord) applyNodeStateViewAtPreparing(incomingQV QueryViewOfShardAtWorkNode) {
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
func (qv *QueryViewAtCoord) applyQueryNodeView(viewAtQueryNode *QueryViewOfShardAtQueryNode) {
	for idx, node := range qv.inner.QueryNode {
		if node.NodeId == viewAtQueryNode.NodeID() {
			if len(viewAtQueryNode.inner.QueryNode) != 1 {
				panic("invalid view from querynode")
			}
			qv.inner.QueryNode[idx] = viewAtQueryNode.inner.QueryNode[0]
			return
		}
	}
	panic("query node not found in query view")
}

// applyStreamingNodeView applies the streaming node view to the coord query view.
func (qv *QueryViewAtCoord) applyStreamingNodeView(viewAtStreamingNode *QueryViewOfShardAtStreamingNode) {
	qv.inner.StreamingNode = viewAtStreamingNode.inner.StreamingNode
}

// transitWhenPreparing transits the query view state when it is preparing.
func (qv *QueryViewAtCoord) transitWhenPreparing(incomingQV QueryViewOfShardAtWorkNode) {
	// Check the state of the query view.
	switch incomingQV.State() {
	case QueryViewStatePreparing:
		// Do nothing.
		return
	case QueryViewStateReady, QueryViewStateUp: // The querynode is ready, the streaming node may be ready or up.
		qv.syncRecord.MarkNodeReady(incomingQV.WorkNode())
		// If all nodes are ready, then transit the state into ready.
		if qv.syncRecord.IsAllReady() {
			qv.readyView()
		}
	case QueryViewStateUnrecoverable:
		qv.unrecoverableView()
	default:
		panic("found inconsistent state")
	}
}

// UnrecoverableView transits the query view state into unrecoverable.
func (qv *QueryViewAtCoord) UnrecoverableView() {
	if qv.State() != QueryViewStatePreparing {
		panic("invalid state transition")
	}
	qv.unrecoverableView()
}

// Down transits the query view state into down.
func (qv *QueryViewAtCoord) DownView() {
	if qv.State() != QueryViewStateUp {
		panic("invalid state transition")
	}
	qv.downView()
}

// DropView transits the query view state into dropping.
func (qv *QueryViewAtCoord) DropView() {
	if qv.State() != QueryViewStateUnrecoverable {
		panic("invalid state transition")
	}
	qv.dropView()
}

// State returns the state of the query view.
func (qv *QueryViewAtCoord) State() QueryViewState {
	return QueryViewState(qv.inner.Meta.State)
}

// Version return the version of the query view.
func (qv *QueryViewAtCoord) Version() QueryViewVersion {
	v := qv.inner.Meta.Version
	return QueryViewVersion{
		DataVersion:  v.DataVersion,
		QueryVersion: v.QueryVersion,
	}
}

// readyView marks the query view as ready.
func (qv *QueryViewAtCoord) readyView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateReady)
	// When the state is transited into ready, we need to sent a notification to streaming node to enable the view.
	qv.syncRecord = newStreamingNodeSyncRecord()
}

// upView marks the query view as up.
func (qv *QueryViewAtCoord) upView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateUp)
	qv.syncRecord = nil
}

// downView marks the query view as down.
func (qv *QueryViewAtCoord) downView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateDown)
	qv.syncRecord = newStreamingNodeSyncRecord()
}

// unrecoverableView marks the query view as unrecoverable.
func (qv *QueryViewAtCoord) unrecoverableView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateUnrecoverable)
	// When the state is transited into unrecoverable, we need to do a persist opeartion with the incoming preparing view.
	qv.syncRecord = nil
}

// dropView marks the query view as dropping.
func (qv *QueryViewAtCoord) dropView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateDropping)
	// When the state is transited into dropping, we need to sent a broadcast to all nodes to notify them drop these view.
	qv.syncRecord = newAllWorkNodeSyncRecord(qv.inner)
}

func (qv *QueryViewAtCoord) deleteView() {
	qv.inner.Meta.State = viewpb.QueryViewState(QueryViewStateDropped)
	qv.syncRecord = nil
}
