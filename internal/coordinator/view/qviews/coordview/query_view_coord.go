package coordview

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// newQueryViewOfShardAtCoord creates a new query view of shard at coord.
func newQueryViewOfShardAtCoord(qv *viewpb.QueryViewOfShard) *queryViewAtCoord {
	return &queryViewAtCoord{
		inner: qv,
		syncRecord: newAllWorkNodeSyncRecord(qv, []qviews.QueryViewState{
			qviews.QueryViewStateReady,
			qviews.QueryViewStateUnrecoverable,
		}, qviews.QueryViewStateUnrecoverable), // The incoming pv must be a preparing state.
	}
}

// queryViewAtCoord is the representation of a query view of one shard on coord.
type queryViewAtCoord struct {
	inner      *viewpb.QueryViewOfShard
	syncRecord *workNodeSyncRecord // syncRecord is a record map to make record sync opeartion of worknode, help to achieve the 2PC.
}

// Proto returns the proto representation of the query view.
func (qv *queryViewAtCoord) Proto() *viewpb.QueryViewOfShard {
	if qv == nil {
		return nil
	}
	return proto.Clone(qv.inner).(*viewpb.QueryViewOfShard)
}

// ApplyViewFromWorkNode applies the node state view to the coord query view from the worknode.
// Return true if the view need to be sync with other.
func (qv *queryViewAtCoord) ApplyViewFromWorkNode(incomingQV qviews.QueryViewAtWorkNode) *qviews.StateTransition {
	// The version must be matched.
	if !qv.Version().EQ(incomingQV.Version()) {
		panic("version of query view not match")
	}
	previousState := qv.State()

	switch previousState {
	case qviews.QueryViewStatePreparing:
		qv.applyNodeStateViewAtPreparing(incomingQV)
	case qviews.QueryViewStateReady:
		if incomingQV.State() == qviews.QueryViewStateUp {
			qv.upView()
		}
	case qviews.QueryViewStateDown:
		if incomingQV.State() == qviews.QueryViewStateDown {
			qv.dropView()
		}
	case qviews.QueryViewStateDropping:
		if incomingQV.State() == qviews.QueryViewStateDropped {
			qv.syncRecord.MarkNodeReady(incomingQV.WorkNode())
			// If all nodes are ready, then transit the state into ready.
			if qv.syncRecord.IsAllReady() {
				qv.deleteView()
			}
		}
	case qviews.QueryViewStateUp, qviews.QueryViewStateDropped, qviews.QueryViewStateUnrecoverable:
		// Some state on coord cannot be changed by the worknode.
		// Can noly be changed by the coord itself, see `Down` and `DropView` interface.
	default:
		panic("invalid query view state")
	}

	currentState := qv.State()
	if previousState == currentState {
		return nil
	}
	return &qviews.StateTransition{
		From: previousState,
		To:   currentState,
	}
}

func (qv *queryViewAtCoord) applyNodeStateViewAtPreparing(incomingQV qviews.QueryViewAtWorkNode) {
	// Update the view of related node parts.
	switch incomingQV := incomingQV.(type) {
	case *qviews.QueryViewAtQueryNode:
		qv.applyQueryNodeView(incomingQV)
	case *qviews.QueryViewAtStreamingNode:
		qv.applyStreamingNodeView(incomingQV)
	default:
		panic("invalid incoming query view type")
	}

	// Do a state transition
	qv.transitWhenPreparing(incomingQV)
}

// applyQueryNodeView applies the query node view to the coord query view.
func (qv *queryViewAtCoord) applyQueryNodeView(viewAtQueryNode *qviews.QueryViewAtQueryNode) {
	for idx, node := range qv.inner.QueryNode {
		if node.NodeId == viewAtQueryNode.NodeID() {
			qv.inner.QueryNode[idx] = viewAtQueryNode.ViewOfQueryNode()
			return
		}
	}
	panic("query node not found in query view")
}

// applyStreamingNodeView applies the streaming node view to the coord query view.
func (qv *queryViewAtCoord) applyStreamingNodeView(viewAtStreamingNode *qviews.QueryViewAtStreamingNode) {
	qv.inner.StreamingNode = viewAtStreamingNode.ViewOfStreamingNode()
}

// transitWhenPreparing transits the query view state when it is preparing.
func (qv *queryViewAtCoord) transitWhenPreparing(incomingQV qviews.QueryViewAtWorkNode) {
	// Check the state of the query view.
	switch incomingQV.State() {
	case qviews.QueryViewStatePreparing:
		// Do nothing.
		return
	case qviews.QueryViewStateReady, qviews.QueryViewStateUp: // The querynode is ready, the streaming node may be ready or up.
		qv.syncRecord.MarkNodeReady(incomingQV.WorkNode())
		// If all nodes are ready, then transit the state into ready.
		if qv.syncRecord.IsAllReady() {
			qv.readyView()
		}
	case qviews.QueryViewStateUnrecoverable:
		qv.unrecoverableView()
	default:
		panic("found inconsistent state")
	}
}

// UnrecoverableView transits the query view state into unrecoverable.
func (qv *queryViewAtCoord) UnrecoverableView() {
	if qv.State() != qviews.QueryViewStatePreparing {
		panic("invalid state transition")
	}
	qv.unrecoverableView()
}

// Down transits the query view state into down.
func (qv *queryViewAtCoord) DownView() {
	if qv.State() != qviews.QueryViewStateUp {
		panic("invalid state transition")
	}
	qv.downView()
}

// DropView transits the query view state into dropping.
func (qv *queryViewAtCoord) DropView() {
	if qv.State() != qviews.QueryViewStateUnrecoverable {
		panic("invalid state transition")
	}
	qv.dropView()
}

// State returns the state of the query view.
func (qv *queryViewAtCoord) State() qviews.QueryViewState {
	return qviews.QueryViewState(qv.inner.Meta.State)
}

// Version return the version of the query view.
func (qv *queryViewAtCoord) Version() qviews.QueryViewVersion {
	return qviews.FromProtoQueryViewVersion(qv.inner.Meta.Version)
}

// GetPendingAckViews returns the pending ack views.
func (qv *queryViewAtCoord) GetPendingAckViews() []syncer.QueryViewAtWorkNodeWithAck {
	return qv.syncRecord.GetPendingAckViews()
}

// readyView marks the query view as ready.
func (qv *queryViewAtCoord) readyView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateReady)
	// When the state is transited into ready, we need to sent a notification to streaming node to enable the view.
	qv.syncRecord = newStreamingNodeSyncRecord(qv.inner, []qviews.QueryViewState{qviews.QueryViewStateUp})
}

// upView marks the query view as up.
func (qv *queryViewAtCoord) upView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateUp)
	qv.syncRecord = nil
}

// downView marks the query view as down.
func (qv *queryViewAtCoord) downView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateDown)
	qv.syncRecord = newStreamingNodeSyncRecord(qv.inner, []qviews.QueryViewState{qviews.QueryViewStateDropping})
}

// unrecoverableView marks the query view as unrecoverable.
func (qv *queryViewAtCoord) unrecoverableView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateUnrecoverable)
	// When the state is transited into unrecoverable, we need to do a persist opeartion with the incoming preparing view.
	qv.syncRecord = nil
}

// dropView marks the query view as dropping.
func (qv *queryViewAtCoord) dropView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateDropping)
	// When the state is transited into dropping, we need to sent a broadcast to all nodes to notify them drop these view.
	qv.syncRecord = newAllWorkNodeSyncRecord(qv.inner, []qviews.QueryViewState{qviews.QueryViewStateDropped}, qviews.QueryViewStateDropped)
}

func (qv *queryViewAtCoord) deleteView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateDropped)
	qv.syncRecord = nil
}
