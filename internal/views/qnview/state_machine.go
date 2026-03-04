package qnview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// QNLocalEvent represents a local event on the QueryNode.
type QNLocalEvent int

const (
	// QNLocalEventReady indicates all segments have been loaded successfully.
	QNLocalEventReady QNLocalEvent = iota + 1
	// QNLocalEventUnrecoverable indicates a fatal error (e.g., OOM during loading).
	QNLocalEventUnrecoverable
)

// QNQueryViewStateMachine manages the lifecycle state machine of a single
// query view on a QueryNode.
//
// The QN is a follower: it responds to Coord pushes and local events.
// The state machine is purely in-memory and non-blocking.
// I/O (reporting to Coord) is signaled through pending proto consumed via ConsumeReport.
//
// State flow:
//
//	Normal:  Preparing → Ready → Dropped
//	Error:   Preparing → Unrecoverable → Dropped
//
// QN has no Up/Down states. Once Ready, it serves queries until Dropped.
//
// Thread-safety: NOT thread-safe. The caller must serialize access.
type QNQueryViewStateMachine struct {
	state qviews.QueryViewState
	view  *viewpb.QueryViewOfShard

	pendingReport *viewpb.QueryViewOfShard
}

// NewQNQueryViewStateMachine creates a state machine when the QN receives
// a Preparing push from Coord.
//
// After construction:
//   - ConsumeReport returns Preparing (acknowledge receipt to Coord).
func NewQNQueryViewStateMachine(view *viewpb.QueryViewOfShard) *QNQueryViewStateMachine {
	sm := &QNQueryViewStateMachine{
		state: qviews.QueryViewStatePreparing,
		view:  view,
	}
	sm.pendingReport = sm.viewWithState(qviews.QueryViewStatePreparing)
	return sm
}

// State returns the current in-memory state of the query view.
func (sm *QNQueryViewStateMachine) State() qviews.QueryViewState {
	return sm.state
}

// View returns the original query view proto definition.
func (sm *QNQueryViewStateMachine) View() *viewpb.QueryViewOfShard {
	return sm.view
}

// OnCoordStateDelivered handles a state push from the Coordinator.
//
// Valid pushes by current state:
//   - Preparing: Preparing (re-push), Dropped
//   - Ready: Dropped
//   - Unrecoverable: Dropped
//   - Dropped: ignored
//
// Invalid pushes are silently ignored.
func (sm *QNQueryViewStateMachine) OnCoordStateDelivered(pushedState qviews.QueryViewState) {
	if sm.state == qviews.QueryViewStateDropped {
		return
	}

	switch pushedState {
	case qviews.QueryViewStatePreparing:
		sm.handleCoordPreparing()
	case qviews.QueryViewStateDropped:
		sm.handleCoordDropped()
	}
}

// OnLocalEvent handles a local event on the QueryNode.
//
// Valid events by current state:
//   - Preparing + QNLocalEventReady → Ready
//   - Preparing + QNLocalEventUnrecoverable → Unrecoverable
//
// Events in other states are silently ignored.
func (sm *QNQueryViewStateMachine) OnLocalEvent(event QNLocalEvent) {
	if sm.state != qviews.QueryViewStatePreparing {
		return
	}

	switch event {
	case QNLocalEventReady:
		sm.state = qviews.QueryViewStateReady
		sm.pendingReport = sm.viewWithState(qviews.QueryViewStateReady)
	case QNLocalEventUnrecoverable:
		sm.state = qviews.QueryViewStateUnrecoverable
		sm.pendingReport = sm.viewWithState(qviews.QueryViewStateUnrecoverable)
	}
}

// ConsumeReport returns the view to report to the Coordinator and clears the flag.
// Returns nil if no report is needed.
func (sm *QNQueryViewStateMachine) ConsumeReport() *viewpb.QueryViewOfShard {
	v := sm.pendingReport
	sm.pendingReport = nil
	return v
}

// --- Coord push handlers ---

func (sm *QNQueryViewStateMachine) handleCoordPreparing() {
	if sm.state != qviews.QueryViewStatePreparing {
		return
	}
	// Re-push: re-report Preparing to Coord.
	sm.pendingReport = sm.viewWithState(qviews.QueryViewStatePreparing)
}

func (sm *QNQueryViewStateMachine) handleCoordDropped() {
	sm.state = qviews.QueryViewStateDropped
	sm.pendingReport = sm.viewWithState(qviews.QueryViewStateDropped)
}

// --- Helpers ---

func (sm *QNQueryViewStateMachine) viewWithState(state qviews.QueryViewState) *viewpb.QueryViewOfShard {
	v := proto.Clone(sm.view).(*viewpb.QueryViewOfShard)
	v.Meta.State = viewpb.QueryViewState(state)
	return v
}
