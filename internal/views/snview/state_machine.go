package snview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// SNLocalEvent represents a local event on the StreamingNode.
type SNLocalEvent int

const (
	// SNLocalEventReady indicates async resource preparation completed successfully.
	SNLocalEventReady SNLocalEvent = iota + 1
	// SNLocalEventUnrecoverable indicates a fatal error (e.g., WAL recovery OOM).
	SNLocalEventUnrecoverable
	// SNLocalEventWALCaughtUp indicates the WAL has caught up after recovery.
	SNLocalEventWALCaughtUp
)

// snState extends QueryViewState with local-only transient states.
// UpRecovering is not a proto state — it exists only in SN memory.
type snState int

const (
	// Map proto states to snState values.
	snStatePreparing     snState = snState(qviews.QueryViewStatePreparing)
	snStateReady         snState = snState(qviews.QueryViewStateReady)
	snStateUp            snState = snState(qviews.QueryViewStateUp)
	snStateDown          snState = snState(qviews.QueryViewStateDown)
	snStateUnrecoverable snState = snState(qviews.QueryViewStateUnrecoverable)
	snStateDropped       snState = snState(qviews.QueryViewStateDropped)

	// Local-only transient state: recovering WAL after SN crash.
	// Not representable in proto; maps to Up for external reporting.
	snStateUpRecovering snState = -1
)

// toQueryViewState converts snState to the externally visible QueryViewState.
// UpRecovering maps to Up (the persisted/external state).
func (s snState) toQueryViewState() qviews.QueryViewState {
	if s == snStateUpRecovering {
		return qviews.QueryViewStateUp
	}
	return qviews.QueryViewState(s)
}

// SNQueryViewStateMachine manages the lifecycle state machine of a single
// query view on a StreamingNode.
//
// The SN is a follower: it responds to Coord pushes and local events.
// The state machine is purely in-memory and non-blocking.
// I/O (reporting to Coord, persistence) is signaled through pending protos
// consumed via ConsumeReport / ConsumePersist.
//
// State flow:
//
//	Normal:        Preparing → Ready → Up → Down → Dropped
//	Error:         Preparing → Unrecoverable → Dropped
//	Recovery:      UpRecovering → Up (WAL caught up)
//	Recovery err:  UpRecovering → Unrecoverable → Dropped
//
// UpRecovering is a local transient state not visible in proto.
//
// Thread-safety: NOT thread-safe. The caller must serialize access.
type SNQueryViewStateMachine struct {
	state snState
	view  *viewpb.QueryViewOfShard

	pendingReport  *viewpb.QueryViewOfShard
	pendingPersist *viewpb.QueryViewOfShard
}

// NewSNQueryViewStateMachine creates a state machine when the SN receives
// a Preparing push from Coord.
//
// After construction:
//   - ConsumeReport returns Preparing (acknowledge receipt to Coord).
func NewSNQueryViewStateMachine(view *viewpb.QueryViewOfShard) *SNQueryViewStateMachine {
	sm := &SNQueryViewStateMachine{
		state: snStatePreparing,
		view:  view,
	}
	sm.pendingReport = sm.viewWithState(qviews.QueryViewStatePreparing)
	return sm
}

// RecoverSNQueryViewStateMachine reconstructs a state machine from a
// persisted Up view after SN crash recovery.
//
// After construction:
//   - State is UpRecovering (WAL must catch up before serving).
//   - No pendingReport (don't report until WAL catches up).
//   - No pendingPersist (already persisted as Up).
func RecoverSNQueryViewStateMachine(view *viewpb.QueryViewOfShard) *SNQueryViewStateMachine {
	return &SNQueryViewStateMachine{
		state: snStateUpRecovering,
		view:  view,
	}
}

// State returns the current externally visible state of the query view.
// UpRecovering is reported as Up (the external/persisted state).
func (sm *SNQueryViewStateMachine) State() qviews.QueryViewState {
	return sm.state.toQueryViewState()
}

// IsRecovering returns true if the SN is in UpRecovering state.
func (sm *SNQueryViewStateMachine) IsRecovering() bool {
	return sm.state == snStateUpRecovering
}

// View returns the original query view proto definition.
func (sm *SNQueryViewStateMachine) View() *viewpb.QueryViewOfShard {
	return sm.view
}

// OnCoordStateDelivered handles a state push from the Coordinator.
//
// Valid pushes vary by current state (see state transition table in design doc).
// Invalid pushes are silently ignored.
func (sm *SNQueryViewStateMachine) OnCoordStateDelivered(pushedState qviews.QueryViewState) {
	if sm.state == snStateDropped {
		return
	}

	switch pushedState {
	case qviews.QueryViewStatePreparing:
		sm.handleCoordPreparing()
	case qviews.QueryViewStateUp:
		sm.handleCoordUp()
	case qviews.QueryViewStateDown:
		sm.handleCoordDown()
	case qviews.QueryViewStateDropped:
		sm.handleCoordDropped()
	}
}

// OnLocalEvent handles a local event on the StreamingNode.
//
// Valid events vary by current state (see state transition table in design doc).
// Events in invalid states are silently ignored.
func (sm *SNQueryViewStateMachine) OnLocalEvent(event SNLocalEvent) {
	switch event {
	case SNLocalEventReady:
		sm.handleLocalReady()
	case SNLocalEventUnrecoverable:
		sm.handleLocalUnrecoverable()
	case SNLocalEventWALCaughtUp:
		sm.handleLocalWALCaughtUp()
	}
}

// ConsumeReport returns the view to report to the Coordinator and clears the flag.
// Returns nil if no report is needed.
func (sm *SNQueryViewStateMachine) ConsumeReport() *viewpb.QueryViewOfShard {
	v := sm.pendingReport
	sm.pendingReport = nil
	return v
}

// ConsumePersist returns the view to persist for crash recovery and clears the flag.
// Returns nil if no persistence is needed.
//
// Persist semantics:
//   - Meta.State == Up → save/overwrite recovery info.
//   - Meta.State == Down or Unrecoverable → delete persisted recovery info.
func (sm *SNQueryViewStateMachine) ConsumePersist() *viewpb.QueryViewOfShard {
	v := sm.pendingPersist
	sm.pendingPersist = nil
	return v
}

// --- Coord push handlers ---

func (sm *SNQueryViewStateMachine) handleCoordPreparing() {
	switch sm.state {
	case snStatePreparing:
		// Re-push: re-report Preparing to Coord.
		sm.pendingReport = sm.viewWithState(qviews.QueryViewStatePreparing)
	case snStateUpRecovering:
		// Re-push during recovery: don't report (wait for WAL catch-up).
	}
}

func (sm *SNQueryViewStateMachine) handleCoordUp() {
	if sm.state != snStateReady {
		return
	}
	sm.state = snStateUp
	sm.pendingReport = sm.viewWithState(qviews.QueryViewStateUp)
	sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateUp)
}

func (sm *SNQueryViewStateMachine) handleCoordDown() {
	switch sm.state {
	case snStateUp:
		sm.state = snStateDown
		sm.pendingReport = sm.viewWithState(qviews.QueryViewStateDown)
		sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateDown)
	case snStateUpRecovering:
		sm.state = snStateDown
		sm.pendingReport = sm.viewWithState(qviews.QueryViewStateDown)
		sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateDown)
	}
}

func (sm *SNQueryViewStateMachine) handleCoordDropped() {
	switch sm.state {
	case snStatePreparing, snStateDown, snStateUnrecoverable:
		sm.state = snStateDropped
		sm.pendingReport = sm.viewWithState(qviews.QueryViewStateDropped)
	}
}

// --- Local event handlers ---

func (sm *SNQueryViewStateMachine) handleLocalReady() {
	if sm.state != snStatePreparing {
		return
	}
	sm.state = snStateReady
	sm.pendingReport = sm.viewWithState(qviews.QueryViewStateReady)
}

func (sm *SNQueryViewStateMachine) handleLocalUnrecoverable() {
	switch sm.state {
	case snStatePreparing:
		sm.state = snStateUnrecoverable
		sm.pendingReport = sm.viewWithState(qviews.QueryViewStateUnrecoverable)
	case snStateUpRecovering:
		sm.state = snStateUnrecoverable
		sm.pendingReport = sm.viewWithState(qviews.QueryViewStateUnrecoverable)
		sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateUnrecoverable)
	}
}

func (sm *SNQueryViewStateMachine) handleLocalWALCaughtUp() {
	if sm.state != snStateUpRecovering {
		return
	}
	sm.state = snStateUp
	sm.pendingReport = sm.viewWithState(qviews.QueryViewStateUp)
	// No pendingPersist: already persisted as Up before crash.
}

// --- Helpers ---

func (sm *SNQueryViewStateMachine) viewWithState(state qviews.QueryViewState) *viewpb.QueryViewOfShard {
	v := proto.Clone(sm.view).(*viewpb.QueryViewOfShard)
	v.Meta.State = viewpb.QueryViewState(state)
	return v
}
