package snview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// SNQueryViewStateMachine manages the lifecycle state machine of a single
// query view on a StreamingNode.
//
// The SN is a follower: it responds to Coord pushes and local events.
// The state machine is purely in-memory and non-blocking.
// I/O (reporting to Coord, persistence) is signaled through pending protos
// consumed via ConsumeReport / ConsumePersist.
//
// The SN only stores its own portion of the view: QueryViewMeta + QueryViewOfStreamingNode.
// It does not have access to the full QueryViewOfShard.
//
// State flow:
//
//	Normal:        Preparing → Ready → Up → Down → Dropped
//	Error:         Preparing → Unrecoverable → Dropped
//	Abort:         Preparing → Dropped, Ready → Dropped
//	Recovery:      UpRecovering → Up (WAL caught up)
//	Recovery err:  UpRecovering → Unrecoverable → Dropped
//
// UpRecovering is a StreamingNode-only state (defined in proto but only used by SN).
// Coord sees UpRecovering as Up for state machine synchronization purposes.
//
// Thread-safety: NOT thread-safe. The caller must serialize access.
type SNQueryViewStateMachine struct {
	state  qviews.QueryViewState
	meta   *viewpb.QueryViewMeta
	snView *viewpb.QueryViewOfStreamingNode

	pendingReport  *viewpb.QueryViewOfShard
	pendingPersist *viewpb.QueryViewOfShard
}

// NewSNQueryViewStateMachine creates a state machine when the SN receives
// a Preparing push from Coord.
//
// After construction:
//   - ConsumeReport returns Preparing (acknowledge receipt to Coord).
func NewSNQueryViewStateMachine(meta *viewpb.QueryViewMeta, snView *viewpb.QueryViewOfStreamingNode) *SNQueryViewStateMachine {
	sm := &SNQueryViewStateMachine{
		state:  qviews.QueryViewStatePreparing,
		meta:   meta,
		snView: snView,
	}
	sm.pendingReport = sm.buildReport()
	return sm
}

// RecoverSNQueryViewStateMachine reconstructs a state machine from a
// persisted Up view after SN crash recovery.
//
// After construction:
//   - State is UpRecovering (WAL must catch up before serving).
//   - No pendingReport (don't report until WAL catches up).
//   - No pendingPersist (already persisted as Up).
func RecoverSNQueryViewStateMachine(meta *viewpb.QueryViewMeta, snView *viewpb.QueryViewOfStreamingNode) *SNQueryViewStateMachine {
	return &SNQueryViewStateMachine{
		state:  qviews.QueryViewStateUpRecovering,
		meta:   meta,
		snView: snView,
	}
}

// State returns the current state of the query view.
func (sm *SNQueryViewStateMachine) State() qviews.QueryViewState {
	return sm.state
}

// IsRecovering returns true if the SN is in UpRecovering state.
func (sm *SNQueryViewStateMachine) IsRecovering() bool {
	return sm.state == qviews.QueryViewStateUpRecovering
}

// Meta returns the query view meta.
func (sm *SNQueryViewStateMachine) Meta() *viewpb.QueryViewMeta {
	return sm.meta
}

// SNView returns the original QueryViewOfStreamingNode.
func (sm *SNQueryViewStateMachine) SNView() *viewpb.QueryViewOfStreamingNode {
	return sm.snView
}

// OnCoordStateDelivered handles a state push from the Coordinator.
//
// In a distributed state machine, any Coord push must produce a response
// so that Coord can learn the node's current state and fast-forward.
// See design doc Section 1.1 (fast-forward logic) and Section 1.6 (Dropping).
func (sm *SNQueryViewStateMachine) OnCoordStateDelivered(pushedState qviews.QueryViewState) {
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

// OnReady reports that async resource preparation completed successfully.
// Only valid in Preparing state; ignored in other states.
func (sm *SNQueryViewStateMachine) OnReady() {
	if sm.state != qviews.QueryViewStatePreparing {
		return
	}
	sm.state = qviews.QueryViewStateReady
	sm.pendingReport = sm.buildReport()
}

// OnUnrecoverable reports a fatal error (e.g., WAL recovery OOM).
// Valid in Preparing and UpRecovering states; ignored in other states.
func (sm *SNQueryViewStateMachine) OnUnrecoverable() {
	switch sm.state {
	case qviews.QueryViewStatePreparing:
		sm.state = qviews.QueryViewStateUnrecoverable
		sm.pendingReport = sm.buildReport()
	case qviews.QueryViewStateUpRecovering:
		sm.state = qviews.QueryViewStateUnrecoverable
		sm.pendingReport = sm.buildReport()
		sm.pendingPersist = sm.buildReport()
	}
}

// OnRecoveringDone reports that the WAL has caught up after crash recovery.
// Only valid in UpRecovering state; ignored in other states.
func (sm *SNQueryViewStateMachine) OnRecoveringDone() {
	if sm.state != qviews.QueryViewStateUpRecovering {
		return
	}
	sm.state = qviews.QueryViewStateUp
	sm.pendingReport = sm.buildReport()
	// No pendingPersist: already persisted as Up before crash.
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
//   - Meta.State == Down, Unrecoverable, or Dropped → delete persisted recovery info.
func (sm *SNQueryViewStateMachine) ConsumePersist() *viewpb.QueryViewOfShard {
	v := sm.pendingPersist
	sm.pendingPersist = nil
	return v
}

// --- Coord push handlers ---

func (sm *SNQueryViewStateMachine) handleCoordPreparing() {
	switch sm.state {
	case qviews.QueryViewStatePreparing:
		// Still Preparing: local events will drive progress. No re-report needed.
	case qviews.QueryViewStateUpRecovering:
		// Recovery: don't report yet (wait for WAL catch-up, then report Up
		// to allow Coord fast-forward). See design doc Section 2.4.
	default:
		// Node has advanced past Preparing: re-report current state so Coord
		// can fast-forward (e.g., Ready, Up, Down, Unrecoverable, Dropped).
		sm.pendingReport = sm.buildReport()
	}
}

func (sm *SNQueryViewStateMachine) handleCoordUp() {
	switch sm.state {
	case qviews.QueryViewStateReady:
		// Normal transition: Ready → Up.
		sm.state = qviews.QueryViewStateUp
		sm.pendingReport = sm.buildReport()
		sm.pendingPersist = sm.buildReport()
	default:
		// Re-push or node has advanced/diverged: re-report current state
		// so Coord can fast-forward.
		sm.pendingReport = sm.buildReport()
	}
}

func (sm *SNQueryViewStateMachine) handleCoordDown() {
	switch sm.state {
	case qviews.QueryViewStateUp:
		sm.state = qviews.QueryViewStateDown
		sm.pendingReport = sm.buildReport()
		sm.pendingPersist = sm.buildReport()
	case qviews.QueryViewStateUpRecovering:
		sm.state = qviews.QueryViewStateDown
		sm.pendingReport = sm.buildReport()
		sm.pendingPersist = sm.buildReport()
	default:
		// Re-push or node has advanced/diverged: re-report current state
		// so Coord can fast-forward.
		sm.pendingReport = sm.buildReport()
	}
}

func (sm *SNQueryViewStateMachine) handleCoordDropped() {
	switch sm.state {
	case qviews.QueryViewStateUp, qviews.QueryViewStateUpRecovering:
		// Recovery info is persisted; must delete it.
		sm.state = qviews.QueryViewStateDropped
		sm.pendingReport = sm.buildReport()
		sm.pendingPersist = sm.buildReport()
	default:
		// Transition to Dropped (or re-push if already Dropped).
		// No persistence needed: either no recovery info was persisted,
		// or it was already deleted in a prior state (e.g., Down).
		sm.state = qviews.QueryViewStateDropped
		sm.pendingReport = sm.buildReport()
	}
}

// --- Helpers ---

// coordVisibleState returns the state visible to Coord.
// UpRecovering maps to Up (Coord is unaware of UpRecovering).
func (sm *SNQueryViewStateMachine) coordVisibleState() qviews.QueryViewState {
	if sm.state == qviews.QueryViewStateUpRecovering {
		return qviews.QueryViewStateUp
	}
	return sm.state
}

// buildReport constructs a QueryViewOfShard report from the SN's current state.
// The report uses the Coord-visible state (UpRecovering → Up).
func (sm *SNQueryViewStateMachine) buildReport() *viewpb.QueryViewOfShard {
	meta := proto.Clone(sm.meta).(*viewpb.QueryViewMeta)
	meta.State = viewpb.QueryViewState(sm.coordVisibleState())

	return &viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: proto.Clone(sm.snView).(*viewpb.QueryViewOfStreamingNode),
	}
}
