package snview

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const (
	testCollectionID int64 = 100
	testReplicaID    int64 = 1
	testVChannel           = "v0_c0"
)

func buildTestView() *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: testCollectionID,
			ReplicaId:    testReplicaID,
			Vchannel:     testVChannel,
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
				QueryVersion: 1,
			},
			State: viewpb.QueryViewState_QueryViewStatePreparing,
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
		QueryNode: []*viewpb.QueryViewOfQueryNode{
			{
				NodeId: 1,
				Partitions: []*viewpb.QueryViewOfPartition{
					{PartitionId: 10, SegmentIds: []int64{1000}},
				},
			},
		},
	}
}

func buildUpView() *viewpb.QueryViewOfShard {
	v := buildTestView()
	v.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	return v
}

func assertPendingReportState(t *testing.T, sm *SNQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumeReport()
	require.NotNil(t, v, "expected pending report with state %s", expected)
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)
}

func assertNoPendingReport(t *testing.T, sm *SNQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumeReport(), "expected no pending report")
}

func assertPendingPersistState(t *testing.T, sm *SNQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumePersist()
	require.NotNil(t, v, "expected pending persist with state %s", expected)
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)
}

func assertNoPendingPersist(t *testing.T, sm *SNQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumePersist(), "expected no pending persist")
}

func assertNoPending(t *testing.T, sm *SNQueryViewStateMachine) {
	t.Helper()
	assertNoPendingReport(t, sm)
	assertNoPendingPersist(t, sm)
}

func drainPending(sm *SNQueryViewStateMachine) {
	sm.ConsumeReport()
	sm.ConsumePersist()
}

// ---------------------------------------------------------------------------
// Construction tests
// ---------------------------------------------------------------------------

func TestNewSNQueryViewStateMachine(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())

	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assert.False(t, sm.IsRecovering())
	assertPendingReportState(t, sm, qviews.QueryViewStatePreparing)
	assertNoPendingPersist(t, sm)
	assertNoPending(t, sm)
}

func TestNewSNQueryViewStateMachine_ViewPreserved(t *testing.T) {
	view := buildTestView()
	sm := NewSNQueryViewStateMachine(view)
	assert.Equal(t, view, sm.View())
}

func TestRecoverSNQueryViewStateMachine(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	// Externally visible state is Up, but internally UpRecovering.
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assert.True(t, sm.IsRecovering())
	// No pending report or persist on recovery.
	assertNoPending(t, sm)
}

// ---------------------------------------------------------------------------
// Normal flow: Preparing → Ready → Up → Down → Dropped
// ---------------------------------------------------------------------------

func TestNormalFlow_PreparingToReady(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnLocalEvent(SNLocalEventReady)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateReady)
	assertNoPendingPersist(t, sm)
	assertNoPending(t, sm)
}

func TestNormalFlow_ReadyToUp(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assert.False(t, sm.IsRecovering())
	assertPendingReportState(t, sm, qviews.QueryViewStateUp)
	assertPendingPersistState(t, sm, qviews.QueryViewStateUp)
	assertNoPending(t, sm)
}

func TestNormalFlow_UpToDown(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDown)
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)
	assertNoPending(t, sm)
}

func TestNormalFlow_DownToDropped(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingPersist(t, sm)
	assertNoPending(t, sm)
}

func TestNormalFlow_FullLifecycle(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())

	// Step 1: Preparing
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStatePreparing)
	assertNoPendingPersist(t, sm)

	// Step 2: LocalReady → Ready
	sm.OnLocalEvent(SNLocalEventReady)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateReady)
	assertNoPendingPersist(t, sm)

	// Step 3: CoordUp → Up
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateUp)
	assertPendingPersistState(t, sm, qviews.QueryViewStateUp)

	// Step 4: CoordDown → Down
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDown)
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)

	// Step 5: CoordDropped → Dropped
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingPersist(t, sm)

	assertNoPending(t, sm)
}

// ---------------------------------------------------------------------------
// Recovery flow: UpRecovering → Up
// ---------------------------------------------------------------------------

func TestRecovery_WALCaughtUp(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	sm.OnLocalEvent(SNLocalEventWALCaughtUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assert.False(t, sm.IsRecovering())
	assertPendingReportState(t, sm, qviews.QueryViewStateUp)
	assertNoPendingPersist(t, sm) // Already persisted as Up.
	assertNoPending(t, sm)
}

func TestRecovery_FullFastForward(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	// UpRecovering → Up (WAL caught up)
	sm.OnLocalEvent(SNLocalEventWALCaughtUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateUp)
	assertNoPendingPersist(t, sm)

	// Up → Down → Dropped (normal flow continues)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
}

// ---------------------------------------------------------------------------
// UpRecovering error: Unrecoverable
// ---------------------------------------------------------------------------

func TestUpRecovering_Unrecoverable(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	sm.OnLocalEvent(SNLocalEventUnrecoverable)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assert.False(t, sm.IsRecovering())
	assertPendingReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPending(t, sm)
}

func TestUpRecovering_UnrecoverableToDropped(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())
	sm.OnLocalEvent(SNLocalEventUnrecoverable)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingPersist(t, sm)
}

// ---------------------------------------------------------------------------
// UpRecovering + CoordDown
// ---------------------------------------------------------------------------

func TestUpRecovering_CoordDown(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assert.False(t, sm.IsRecovering())
	assertPendingReportState(t, sm, qviews.QueryViewStateDown)
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)
	assertNoPending(t, sm)
}

// ---------------------------------------------------------------------------
// UpRecovering + CoordPreparing re-push: no report
// ---------------------------------------------------------------------------

func TestUpRecovering_CoordPreparingRePush(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State()) // Still UpRecovering externally Up.
	assert.True(t, sm.IsRecovering())
	assertNoPending(t, sm) // No report during recovery.
}

// ---------------------------------------------------------------------------
// Error paths: Preparing → Unrecoverable
// ---------------------------------------------------------------------------

func TestErrorPath_PreparingToUnrecoverable(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnLocalEvent(SNLocalEventUnrecoverable)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingPersist(t, sm) // Preparing has no persist to delete.
	assertNoPending(t, sm)
}

func TestErrorPath_PreparingUnrecoverableToDropped(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventUnrecoverable)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingPersist(t, sm)
}

// ---------------------------------------------------------------------------
// Coord re-push Preparing in Preparing state
// ---------------------------------------------------------------------------

func TestCoordRePush_Preparing(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStatePreparing)
	assertNoPendingPersist(t, sm)
}

func TestCoordRePush_PreparingMultipleTimes(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	for i := 0; i < 3; i++ {
		sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
		assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
		assertPendingReportState(t, sm, qviews.QueryViewStatePreparing)
		assertNoPendingPersist(t, sm)
	}
}

// ---------------------------------------------------------------------------
// Preparing → Dropped (direct shortcut)
// ---------------------------------------------------------------------------

func TestPreparingDirectToDropped(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingPersist(t, sm)
}

// ---------------------------------------------------------------------------
// Invalid Coord pushes — silently ignored
// ---------------------------------------------------------------------------

func TestInvalidCoordPush_PreparingIgnoresUp(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_PreparingIgnoresDown(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_ReadyIgnoresPreparing(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_ReadyIgnoresDown(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_ReadyIgnoresDropped(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_UpIgnoresPreparing(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_UpIgnoresUp(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_UpIgnoresDropped(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_DownIgnoresUp(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_DownIgnoresPreparing(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_UnrecoverableIgnoresUp(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventUnrecoverable)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_UnrecoverableIgnoresDown(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventUnrecoverable)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_UpRecoveringIgnoresUp(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assert.True(t, sm.IsRecovering())
	assertNoPending(t, sm)
}

func TestInvalidCoordPush_UpRecoveringIgnoresDropped(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assert.True(t, sm.IsRecovering())
	assertNoPending(t, sm)
}

// ---------------------------------------------------------------------------
// Invalid local events — silently ignored
// ---------------------------------------------------------------------------

func TestInvalidLocalEvent_ReadyIgnoresReady(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)

	sm.OnLocalEvent(SNLocalEventReady)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidLocalEvent_ReadyIgnoresWALCaughtUp(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)

	sm.OnLocalEvent(SNLocalEventWALCaughtUp)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidLocalEvent_UpIgnoresReady(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)

	sm.OnLocalEvent(SNLocalEventReady)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidLocalEvent_UpIgnoresWALCaughtUp(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)

	sm.OnLocalEvent(SNLocalEventWALCaughtUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidLocalEvent_UpIgnoresUnrecoverable(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)

	sm.OnLocalEvent(SNLocalEventUnrecoverable)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPending(t, sm)
}

func TestInvalidLocalEvent_DownIgnoresAll(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	drainPending(sm)

	events := []SNLocalEvent{SNLocalEventReady, SNLocalEventUnrecoverable, SNLocalEventWALCaughtUp}
	for _, event := range events {
		sm.OnLocalEvent(event)
		assert.Equal(t, qviews.QueryViewStateDown, sm.State())
		assertNoPending(t, sm)
	}
}

func TestInvalidLocalEvent_PreparingIgnoresWALCaughtUp(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnLocalEvent(SNLocalEventWALCaughtUp)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)
}

// ---------------------------------------------------------------------------
// Dropped is terminal — all events ignored
// ---------------------------------------------------------------------------

func TestDroppedTerminal_IgnoresCoordPush(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	drainPending(sm)

	pushes := []qviews.QueryViewState{
		qviews.QueryViewStatePreparing,
		qviews.QueryViewStateUp,
		qviews.QueryViewStateDown,
		qviews.QueryViewStateDropped,
	}
	for _, push := range pushes {
		sm.OnCoordStateDelivered(push)
		assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
		assertNoPending(t, sm)
	}
}

func TestDroppedTerminal_IgnoresLocalEvents(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	drainPending(sm)

	events := []SNLocalEvent{SNLocalEventReady, SNLocalEventUnrecoverable, SNLocalEventWALCaughtUp}
	for _, event := range events {
		sm.OnLocalEvent(event)
		assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
		assertNoPending(t, sm)
	}
}

// ---------------------------------------------------------------------------
// Idempotency — consume clears pending
// ---------------------------------------------------------------------------

func TestIdempotency_ConsumeReportClearsPending(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())

	v := sm.ConsumeReport()
	require.NotNil(t, v)
	assertNoPendingReport(t, sm)
}

func TestIdempotency_ConsumePersistClearsPending(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)

	v := sm.ConsumePersist()
	require.NotNil(t, v)
	assertNoPendingPersist(t, sm)
}

// ---------------------------------------------------------------------------
// Persist semantics
// ---------------------------------------------------------------------------

func TestPersist_UpSavesRecoveryInfo(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assertPendingPersistState(t, sm, qviews.QueryViewStateUp)
}

func TestPersist_DownDeletesRecoveryInfo(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)
}

func TestPersist_UpRecoveringUnrecoverableDeletesRecoveryInfo(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	sm.OnLocalEvent(SNLocalEventUnrecoverable)
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestPersist_UpRecoveringDownDeletesRecoveryInfo(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)
}

func TestPersist_WALCaughtUpNoPersist(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildUpView())

	sm.OnLocalEvent(SNLocalEventWALCaughtUp)
	assertNoPendingPersist(t, sm)
}

func TestPersist_PreparingNoPersist(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	assertNoPendingPersist(t, sm)
}

func TestPersist_ReadyNoPersist(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventReady)
	assertNoPendingPersist(t, sm)
}

func TestPersist_PreparingUnrecoverableNoPersist(t *testing.T) {
	sm := NewSNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(SNLocalEventUnrecoverable)
	assertNoPendingPersist(t, sm)
}
