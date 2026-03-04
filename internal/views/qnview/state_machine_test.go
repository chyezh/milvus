package qnview

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

func assertPendingReportState(t *testing.T, sm *QNQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumeReport()
	require.NotNil(t, v, "expected pending report with state %s", expected)
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)
}

func assertNoPendingReport(t *testing.T, sm *QNQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumeReport(), "expected no pending report")
}

func drainPending(sm *QNQueryViewStateMachine) {
	sm.ConsumeReport()
}

// ---------------------------------------------------------------------------
// Construction tests
// ---------------------------------------------------------------------------

func TestNewQNQueryViewStateMachine(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())

	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStatePreparing)
	// After consume, no more pending.
	assertNoPendingReport(t, sm)
}

func TestNewQNQueryViewStateMachine_ViewPreserved(t *testing.T) {
	view := buildTestView()
	sm := NewQNQueryViewStateMachine(view)
	assert.Equal(t, view, sm.View())
}

// ---------------------------------------------------------------------------
// Normal flow: Preparing → Ready → Dropped
// ---------------------------------------------------------------------------

func TestNormalFlow_PreparingToReady(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnLocalEvent(QNLocalEventReady)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateReady)
	assertNoPendingReport(t, sm)
}

func TestNormalFlow_ReadyToDropped(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(QNLocalEventReady)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingReport(t, sm)
}

func TestNormalFlow_FullLifecycle(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())

	// Step 1: New → Preparing, report Preparing.
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStatePreparing)

	// Step 2: LocalReady → Ready, report Ready.
	sm.OnLocalEvent(QNLocalEventReady)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateReady)

	// Step 3: CoordDropped → Dropped, report Dropped.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)

	assertNoPendingReport(t, sm)
}

// ---------------------------------------------------------------------------
// Error path: Preparing → Unrecoverable → Dropped
// ---------------------------------------------------------------------------

func TestErrorPath_PreparingToUnrecoverable(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnLocalEvent(QNLocalEventUnrecoverable)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingReport(t, sm)
}

func TestErrorPath_UnrecoverableToDropped(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(QNLocalEventUnrecoverable)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingReport(t, sm)
}

func TestErrorPath_FullLifecycle(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	assertPendingReportState(t, sm, qviews.QueryViewStatePreparing)

	sm.OnLocalEvent(QNLocalEventUnrecoverable)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateUnrecoverable)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)

	assertNoPendingReport(t, sm)
}

// ---------------------------------------------------------------------------
// Coord re-push Preparing
// ---------------------------------------------------------------------------

func TestCoordRePush_Preparing(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStatePreparing)
	assertNoPendingReport(t, sm)
}

func TestCoordRePush_PreparingMultipleTimes(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	// Multiple re-pushes should each generate a report.
	for i := 0; i < 3; i++ {
		sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
		assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
		assertPendingReportState(t, sm, qviews.QueryViewStatePreparing)
	}
}

// ---------------------------------------------------------------------------
// Coord re-push Preparing is invalid in non-Preparing states
// ---------------------------------------------------------------------------

func TestCoordRePush_IgnoredInReady(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(QNLocalEventReady)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingReport(t, sm)
}

func TestCoordRePush_IgnoredInUnrecoverable(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(QNLocalEventUnrecoverable)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoPendingReport(t, sm)
}

// ---------------------------------------------------------------------------
// Invalid Coord pushes — silently ignored
// ---------------------------------------------------------------------------

func TestInvalidCoordPush_PreparingIgnoresUp(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPendingReport(t, sm)
}

func TestInvalidCoordPush_PreparingIgnoresDown(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPendingReport(t, sm)
}

func TestInvalidCoordPush_ReadyIgnoresUp(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(QNLocalEventReady)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingReport(t, sm)
}

func TestInvalidCoordPush_UnrecoverableIgnoresDown(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(QNLocalEventUnrecoverable)
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoPendingReport(t, sm)
}

// ---------------------------------------------------------------------------
// Invalid local events — silently ignored
// ---------------------------------------------------------------------------

func TestInvalidLocalEvent_ReadyIgnoresReady(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(QNLocalEventReady)
	drainPending(sm)

	sm.OnLocalEvent(QNLocalEventReady)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingReport(t, sm)
}

func TestInvalidLocalEvent_ReadyIgnoresUnrecoverable(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(QNLocalEventReady)
	drainPending(sm)

	sm.OnLocalEvent(QNLocalEventUnrecoverable)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingReport(t, sm)
}

func TestInvalidLocalEvent_UnrecoverableIgnoresReady(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnLocalEvent(QNLocalEventUnrecoverable)
	drainPending(sm)

	sm.OnLocalEvent(QNLocalEventReady)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoPendingReport(t, sm)
}

// ---------------------------------------------------------------------------
// Dropped is terminal — all events ignored
// ---------------------------------------------------------------------------

func TestDroppedTerminal_IgnoresCoordPush(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
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
		assertNoPendingReport(t, sm)
	}
}

func TestDroppedTerminal_IgnoresLocalEvents(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	drainPending(sm)

	events := []QNLocalEvent{QNLocalEventReady, QNLocalEventUnrecoverable}
	for _, event := range events {
		sm.OnLocalEvent(event)
		assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
		assertNoPendingReport(t, sm)
	}
}

// ---------------------------------------------------------------------------
// Idempotency — consume clears pending, no double output
// ---------------------------------------------------------------------------

func TestIdempotency_ConsumeReportClearsPending(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())

	// First consume gets the pending report.
	v := sm.ConsumeReport()
	require.NotNil(t, v)

	// Second consume returns nil.
	assertNoPendingReport(t, sm)
}

func TestIdempotency_CoordDroppedFromPreparingOnce(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
	// No more pending.
	assertNoPendingReport(t, sm)
}

// ---------------------------------------------------------------------------
// Direct Preparing → Dropped shortcut
// ---------------------------------------------------------------------------

func TestPreparingDirectToDropped(t *testing.T) {
	sm := NewQNQueryViewStateMachine(buildTestView())
	drainPending(sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingReport(t, sm)
}
