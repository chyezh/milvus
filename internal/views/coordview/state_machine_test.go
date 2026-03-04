//go:build test && dynamic

package coordview

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

// buildTestView creates a QueryViewOfShard in Preparing state with the given
// number of query nodes. QN IDs are 1..numQN.
func buildTestView(numQN int) *viewpb.QueryViewOfShard {
	qns := make([]*viewpb.QueryViewOfQueryNode, numQN)
	for i := range qns {
		qns[i] = &viewpb.QueryViewOfQueryNode{
			NodeId: int64(i + 1),
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{1000 + int64(i)}},
			},
		}
	}
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
		QueryNode:     qns,
	}
}

// snReport creates a StreamingNode report with the given state.
func snReport(view *viewpb.QueryViewOfShard, state qviews.QueryViewState) qviews.QueryViewAtWorkNode {
	meta := &viewpb.QueryViewMeta{
		CollectionId: view.Meta.CollectionId,
		ReplicaId:    view.Meta.ReplicaId,
		Vchannel:     view.Meta.Vchannel,
		Version:      view.Meta.Version,
		State:        viewpb.QueryViewState(state),
	}
	return qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
}

// qnReport creates a QueryNode report with the given state and optional ready segment IDs.
func qnReport(view *viewpb.QueryViewOfShard, nodeID int64, state qviews.QueryViewState, readySegs ...int64) qviews.QueryViewAtWorkNode {
	meta := &viewpb.QueryViewMeta{
		CollectionId: view.Meta.CollectionId,
		ReplicaId:    view.Meta.ReplicaId,
		Vchannel:     view.Meta.Vchannel,
		Version:      view.Meta.Version,
		State:        viewpb.QueryViewState(state),
	}
	partitions := []*viewpb.QueryViewOfPartition{
		{PartitionId: 10, SegmentIds: []int64{1000}, ReadySegmentIds: readySegs},
	}
	return qviews.NewQueryViewAtQueryNode(meta, &viewpb.QueryViewOfQueryNode{
		NodeId:     nodeID,
		Partitions: partitions,
	})
}

// consumeAndClear drains pending persist and sync so they don't carry over.
func consumeAndClear(sm *CoordQueryViewStateMachine) {
	sm.ConsumePersist()
	sm.ConsumeSync()
}

// assertPendingPersistState checks that ConsumePersist returns a view with the
// expected state, then clears it.
func assertPendingPersistState(t *testing.T, sm *CoordQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumePersist()
	require.NotNil(t, v, "expected pending persist with state %s", expected)
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)
}

// assertPendingSyncState checks that ConsumeSync returns a view with the
// expected state, then clears it.
func assertPendingSyncState(t *testing.T, sm *CoordQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumeSync()
	require.NotNil(t, v, "expected pending sync with state %s", expected)
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)
}

// assertNoPendingPersist checks that ConsumePersist returns nil.
func assertNoPendingPersist(t *testing.T, sm *CoordQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumePersist(), "expected no pending persist")
}

// assertNoPendingSync checks that ConsumeSync returns nil.
func assertNoPendingSync(t *testing.T, sm *CoordQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumeSync(), "expected no pending sync")
}

// ===========================================================================
// 1. NORMAL STATE TRANSITIONS (Happy Path)
// ===========================================================================

// TestNormalFlow_SingleQN validates the complete normal lifecycle:
// Preparing → Ready → Up → Down → Dropping → Dropped
func TestNormalFlow_SingleQN(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	// --- Preparing ---
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStatePreparing)
	assertPendingSyncState(t, sm, qviews.QueryViewStatePreparing)

	// QN1 reports Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State(), "SN not ready yet")

	// SN reports Ready → all nodes ready → transition to Ready
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp) // push Up to SN

	// --- Ready → Up ---
	// SN reports Up → transition to Up
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUp)
	assertNoPendingSync(t, sm)

	// --- Up → Down ---
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)

	// --- Down → Dropping ---
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped) // push Dropped to all

	// --- Dropping → Dropped ---
	// SN reports Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State(), "QN1 not Dropped yet")

	// QN1 reports Dropped → all nodes Dropped → Dropped
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped) // delete from ETCD
}

// TestNormalFlow_MultipleQN validates that transition from Preparing to Ready
// requires ALL QNs to report Ready.
func TestNormalFlow_MultipleQN(t *testing.T) {
	view := buildTestView(3)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// QN1 Ready, QN2 Ready, SN Ready — QN3 still missing
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State(), "QN3 not ready yet")

	// QN3 Ready → all ready → Ready
	sm.OnNodeStateReported(qnReport(view, 3, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
}

// TestNormalFlow_DroppingRequiresAllNodesDropped validates that Dropping → Dropped
// needs ALL nodes (SN + all QNs) to report Dropped.
func TestNormalFlow_DroppingRequiresAllNodesDropped(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Fast-forward to Dropping state
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	consumeAndClear(sm)
	sm.EnterDown()
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	consumeAndClear(sm)

	// Only SN and QN1 Dropped — still Dropping
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())

	// QN2 Dropped → all Dropped → Dropped
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
}

// ===========================================================================
// 2. RECOVERY FAST-FORWARD (SN already Up during Preparing)
// ===========================================================================

// TestPreparing_SNAlreadyUp_FastForward tests recovery scenario where SN
// reports Up (from persistence recovery) while Coord is still in Preparing.
// Should fast-forward directly to Up, skipping Ready.
func TestPreparing_SNAlreadyUp_FastForward(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// QN1 Ready, SN Up (recovery) → fast-forward to Up
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUp)
	assertNoPendingSync(t, sm) // no need to push Up, SN already Up
}

// TestPreparing_SNUpBeforeQNReady_WaitsForQN ensures fast-forward only
// happens after ALL QNs are also Ready.
func TestPreparing_SNUpBeforeQNReady_WaitsForQN(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// SN reports Up but QNs are not ready
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())

	// Last QN Ready → fast-forward to Up
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
}

// ===========================================================================
// 3. UNRECOVERABLE TRANSITIONS (Error Path)
// ===========================================================================

// TestPreparing_SNUnrecoverable transitions to Unrecoverable when SN reports
// Unrecoverable during Preparing.
func TestPreparing_SNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
}

// TestPreparing_QNUnrecoverable transitions to Unrecoverable when any QN
// reports Unrecoverable during Preparing.
func TestPreparing_QNUnrecoverable(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// QN1 is Ready but QN2 is Unrecoverable → whole view Unrecoverable
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
}

// TestReady_SNUnrecoverable transitions to Unrecoverable from Ready state.
func TestReady_SNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	consumeAndClear(sm)

	// SN reports Unrecoverable in Ready
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
}

// TestReady_QNUnrecoverable transitions to Unrecoverable from Ready state
// when a QN fails.
func TestReady_QNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	consumeAndClear(sm)

	// QN reports Unrecoverable in Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
}

// TestUp_SNUnrecoverable transitions to Unrecoverable from Up state.
func TestUp_SNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Up
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	consumeAndClear(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
}

// TestUp_QNUnrecoverable transitions to Unrecoverable from Up when QN fails.
func TestUp_QNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Up
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	consumeAndClear(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
}

// TestUnrecoverable_EnterDropping_ToDropping validates the Manager-triggered
// transition from Unrecoverable → Dropping.
func TestUnrecoverable_EnterDropping_ToDropping(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Drive to Unrecoverable
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	consumeAndClear(sm)

	// Manager calls EnterDropping
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped) // push Dropped to nodes
}

// TestUnrecoverable_FullCleanupCycle validates the complete error recovery
// path: Unrecoverable → Dropping → Dropped.
func TestUnrecoverable_FullCleanupCycle(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Unrecoverable
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	consumeAndClear(sm)

	// Dropping
	sm.EnterDropping()
	consumeAndClear(sm)

	// All nodes report Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
}

// ===========================================================================
// 4. IDEMPOTENCY TESTS
// ===========================================================================

// TestIdempotency_EnterDown_NotInUp verifies EnterDown is no-op when not in Up.
func TestIdempotency_EnterDown_NotInUp(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*CoordQueryViewStateMachine, *viewpb.QueryViewOfShard)
		state qviews.QueryViewState
	}{
		{
			name:  "Preparing",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {},
			state: qviews.QueryViewStatePreparing,
		},
		{
			name: "Ready",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(qnReport(v, 1, qviews.QueryViewStateReady))
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateReady))
			},
			state: qviews.QueryViewStateReady,
		},
		{
			name: "Down",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(qnReport(v, 1, qviews.QueryViewStateReady))
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateReady))
				consumeAndClear(sm)
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUp))
				consumeAndClear(sm)
				sm.EnterDown()
			},
			state: qviews.QueryViewStateDown,
		},
		{
			name: "Unrecoverable",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUnrecoverable))
			},
			state: qviews.QueryViewStateUnrecoverable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			view := buildTestView(1)
			sm := NewCoordQueryViewStateMachine(view)
			consumeAndClear(sm)
			tc.setup(sm, view)
			assert.Equal(t, tc.state, sm.State())

			consumeAndClear(sm)
			sm.EnterDown()
			assert.Equal(t, tc.state, sm.State(), "EnterDown should be no-op in %s", tc.state)
			assertNoPendingPersist(t, sm)
			assertNoPendingSync(t, sm)
		})
	}
}

// TestIdempotency_EnterDropping_NotInUnrecoverable verifies EnterDropping is
// no-op when not in Unrecoverable.
func TestIdempotency_EnterDropping_NotInUnrecoverable(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*CoordQueryViewStateMachine, *viewpb.QueryViewOfShard)
		state qviews.QueryViewState
	}{
		{
			name:  "Preparing",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {},
			state: qviews.QueryViewStatePreparing,
		},
		{
			name: "Up",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(qnReport(v, 1, qviews.QueryViewStateReady))
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateReady))
				consumeAndClear(sm)
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUp))
			},
			state: qviews.QueryViewStateUp,
		},
		{
			name: "Down",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(qnReport(v, 1, qviews.QueryViewStateReady))
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateReady))
				consumeAndClear(sm)
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUp))
				consumeAndClear(sm)
				sm.EnterDown()
			},
			state: qviews.QueryViewStateDown,
		},
		{
			name: "Dropping",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUnrecoverable))
				consumeAndClear(sm)
				sm.EnterDropping()
			},
			state: qviews.QueryViewStateDropping,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			view := buildTestView(1)
			sm := NewCoordQueryViewStateMachine(view)
			consumeAndClear(sm)
			tc.setup(sm, view)
			assert.Equal(t, tc.state, sm.State())

			consumeAndClear(sm)
			sm.EnterDropping()
			assert.Equal(t, tc.state, sm.State(), "EnterDropping should be no-op in %s", tc.state)
			assertNoPendingPersist(t, sm)
			assertNoPendingSync(t, sm)
		})
	}
}

// TestIdempotency_DuplicateNodeReports validates that processing the same
// node report multiple times is idempotent and does not corrupt state.
func TestIdempotency_DuplicateNodeReports(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Report QN Ready three times
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State(), "still waiting for SN")

	// Report SN Ready twice → should only transition once
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	consumeAndClear(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State(), "still Ready, waiting for SN Up")
}

// TestIdempotency_EnterDown_CalledTwice verifies double EnterDown is safe.
func TestIdempotency_EnterDown_CalledTwice(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Up
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	consumeAndClear(sm)

	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	consumeAndClear(sm)

	// Second call is no-op (already Down)
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPendingPersist(t, sm)
	assertNoPendingSync(t, sm)
}

// TestIdempotency_EnterDropping_CalledTwice verifies double EnterDropping is safe.
func TestIdempotency_EnterDropping_CalledTwice(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	consumeAndClear(sm)

	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	consumeAndClear(sm)

	// Second call is no-op (already Dropping)
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertNoPendingSync(t, sm)
}

// ===========================================================================
// 5. COORDINATOR CRASH RECOVERY
// ===========================================================================

// TestRecovery_Preparing re-pushes Preparing sync, no persist (already persisted).
func TestRecovery_Preparing(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStatePreparing

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStatePreparing)

	// Can proceed normally after recovery
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
}

// TestRecovery_Up has no pending operations, waits for events.
func TestRecovery_Up(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUp

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPendingPersist(t, sm)
	assertNoPendingSync(t, sm)

	// Can receive EnterDown after recovery
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
}

// TestRecovery_Down re-pushes Down sync to SN.
func TestRecovery_Down(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateDown

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)

	// SN reports Down → Dropping
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
}

// TestRecovery_Unrecoverable stays in Unrecoverable, waits for Manager.
func TestRecovery_Unrecoverable(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoPendingPersist(t, sm)
	assertNoPendingSync(t, sm)

	// Manager can call EnterDropping after recovery
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
}

// TestRecovery_InvalidState panics on invalid persisted state (Ready, Dropping, Dropped).
func TestRecovery_InvalidState(t *testing.T) {
	invalidStates := []viewpb.QueryViewState{
		viewpb.QueryViewState_QueryViewStateReady,
		viewpb.QueryViewState_QueryViewStateDropping,
		viewpb.QueryViewState_QueryViewStateDropped,
		viewpb.QueryViewState_QueryViewStateUnknown,
	}

	for _, state := range invalidStates {
		t.Run(state.String(), func(t *testing.T) {
			view := buildTestView(1)
			view.Meta.State = state
			assert.Panics(t, func() {
				RecoverCoordQueryViewStateMachine(view)
			}, "recovery from %s should panic", state)
		})
	}
}

// TestRecovery_Preparing_ThenUnrecoverable validates that after recovering in
// Preparing, receiving Unrecoverable still works.
func TestRecovery_Preparing_ThenUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStatePreparing

	sm := RecoverCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
}

// TestRecovery_Up_ThenUnrecoverable validates that after recovering in Up,
// receiving Unrecoverable still works.
func TestRecovery_Up_ThenUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUp

	sm := RecoverCoordQueryViewStateMachine(view)
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
}

// ===========================================================================
// 6. PENDING I/O CONSUMPTION SEMANTICS
// ===========================================================================

// TestConsumePersist_ConsumeOnce verifies persist is only returned once.
func TestConsumePersist_ConsumeOnce(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	v := sm.ConsumePersist()
	require.NotNil(t, v)

	// Second consume returns nil
	assert.Nil(t, sm.ConsumePersist())
}

// TestConsumeSync_ConsumeOnce verifies sync is only returned once.
func TestConsumeSync_ConsumeOnce(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	v := sm.ConsumeSync()
	require.NotNil(t, v)

	// Second consume returns nil
	assert.Nil(t, sm.ConsumeSync())
}

// TestPendingPersist_Dropped_MeansDelete validates that a pending persist
// with Dropped state signals ETCD deletion.
func TestPendingPersist_Dropped_MeansDelete(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Drive to Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	consumeAndClear(sm)
	sm.EnterDropping()
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))

	v := sm.ConsumePersist()
	require.NotNil(t, v)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, v.Meta.State)
}

// ===========================================================================
// 7. RE-PUSH BEHAVIOR (Retries on stale node state)
// ===========================================================================

// TestReady_SNNotUpYet_RePushUp verifies that when SN reports non-Up state
// in Ready, Coord re-pushes Up.
func TestReady_SNNotUpYet_RePushUp(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	consumeAndClear(sm)

	// SN reports Ready again (hasn't picked up Up yet) → re-push Up
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
}

// TestDown_SNNotDownYet_RePushDown verifies re-push of Down when SN reports
// non-Down state.
func TestDown_SNNotDownYet_RePushDown(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Down
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	consumeAndClear(sm)
	sm.EnterDown()
	consumeAndClear(sm)

	// SN reports Up (hasn't received Down yet) → re-push Down
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)
}

// TestDropping_NodeNotDropped_RePushDropped verifies re-push of Dropped when
// a node reports a non-Dropped state.
func TestDropping_NodeNotDropped_RePushDropped(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Drive to Dropping
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	consumeAndClear(sm)
	sm.EnterDropping()
	consumeAndClear(sm)

	// SN reports Ready (hasn't received Dropped yet) → re-push Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)
}

// ===========================================================================
// 8. UNRECOVERABLE IS A STABLE STATE (ignores node reports)
// ===========================================================================

// TestUnrecoverable_IgnoresNodeReports validates that no node report can
// move the state machine out of Unrecoverable.
func TestUnrecoverable_IgnoresNodeReports(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	consumeAndClear(sm)

	// Various node reports should not change state
	reports := []qviews.QueryViewAtWorkNode{
		snReport(view, qviews.QueryViewStateReady),
		snReport(view, qviews.QueryViewStateUp),
		snReport(view, qviews.QueryViewStateDown),
		snReport(view, qviews.QueryViewStateDropped),
		qnReport(view, 1, qviews.QueryViewStateReady),
		qnReport(view, 2, qviews.QueryViewStateUnrecoverable),
	}

	for _, report := range reports {
		sm.OnNodeStateReported(report)
		assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
		assertNoPendingPersist(t, sm)
		assertNoPendingSync(t, sm)
	}
}

// ===========================================================================
// 9. DOWN STATE IGNORES QN REPORTS
// ===========================================================================

// TestDown_QNReportIgnored verifies that QN reports are ignored in Down state.
// Only SN Down triggers the transition.
func TestDown_QNReportIgnored(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Down
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	consumeAndClear(sm)
	sm.EnterDown()
	consumeAndClear(sm)

	// QN reports should not affect state
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPendingPersist(t, sm)
	assertNoPendingSync(t, sm)
}

// ===========================================================================
// 10. READY STATE IGNORES QN REPORTS (only SN matters)
// ===========================================================================

// TestReady_QNReportIgnored verifies that QN reports in Ready state don't
// trigger state transitions (only SN Up matters).
func TestReady_QNReportIgnored(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	consumeAndClear(sm)

	// QN report in Ready state — not SN, so should be ignored (except Unrecoverable)
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertNoPendingSync(t, sm)
}

// ===========================================================================
// 11. QN READY SEGMENTS TRACKING
// ===========================================================================

// TestQNReadySegments_TrackedDuringPreparing validates that ready segments
// reported by QNs are tracked.
func TestQNReadySegments_TrackedDuringPreparing(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// QN1 reports Ready with segments
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady, 100, 101))
	segs := sm.QNReadySegments()
	assert.Equal(t, []int64{100, 101}, segs[1])

	// QN2 reports Ready with segments
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady, 200))
	segs = sm.QNReadySegments()
	assert.Equal(t, []int64{100, 101}, segs[1])
	assert.Equal(t, []int64{200}, segs[2])
}

// TestQNReadySegments_UpdatedOnReReport validates that re-reports update
// the ready segments.
func TestQNReadySegments_UpdatedOnReReport(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady, 100))
	assert.Equal(t, []int64{100}, sm.QNReadySegments()[1])

	// Update with new segments
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady, 100, 101, 102))
	assert.Equal(t, []int64{100, 101, 102}, sm.QNReadySegments()[1])
}

// ===========================================================================
// 12. VIEW ACCESSOR
// ===========================================================================

// TestView_ReturnsSameReference ensures View() returns the original proto.
func TestView_ReturnsSameReference(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	assert.Equal(t, view, sm.View())
}

// ===========================================================================
// 13. NEW STATE MACHINE INITIAL STATE
// ===========================================================================

// TestNewStateMachine_InitialState validates all initial properties.
func TestNewStateMachine_InitialState(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)

	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assert.Equal(t, view, sm.View())
	assert.NotNil(t, sm.QNReadySegments())
	assert.Len(t, sm.QNReadySegments(), 0)

	// Both pending operations set
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, persist.Meta.State)

	sync := sm.ConsumeSync()
	require.NotNil(t, sync)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, sync.Meta.State)
}

// ===========================================================================
// 14. EDGE CASES
// ===========================================================================

// TestNoQN_NormalFlow validates state machine with zero query nodes
// (SN-only view). Should progress normally through lifecycle.
func TestNoQN_NormalFlow(t *testing.T) {
	view := buildTestView(0)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// No QNs to wait for, only SN Ready needed
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
}

// TestPreparing_QNReportsBeforeSN validates that QN Ready reports are
// accumulated and state waits for SN.
func TestPreparing_QNReportsBeforeSN(t *testing.T) {
	view := buildTestView(3)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// All QNs Ready, no SN yet
	for i := 1; i <= 3; i++ {
		sm.OnNodeStateReported(qnReport(view, int64(i), qviews.QueryViewStateReady))
		assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	}

	// SN Ready → transition
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
}

// TestDown_UnrecoverableFromSN_NotHandled verifies that in Down state,
// Unrecoverable reports from nodes do NOT transition (Down doesn't check
// for Unrecoverable per the implementation).
func TestDown_UnrecoverableFromSN_NotHandled(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Advance to Down
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	consumeAndClear(sm)
	sm.EnterDown()
	consumeAndClear(sm)

	// SN Unrecoverable in Down → handleDown only cares about SN Down
	// Non-SN reports are ignored, and SN non-Down triggers re-push
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	// Should re-push Down since SN didn't report Down
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)
}

// TestDropping_PartialDropped_StaysDropping verifies that Dropping stays until
// ALL nodes are Dropped.
func TestDropping_PartialDropped_StaysDropping(t *testing.T) {
	view := buildTestView(3)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Fast-forward to Dropping via Unrecoverable path
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	consumeAndClear(sm)
	sm.EnterDropping()
	consumeAndClear(sm)

	// Nodes report Dropped one by one
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())

	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())

	// Last node Dropped → Dropped
	sm.OnNodeStateReported(qnReport(view, 3, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
}

// TestPendingSync_PreservesMeta validates that pending sync views preserve
// the original view's metadata (collection, replica, vchannel, version).
func TestPendingSync_PreservesMeta(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	sync := sm.ConsumeSync()
	require.NotNil(t, sync)
	assert.Equal(t, view.Meta.CollectionId, sync.Meta.CollectionId)
	assert.Equal(t, view.Meta.ReplicaId, sync.Meta.ReplicaId)
	assert.Equal(t, view.Meta.Vchannel, sync.Meta.Vchannel)
	assert.Equal(t, view.Meta.Version.QueryVersion, sync.Meta.Version.QueryVersion)
}

// TestPendingPersist_PreservesMeta validates that pending persist views
// preserve the original view's metadata.
func TestPendingPersist_PreservesMeta(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, view.Meta.CollectionId, persist.Meta.CollectionId)
	assert.Equal(t, view.Meta.ReplicaId, persist.Meta.ReplicaId)
	assert.Equal(t, view.Meta.Vchannel, persist.Meta.Vchannel)
}

// TestViewWithState_IsClone verifies that viewWithState returns a clone,
// not a reference to the original.
func TestViewWithState_IsClone(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	persist := sm.ConsumePersist()
	require.NotNil(t, persist)

	// Mutating the returned view should not affect the original
	persist.Meta.CollectionId = 999
	assert.Equal(t, testCollectionID, sm.View().Meta.CollectionId)
}

// ===========================================================================
// 15. COMPLETE LIFECYCLE WITH MULTIPLE QNs (Integration-style)
// ===========================================================================

// TestCompleteLifecycle_3QN_ErrorRecovery validates a full cycle with 3 QNs
// where one QN fails during Preparing, triggering Unrecoverable path.
func TestCompleteLifecycle_3QN_ErrorRecovery(t *testing.T) {
	view := buildTestView(3)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Preparing: QN1 Ready, QN2 Unrecoverable → immediate Unrecoverable
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())

	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)

	// Manager generates replacement, then calls EnterDropping
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)

	// All nodes report Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State()) // QN3 pending

	sm.OnNodeStateReported(qnReport(view, 3, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
}

// TestCompleteLifecycle_UpThenRecovery simulates Coord crash recovery from
// Up state, then normal completion.
func TestCompleteLifecycle_UpThenRecovery(t *testing.T) {
	view := buildTestView(2)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUp

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPendingPersist(t, sm)
	assertNoPendingSync(t, sm)

	// Normal Down flow after recovery
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	consumeAndClear(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	consumeAndClear(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
}

// ===========================================================================
// 16. DROPPED STATE IS TERMINAL
// ===========================================================================

// TestDropped_IsTerminal verifies no operations change state after Dropped.
func TestDropped_IsTerminal(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	// Drive to Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	consumeAndClear(sm)
	sm.EnterDropping()
	consumeAndClear(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	consumeAndClear(sm)

	// No operations should change state
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())

	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())

	assertNoPendingPersist(t, sm)
	assertNoPendingSync(t, sm)
}

// ===========================================================================
// 17. SN PREPARING REPORT DOES NOT ADVANCE STATE
// ===========================================================================

// TestPreparing_SNPreparing_NoTransition verifies that SN reporting Preparing
// does not advance the state machine.
func TestPreparing_SNPreparing_NoTransition(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStatePreparing))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
}

// ===========================================================================
// 18. ORDERING OF NODE REPORTS
// ===========================================================================

// TestPreparing_SNReadyBeforeAllQN verifies SN Ready before all QNs are
// Ready does not prematurely advance state.
func TestPreparing_SNReadyBeforeAllQN(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())

	// Last QN Ready triggers transition
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
}

// TestPreparing_AllQNReadyThenSNReady verifies the reverse ordering also works.
func TestPreparing_AllQNReadyThenSNReady(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	consumeAndClear(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
}
