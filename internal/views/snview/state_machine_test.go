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

func buildTestMeta() *viewpb.QueryViewMeta {
	return &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			QueryVersion: 1,
		},
		State: viewpb.QueryViewState_QueryViewStatePreparing,
	}
}

func buildTestSNView() *viewpb.QueryViewOfStreamingNode {
	return &viewpb.QueryViewOfStreamingNode{}
}

func newTestSM() *SNQueryViewStateMachine {
	return NewSNQueryViewStateMachine(buildTestMeta(), buildTestSNView())
}

// newReadySM returns a SM in Ready state with all pending drained.
func newReadySM() *SNQueryViewStateMachine {
	sm := newTestSM()
	sm.ConsumeReport() // drain Preparing report
	sm.OnReady()
	sm.ConsumeReport()
	return sm
}

// newUpSM returns a SM in Up state with all pending drained.
func newUpSM() *SNQueryViewStateMachine {
	sm := newReadySM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	sm.ConsumeReport()
	sm.ConsumePersist()
	return sm
}

// newDownSM returns a SM in Down state with all pending drained.
func newDownSM() *SNQueryViewStateMachine {
	sm := newUpSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	sm.ConsumeReport()
	sm.ConsumePersist()
	return sm
}

// newUnrecoverableSM returns a SM in Unrecoverable state (from Preparing) with all pending drained.
func newUnrecoverableSM() *SNQueryViewStateMachine {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnUnrecoverable()
	sm.ConsumeReport()
	return sm
}

// newDroppedSM returns a SM in Dropped state with all pending drained.
func newDroppedSM() *SNQueryViewStateMachine {
	sm := newDownSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	sm.ConsumeReport()
	sm.ConsumePersist() // nil, but drain anyway
	return sm
}

// newRecoveringSM returns a SM in UpRecovering state with all pending drained.
func newRecoveringSM() *SNQueryViewStateMachine {
	return RecoverSNQueryViewStateMachine(buildTestMeta(), buildTestSNView())
}

func assertReportState(t *testing.T, sm *SNQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumeReport()
	require.NotNil(t, v, "expected pending report with state %s", expected)

	// Verify state.
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)

	// Verify meta fields are correctly carried.
	assert.Equal(t, sm.Meta().CollectionId, v.Meta.CollectionId)
	assert.Equal(t, sm.Meta().ReplicaId, v.Meta.ReplicaId)
	assert.Equal(t, sm.Meta().Vchannel, v.Meta.Vchannel)
	assert.Equal(t, sm.Meta().Version.QueryVersion, v.Meta.Version.QueryVersion)
	assert.Equal(t, sm.Meta().Version.DataVersion.StreamingVersion, v.Meta.Version.DataVersion.StreamingVersion)
	assert.Equal(t, sm.Meta().Version.DataVersion.CompactVersion, v.Meta.Version.DataVersion.CompactVersion)

	// Verify report structure: SN report has StreamingNode, no QueryNode.
	assert.NotNil(t, v.StreamingNode)
	assert.Nil(t, v.QueryNode)

	// Verify report meta is a clone (mutation doesn't affect SM).
	v.Meta.CollectionId = -1
	assert.NotEqual(t, int64(-1), sm.Meta().CollectionId)
}

func assertNoReport(t *testing.T, sm *SNQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumeReport(), "expected no pending report")
}

func assertPersistState(t *testing.T, sm *SNQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumePersist()
	require.NotNil(t, v, "expected pending persist with state %s", expected)

	// Verify state.
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)

	// Verify meta fields are correctly carried.
	assert.Equal(t, sm.Meta().CollectionId, v.Meta.CollectionId)
	assert.Equal(t, sm.Meta().ReplicaId, v.Meta.ReplicaId)
	assert.Equal(t, sm.Meta().Vchannel, v.Meta.Vchannel)
	assert.Equal(t, sm.Meta().Version.QueryVersion, v.Meta.Version.QueryVersion)
	assert.Equal(t, sm.Meta().Version.DataVersion.StreamingVersion, v.Meta.Version.DataVersion.StreamingVersion)
	assert.Equal(t, sm.Meta().Version.DataVersion.CompactVersion, v.Meta.Version.DataVersion.CompactVersion)

	// Verify persist structure: SN persist has StreamingNode, no QueryNode.
	assert.NotNil(t, v.StreamingNode)
	assert.Nil(t, v.QueryNode)

	// Verify persist meta is a clone (mutation doesn't affect SM).
	v.Meta.CollectionId = -1
	assert.NotEqual(t, int64(-1), sm.Meta().CollectionId)
}

func assertNoPersist(t *testing.T, sm *SNQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumePersist(), "expected no pending persist")
}

// ---------------------------------------------------------------------------
// 1. Construction — NewSNQueryViewStateMachine
// ---------------------------------------------------------------------------

func TestNew_InitialState(t *testing.T) {
	sm := newTestSM()
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assert.False(t, sm.IsRecovering())
}

func TestNew_PendingReport(t *testing.T) {
	sm := newTestSM()
	// Constructor generates a Preparing report to acknowledge receipt.
	assertReportState(t, sm, qviews.QueryViewStatePreparing)
	assertNoReport(t, sm)
}

func TestNew_NoPendingPersist(t *testing.T) {
	sm := newTestSM()
	assertNoPersist(t, sm)
}

func TestNew_MetaAndViewPreserved(t *testing.T) {
	meta := buildTestMeta()
	snView := buildTestSNView()
	sm := NewSNQueryViewStateMachine(meta, snView)
	assert.Equal(t, meta, sm.Meta())
	assert.Equal(t, snView, sm.SNView())
}

func TestNew_ReportIsClone(t *testing.T) {
	sm := newTestSM()
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	report.Meta.CollectionId = 999
	assert.Equal(t, testCollectionID, sm.Meta().CollectionId)
}

func TestNew_ReportStructure(t *testing.T) {
	sm := newTestSM()
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	assert.NotNil(t, report.Meta)
	assert.NotNil(t, report.StreamingNode)
	assert.Nil(t, report.QueryNode)
}

// ---------------------------------------------------------------------------
// 2. Construction — RecoverSNQueryViewStateMachine
// ---------------------------------------------------------------------------

func TestRecover_InitialState(t *testing.T) {
	sm := newRecoveringSM()
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	assert.True(t, sm.IsRecovering())
}

func TestRecover_NoPendingReport(t *testing.T) {
	sm := newRecoveringSM()
	assertNoReport(t, sm)
}

func TestRecover_NoPendingPersist(t *testing.T) {
	sm := newRecoveringSM()
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 3. Normal flow: Preparing → Ready → Up → Down → Dropped
// ---------------------------------------------------------------------------

func TestNormalFlow_PreparingToReady(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
	assertNoPersist(t, sm)
}

func TestNormalFlow_ReadyToUp(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	assertPersistState(t, sm, qviews.QueryViewStateUp)
}

func TestNormalFlow_UpToDown(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

func TestNormalFlow_DownToDropped(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	// Down already deleted recovery info; no persist needed.
	assertNoPersist(t, sm)
}

func TestNormalFlow_FullLifecycle(t *testing.T) {
	sm := newTestSM()

	// Preparing
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertReportState(t, sm, qviews.QueryViewStatePreparing)
	assertNoPersist(t, sm)

	// Ready
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
	assertNoPersist(t, sm)

	// Up
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	assertPersistState(t, sm, qviews.QueryViewStateUp)

	// Down
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)

	// Dropped
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)

	// Terminal
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 4. Error path: Preparing → Unrecoverable → Dropped
// ---------------------------------------------------------------------------

func TestErrorPath_PreparingToUnrecoverable(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	// No persist: no recovery info was persisted in Preparing.
	assertNoPersist(t, sm)
}

func TestErrorPath_UnrecoverableToDropped(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 5. Abort paths: Preparing → Dropped, Ready → Dropped
// ---------------------------------------------------------------------------

func TestAbort_PreparingToDropped(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestAbort_ReadyToDropped(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 6. Recovery flow: UpRecovering → Up / Down / Unrecoverable
// ---------------------------------------------------------------------------

func TestRecovery_RecoveringDone(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assert.False(t, sm.IsRecovering())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	// Already persisted as Up before crash — no new persist.
	assertNoPersist(t, sm)
}

func TestRecovery_UpRecoveringToDown(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assert.False(t, sm.IsRecovering())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	// Must delete recovery info.
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

func TestRecovery_UpRecoveringToUnrecoverable(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	// Must delete recovery info.
	assertPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestRecovery_UpRecoveringToDropped(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	// Must delete recovery info.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
}

func TestRecovery_FullFlow_RecoveringToUpToDownToDropped(t *testing.T) {
	sm := newRecoveringSM()

	// UpRecovering → Up
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	assertNoPersist(t, sm)

	// Up → Down
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)

	// Down → Dropped
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestRecovery_UnrecoverableToDropped(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnUnrecoverable()
	sm.ConsumeReport()
	sm.ConsumePersist()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 7. Coord re-push Preparing — distributed state recoverability
//
// Coord pushes Preparing when it doesn't know the node's current state
// (e.g., after Coord crash recovery or message loss).
// If SN has advanced past Preparing, it must re-report so Coord can fast-forward (doc 1.1).
// If SN is still Preparing, no re-report needed (local events drive it).
// If SN is UpRecovering, no report (wait for WAL catch-up, doc 2.4).
// ---------------------------------------------------------------------------

func TestCoordPreparing_StillPreparing_NoReport(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}

func TestCoordPreparing_StillPreparing_MultipleRePush_NoReport(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	for range 3 {
		sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
		assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
		assertNoReport(t, sm)
	}
}

func TestCoordPreparing_Ready_ReReportsReady(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
}

func TestCoordPreparing_Up_ReReportsUp(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
}

func TestCoordPreparing_Down_ReReportsDown(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
}

func TestCoordPreparing_Unrecoverable_ReReportsUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestCoordPreparing_Dropped_ReReportsDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestCoordPreparing_UpRecovering_NoReport(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	// Don't report: wait for WAL catch-up, then report Up for fast-forward.
	assertNoReport(t, sm)
}

func TestCoordPreparing_UpRecovering_MultipleRePush_NoReport(t *testing.T) {
	sm := newRecoveringSM()

	for range 3 {
		sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
		assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
		assertNoReport(t, sm)
	}
}

// ---------------------------------------------------------------------------
// 8. Coord re-push Up — fast-forward guarantee
// ---------------------------------------------------------------------------

func TestCoordUp_Ready_TransitionsToUp(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	assertPersistState(t, sm, qviews.QueryViewStateUp)
}

func TestCoordUp_AlreadyUp_ReReportsUp(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	// No new persist: already persisted.
	assertNoPersist(t, sm)
}

func TestCoordUp_Down_ReReportsDown(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
}

func TestCoordUp_Dropped_ReReportsDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestCoordUp_Preparing_ReReportsPreparing(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertReportState(t, sm, qviews.QueryViewStatePreparing)
}

func TestCoordUp_Unrecoverable_ReReportsUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestCoordUp_UpRecovering_ReReportsUp(t *testing.T) {
	sm := newRecoveringSM()

	// UpRecovering maps to Up for Coord-visible state.
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
}

// ---------------------------------------------------------------------------
// 9. Coord re-push Down — fast-forward guarantee
// ---------------------------------------------------------------------------

func TestCoordDown_Up_TransitionsToDown(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

func TestCoordDown_UpRecovering_TransitionsToDown(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

func TestCoordDown_AlreadyDown_ReReportsDown(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertNoPersist(t, sm)
}

func TestCoordDown_Dropped_ReReportsDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestCoordDown_Preparing_ReReportsPreparing(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertReportState(t, sm, qviews.QueryViewStatePreparing)
}

func TestCoordDown_Ready_ReReportsReady(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
}

func TestCoordDown_Unrecoverable_ReReportsUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

// ---------------------------------------------------------------------------
// 10. Coord Dropped — transition from any state
// ---------------------------------------------------------------------------

func TestCoordDropped_FromPreparing(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestCoordDropped_FromReady(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestCoordDropped_FromUp(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	// Must delete recovery info.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
}

func TestCoordDropped_FromUpRecovering(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	// Must delete recovery info.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
}

func TestCoordDropped_FromDown(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestCoordDropped_FromUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestCoordDropped_RePushInDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestCoordDropped_RePushMultiple(t *testing.T) {
	sm := newDroppedSM()

	for range 3 {
		sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
		assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
		assertReportState(t, sm, qviews.QueryViewStateDropped)
		assertNoPersist(t, sm)
	}
}

// ---------------------------------------------------------------------------
// 11. Local event idempotency — events ignored in invalid states
// ---------------------------------------------------------------------------

func TestOnReady_IgnoredInReady(t *testing.T) {
	sm := newReadySM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInUp(t *testing.T) {
	sm := newUpSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInDown(t *testing.T) {
	sm := newDownSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInUpRecovering(t *testing.T) {
	sm := newRecoveringSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInReady(t *testing.T) {
	sm := newReadySM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInUp(t *testing.T) {
	sm := newUpSM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInDown(t *testing.T) {
	sm := newDownSM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInPreparing(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInReady(t *testing.T) {
	sm := newReadySM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInUp(t *testing.T) {
	sm := newUpSM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInDown(t *testing.T) {
	sm := newDownSM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoReport(t, sm)
}

// ---------------------------------------------------------------------------
// 12. Dropped terminal — all events ignored
// ---------------------------------------------------------------------------

func TestDroppedTerminal_AllLocalEventsIgnored(t *testing.T) {
	sm := newDroppedSM()

	sm.OnReady()
	assertNoReport(t, sm)
	sm.OnUnrecoverable()
	assertNoReport(t, sm)
	sm.OnRecoveringDone()
	assertNoReport(t, sm)

	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
}

func TestDroppedTerminal_AllCoordPushesReReportDropped(t *testing.T) {
	sm := newDroppedSM()

	pushes := []qviews.QueryViewState{
		qviews.QueryViewStatePreparing,
		qviews.QueryViewStateUp,
		qviews.QueryViewStateDown,
		qviews.QueryViewStateDropped,
	}
	for _, push := range pushes {
		sm.OnCoordStateDelivered(push)
		assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
		assertReportState(t, sm, qviews.QueryViewStateDropped)
		assertNoPersist(t, sm)
	}
}

// ---------------------------------------------------------------------------
// 13. Consume idempotency — double consume returns nil
// ---------------------------------------------------------------------------

func TestConsume_DoubleConsumeReportReturnsNil(t *testing.T) {
	sm := newTestSM()
	v := sm.ConsumeReport()
	require.NotNil(t, v)
	assertNoReport(t, sm)
}

func TestConsume_DoubleConsumePersistReturnsNil(t *testing.T) {
	sm := newReadySM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	v := sm.ConsumePersist()
	require.NotNil(t, v)
	assertNoPersist(t, sm)
}

func TestConsume_NoEventNoReport(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport() // drain initial
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 14. Distributed recoverability — Coord crash + re-push scenarios
//
// Simulates Coord crash-recovery: Coord re-pushes Preparing to all nodes.
// SN must re-report its current state so Coord can reconstruct progress.
// ---------------------------------------------------------------------------

func TestRecoverability_ReadyAfterCoordCrash(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	// SN completes preparation.
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	sm.ConsumeReport() // Coord consumed, then crashes.

	// Coord recovers from ETCD (still Preparing), re-pushes Preparing.
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assertReportState(t, sm, qviews.QueryViewStateReady)
}

func TestRecoverability_UpAfterCoordCrash(t *testing.T) {
	sm := newUpSM()

	// Coord crashes, recovers from ETCD as Preparing, re-pushes.
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assertReportState(t, sm, qviews.QueryViewStateUp)
}

func TestRecoverability_DownAfterCoordCrash(t *testing.T) {
	sm := newDownSM()

	// Coord crashes, recovers from ETCD as Down, re-pushes Down.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assertReportState(t, sm, qviews.QueryViewStateDown)
}

func TestRecoverability_UnrecoverableAfterCoordCrash(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnUnrecoverable()
	sm.ConsumeReport()

	// Coord re-pushes Preparing after crash.
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestRecoverability_DroppedAfterCoordCrash(t *testing.T) {
	sm := newDroppedSM()

	// Coord re-pushes Dropped (Dropping not persisted, re-executes flow).
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestRecoverability_RepeatedRePushAlwaysProducesReport(t *testing.T) {
	sm := newUpSM()

	// Simulate multiple Coord re-pushes (retries due to network issues).
	for range 5 {
		sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
		assertReportState(t, sm, qviews.QueryViewStateUp)
	}
}

func TestRecoverability_UpRecoveringThenCoordCrash(t *testing.T) {
	sm := newRecoveringSM()

	// Coord crashes, re-pushes Preparing. SN in UpRecovering waits.
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assertNoReport(t, sm)

	// WAL catches up → transitions to Up → reports Up.
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)

	// Coord sees Up and can fast-forward.
}

func TestRecoverability_SNCrashRecovery_FullFlow(t *testing.T) {
	// SN was Up, persisted recovery info, then crashed.
	// On restart, SN reconstructs from persisted Up view.
	sm := RecoverSNQueryViewStateMachine(buildTestMeta(), buildTestSNView())
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	assertNoReport(t, sm)
	assertNoPersist(t, sm)

	// WAL catches up.
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	// Already persisted as Up — no new persist.
	assertNoPersist(t, sm)

	// Normal lifecycle resumes.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

// ---------------------------------------------------------------------------
// 15. coordVisibleState — UpRecovering maps to Up in reports
// ---------------------------------------------------------------------------

func TestCoordVisibleState_UpRecoveringReportsAsUp(t *testing.T) {
	sm := newRecoveringSM()

	// Trigger a report via Coord push (Up re-push to UpRecovering).
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	// Report shows Up, not UpRecovering.
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUp, report.Meta.State)
	// But internal state is still UpRecovering.
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
}

// ---------------------------------------------------------------------------
// 16. Persist semantics — Up persists, Down/Unrecoverable/Dropped deletes
// ---------------------------------------------------------------------------

func TestPersist_UpSaves(t *testing.T) {
	sm := newReadySM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUp, persist.Meta.State)
}

func TestPersist_DownDeletes(t *testing.T) {
	sm := newUpSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDown, persist.Meta.State)
}

func TestPersist_UnrecoverableFromUpRecoveringDeletes(t *testing.T) {
	sm := newRecoveringSM()
	sm.OnUnrecoverable()
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, persist.Meta.State)
}

func TestPersist_DroppedFromUpDeletes(t *testing.T) {
	sm := newUpSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, persist.Meta.State)
}

func TestPersist_DroppedFromUpRecoveringDeletes(t *testing.T) {
	sm := newRecoveringSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, persist.Meta.State)
}

func TestPersist_PreparingNoPersist(t *testing.T) {
	sm := newTestSM()
	assertNoPersist(t, sm)
}

func TestPersist_ReadyNoPersist(t *testing.T) {
	sm := newReadySM()
	assertNoPersist(t, sm)
}

func TestPersist_UnrecoverableFromPreparingNoPersist(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnUnrecoverable()
	assertNoPersist(t, sm)
}

func TestPersist_DroppedFromDownNoPersist(t *testing.T) {
	sm := newDownSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestPersist_DroppedFromPreparingNoPersist(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestPersist_DroppedFromReadyNoPersist(t *testing.T) {
	sm := newReadySM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestPersist_DroppedFromUnrecoverableNoPersist(t *testing.T) {
	sm := newUnrecoverableSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 17. Pending report overwrite — latest event wins
// ---------------------------------------------------------------------------

func TestPendingOverwrite_ReadyThenDropped(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnReady()
	// Before consuming Ready report, Coord pushes Dropped.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)

	// Only the Dropped report should be present.
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoReport(t, sm)
}

func TestPendingOverwrite_ReadyThenUnrecoverable(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	// OnReady is only valid in Preparing, and OnUnrecoverable is only valid in Preparing.
	// OnReady transitions to Ready, so OnUnrecoverable won't fire.
	// Instead test: Preparing report overwritten by Unrecoverable.
	sm.OnUnrecoverable()
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoReport(t, sm)
}

func TestPendingOverwrite_CoordUpThenDown(t *testing.T) {
	sm := newReadySM()

	// Coord pushes Up (transition Ready→Up), then Down (transition Up→Down).
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)

	// Only the Down report & persist should be present.
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

// ---------------------------------------------------------------------------
// 18. Unrecognized Coord pushes — no handler in OnCoordStateDelivered
// ---------------------------------------------------------------------------

func TestUnrecognizedPush_DroppingIgnored(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	// Dropping is a Coord-only state; SN has no handler for it.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropping)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}
