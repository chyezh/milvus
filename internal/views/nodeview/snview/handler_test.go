//go:build test && dynamic

package snview

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/nodeview/handler"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// ---------------------------------------------------------------------------
// Mock catalog
// ---------------------------------------------------------------------------

type mockCatalog struct {
	mu      sync.Mutex
	saved   map[string]*viewpb.QueryViewOfShard
	deleted []string
}

func newMockCatalog() *mockCatalog {
	return &mockCatalog{
		saved: make(map[string]*viewpb.QueryViewOfShard),
	}
}

func (c *mockCatalog) SaveQueryView(key string, view *viewpb.QueryViewOfShard) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saved[key] = view
	return nil
}

func (c *mockCatalog) DeleteQueryView(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.saved, key)
	c.deleted = append(c.deleted, key)
	return nil
}

func (c *mockCatalog) ListQueryViews() ([]*viewpb.QueryViewOfShard, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []*viewpb.QueryViewOfShard
	for _, v := range c.saved {
		result = append(result, v)
	}
	return result, nil
}

func (c *mockCatalog) savedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.saved)
}

func (c *mockCatalog) deletedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.deleted)
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func buildHandlerTestMeta(version int64) *viewpb.QueryViewMeta {
	return &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: version, CompactVersion: 1},
			QueryVersion: version,
		},
		State: viewpb.QueryViewState_QueryViewStatePreparing,
	}
}

func newPreparingSNView(version int64) qviews.QueryViewAtWorkNode {
	return qviews.NewQueryViewAtStreamingNode(buildHandlerTestMeta(version), &viewpb.QueryViewOfStreamingNode{})
}

func newSNViewWithState(version int64, state viewpb.QueryViewState) qviews.QueryViewAtWorkNode {
	meta := buildHandlerTestMeta(version)
	meta.State = state
	return qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
}

type reportCollector struct {
	mu      sync.Mutex
	reports []qviews.QueryViewAtWorkNode
}

func (c *reportCollector) onReport(report qviews.QueryViewAtWorkNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reports = append(c.reports, report)
}

func (c *reportCollector) get() []qviews.QueryViewAtWorkNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]qviews.QueryViewAtWorkNode{}, c.reports...)
}

func (c *reportCollector) last() qviews.QueryViewAtWorkNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reports) == 0 {
		return nil
	}
	return c.reports[len(c.reports)-1]
}

func (c *reportCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reports)
}

// ---------------------------------------------------------------------------
// 1. ApplyViews — new Preparing view
// ---------------------------------------------------------------------------

func TestSNHandler_ApplyViews_NewPreparing(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newPreparingSNView(1), OnReport: rc.onReport},
	})

	// SN SM generates Preparing report on construction.
	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStatePreparing, rc.last().State())
	// No persistence for Preparing.
	assert.Equal(t, 0, cat.savedCount())
}

func TestSNHandler_ApplyViews_NonPreparingNewViewIgnored(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})

	assert.Equal(t, 0, rc.count())
}

// ---------------------------------------------------------------------------
// 2. NotifyReady — Preparing → Ready
// ---------------------------------------------------------------------------

func TestSNHandler_NotifyReady(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	h.NotifyReady(key)

	require.Equal(t, 2, rc.count()) // Preparing + Ready
	assert.Equal(t, qviews.QueryViewStateReady, rc.last().State())
	// No persistence for Ready.
	assert.Equal(t, 0, cat.savedCount())
}

// ---------------------------------------------------------------------------
// 3. Coord Up — Ready → Up (persists)
// ---------------------------------------------------------------------------

func TestSNHandler_CoordUp_PersistsRecoveryInfo(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	h.NotifyReady(key)

	// Coord pushes Up.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
	})

	require.Equal(t, 3, rc.count()) // Preparing + Ready + Up
	assert.Equal(t, qviews.QueryViewStateUp, rc.last().State())
	assert.Equal(t, 1, cat.savedCount())
}

// ---------------------------------------------------------------------------
// 4. Coord Down — Up → Down (deletes recovery info)
// ---------------------------------------------------------------------------

func TestSNHandler_CoordDown_DeletesRecoveryInfo(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	h.NotifyReady(key)

	// Coord Up.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
	})
	assert.Equal(t, 1, cat.savedCount())

	// Coord Down.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown), OnReport: rc.onReport},
	})
	assert.Equal(t, qviews.QueryViewStateDown, rc.last().State())
	assert.Equal(t, 0, cat.savedCount()) // deleted
	assert.Equal(t, 1, cat.deletedCount())
}

// ---------------------------------------------------------------------------
// 5. Coord Dropped — auto-destroy
// ---------------------------------------------------------------------------

func TestSNHandler_CoordDropped_AutoDestroy(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	// Push Dropped.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})

	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())

	// Verify entry is removed: NotifyReady should be a no-op.
	key := view.QueryViewKey()
	countBefore := rc.count()
	h.NotifyReady(key)
	assert.Equal(t, countBefore, rc.count())
}

// ---------------------------------------------------------------------------
// 6. NotifyUnrecoverable
// ---------------------------------------------------------------------------

func TestSNHandler_NotifyUnrecoverable(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	h.NotifyUnrecoverable(key)

	assert.Equal(t, qviews.QueryViewStateUnrecoverable, rc.last().State())
	// No persist needed from Preparing → Unrecoverable.
	assert.Equal(t, 0, cat.savedCount())
}

func TestSNHandler_NotifyUnrecoverable_FromUpRecovering_DeletesPersist(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	// Simulate persisted Up view.
	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateUp
	persistedView := &viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
	cat.SaveQueryView(buildPersistKey(meta), persistedView)
	assert.Equal(t, 1, cat.savedCount())

	// Recover.
	h.Recover([]*viewpb.QueryViewOfShard{persistedView})

	// Set up report callback via ApplyViews re-push.
	rc := &reportCollector{}
	preparingView := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: preparingView, OnReport: rc.onReport},
	})
	// UpRecovering SM reports nothing for Preparing re-push (waits for WAL).

	key := preparingView.QueryViewKey()
	h.NotifyUnrecoverable(key)

	assert.Equal(t, qviews.QueryViewStateUnrecoverable, rc.last().State())
	// Recovery info should be deleted.
	assert.Equal(t, 0, cat.savedCount())
}

// ---------------------------------------------------------------------------
// 7. Recover — crash recovery
// ---------------------------------------------------------------------------

func TestSNHandler_Recover_CreatesUpRecoveringViews(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateUp
	persistedView := &viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}

	h.Recover([]*viewpb.QueryViewOfShard{persistedView})

	// Register callback via ApplyViews (simulating Coord re-push).
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newPreparingSNView(1), OnReport: rc.onReport},
	})

	// UpRecovering: Coord re-push Preparing → no report (SM suppresses).
	assert.Equal(t, 0, rc.count())

	// WAL catches up.
	key := newPreparingSNView(1).QueryViewKey()
	h.NotifyRecoveringDone(key)

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateUp, rc.last().State())
	// Already persisted as Up — no new save.
	assert.Equal(t, 0, cat.savedCount())
}

// ---------------------------------------------------------------------------
// 8. Callback replacement on re-apply
// ---------------------------------------------------------------------------

func TestSNHandler_CallbackReplacement(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc1 := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc1.onReport},
	})
	assert.Equal(t, 1, rc1.count()) // Preparing

	// Make Ready.
	key := view.QueryViewKey()
	h.NotifyReady(key)
	assert.Equal(t, 2, rc1.count()) // Preparing + Ready

	// Re-apply with new callback.
	rc2 := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newPreparingSNView(1), OnReport: rc2.onReport},
	})

	// rc1 unchanged, rc2 gets re-report.
	assert.Equal(t, 2, rc1.count())
	require.Equal(t, 1, rc2.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc2.last().State())
}

// ---------------------------------------------------------------------------
// 9. Full lifecycle
// ---------------------------------------------------------------------------

func TestSNHandler_FullLifecycle(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	key := view.QueryViewKey()

	// 1. Apply Preparing → report Preparing.
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})
	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStatePreparing, rc.last().State())

	// 2. NotifyReady → Ready.
	h.NotifyReady(key)
	require.Equal(t, 2, rc.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc.last().State())

	// 3. Coord Up → persist.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
	})
	require.Equal(t, 3, rc.count())
	assert.Equal(t, qviews.QueryViewStateUp, rc.last().State())
	assert.Equal(t, 1, cat.savedCount())

	// 4. Coord Down → delete persist.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown), OnReport: rc.onReport},
	})
	require.Equal(t, 4, rc.count())
	assert.Equal(t, qviews.QueryViewStateDown, rc.last().State())
	assert.Equal(t, 0, cat.savedCount())

	// 5. Coord Dropped → auto-destroy.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})
	require.Equal(t, 5, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())

	// 6. Further notifications are no-op.
	h.NotifyReady(key)
	assert.Equal(t, 5, rc.count())
}

// ---------------------------------------------------------------------------
// 10. Multiple versions in same shard
// ---------------------------------------------------------------------------

func TestSNHandler_MultipleVersions(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc1 := &reportCollector{}
	rc2 := &reportCollector{}
	view1 := newPreparingSNView(1)
	view2 := newPreparingSNView(2)

	h.ApplyViews([]handler.ApplyView{
		{View: view1, OnReport: rc1.onReport},
		{View: view2, OnReport: rc2.onReport},
	})

	// Both get Preparing report.
	assert.Equal(t, 1, rc1.count())
	assert.Equal(t, 1, rc2.count())

	// Only notify version 1 ready.
	key1 := view1.QueryViewKey()
	h.NotifyReady(key1)

	assert.Equal(t, 2, rc1.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc1.last().State())
	assert.Equal(t, 1, rc2.count()) // version 2 unaffected
}

// ---------------------------------------------------------------------------
// 11. Close
// ---------------------------------------------------------------------------

func TestSNHandler_Close(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)

	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: func(qviews.QueryViewAtWorkNode) {}},
	})

	h.Close()

	// After close, notifications are no-op.
	key := view.QueryViewKey()
	h.NotifyReady(key)
}

// ---------------------------------------------------------------------------
// 12. Concurrency safety
// ---------------------------------------------------------------------------

func TestSNHandler_ConcurrentApplyAndNotify(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	const numViews = 20
	var wg sync.WaitGroup
	var readyCount atomic.Int32

	for i := int64(1); i <= numViews; i++ {
		view := newPreparingSNView(i)
		h.ApplyViews([]handler.ApplyView{
			{View: view, OnReport: func(report qviews.QueryViewAtWorkNode) {
				if report.State() == qviews.QueryViewStateReady {
					readyCount.Add(1)
				}
			}},
		})
	}

	// Notify all views concurrently.
	for i := int64(1); i <= numViews; i++ {
		wg.Add(1)
		go func(version int64) {
			defer wg.Done()
			view := newPreparingSNView(version)
			key := view.QueryViewKey()
			h.NotifyReady(key)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int32(numViews), readyCount.Load())
}

// ---------------------------------------------------------------------------
// 13. Recover with callback via ApplyViews re-push, then Coord Down
// ---------------------------------------------------------------------------

func TestSNHandler_Recover_ThenCoordDown(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateUp
	persistedView := &viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
	cat.SaveQueryView(buildPersistKey(meta), persistedView)

	h.Recover([]*viewpb.QueryViewOfShard{persistedView})

	// Coord pushes Down to recovered view.
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown), OnReport: rc.onReport},
	})

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateDown, rc.last().State())
	// Recovery info deleted.
	assert.Equal(t, 0, cat.savedCount())
}

// ---------------------------------------------------------------------------
// 14. NotifyRecoveringDone on unknown shard/version
// ---------------------------------------------------------------------------

func TestSNHandler_NotifyRecoveringDone_UnknownIgnored(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	h.NotifyRecoveringDone(qviews.QueryViewKey{ShardID: qviews.ShardID{ReplicaID: 999, VChannel: "unknown"}})
}

// ---------------------------------------------------------------------------
// 15. Dropped from Up — deletes recovery info
// ---------------------------------------------------------------------------

func TestSNHandler_DroppedFromUp_DeletesRecoveryInfo(t *testing.T) {
	cat := newMockCatalog()
	h := NewSNQueryViewHandler(cat)
	defer h.Close()

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	key := view.QueryViewKey()

	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})
	h.NotifyReady(key)

	// Up.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
	})
	assert.Equal(t, 1, cat.savedCount())

	// Dropped directly from Up.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())
	assert.Equal(t, 0, cat.savedCount())
}
