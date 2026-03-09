package coordview

import (
	"context"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
	"go.uber.org/zap"
)

// ShardViewManager manages multiple QueryViews for a single shard (vchannel)
// within a single replica on the Coord side.
//
// It orchestrates CoordQueryViewStateMachine instances and their cross-view
// interactions, calling Catalog for ETCD persistence and ReliableSyncer for
// node sync. Node responses are delivered via callbacks registered with the syncer.
//
// Invariants (maintained by all methods):
//   - At most one view in Preparing or Ready state (tracked by preparingView).
//   - At most one view in Up state (tracked by upView).
//
// Thread-safety: All methods are thread-safe.
type ShardViewManager struct {
	ctx     context.Context // lifecycle context for Catalog calls within callbacks
	mu      sync.Mutex
	shardID qviews.ShardID
	catalog queryview.QueryViewCatalog
	syncer  syncer.ReliableSyncer

	// All active views.
	views []*CoordQueryViewStateMachine

	// Fast pointers to the unique Preparing/Ready and Up views.
	// Invariant: at most one of each at any time.
	preparingView *CoordQueryViewStateMachine // Preparing or Ready state; nil if none
	upView        *CoordQueryViewStateMachine // Up state; nil if none

	// Accumulates persist and sync operations within a single lock-hold scope.
	// All persists are flushed in a single SaveQueryViews call, then all syncs
	// are flushed in a single SyncViews call. This ensures atomicity at the
	// persistence layer and reduces the number of I/O calls.
	// Must only be accessed under m.mu.
	pendingPersists []*viewpb.QueryViewOfShard
	pendingSyncs    []syncEntry
}

// syncEntry pairs a state machine with its per-node views for deferred sync dispatch.
type syncEntry struct {
	sm    *CoordQueryViewStateMachine
	views []qviews.QueryViewAtWorkNode
}

// NewShardViewManager creates a new ShardViewManager for the given shard.
//
// ctx is the lifecycle context used for Catalog calls within callbacks.
// recoveredViews are views loaded from ETCD during crash recovery.
// During construction, Unrecoverable views are immediately advanced to Dropping,
// and all active views are pushed to their target nodes via syncer.
func NewShardViewManager(
	ctx context.Context,
	shardID qviews.ShardID,
	catalog queryview.QueryViewCatalog,
	s syncer.ReliableSyncer,
	recoveredViews []*viewpb.QueryViewOfShard,
) *ShardViewManager {
	m := &ShardViewManager{
		ctx:     ctx,
		shardID: shardID,
		catalog: catalog,
		syncer:  s,
		views:   make([]*CoordQueryViewStateMachine, 0),
	}

	// Recover state machines from persisted views.
	for _, view := range recoveredViews {
		sm := RecoverCoordQueryViewStateMachine(view)
		m.views = append(m.views, sm)
	}

	// Sort by version ascending.
	sort.Slice(m.views, func(i, j int) bool {
		vi := qviews.FromProtoQueryViewVersion(m.views[i].View().Meta.Version)
		vj := qviews.FromProtoQueryViewVersion(m.views[j].View().Meta.Version)
		return vj.GT(vi)
	})

	// Process each recovered view: handle Unrecoverable and push initial syncs.
	// processStateMachine sets preparingView/upView as views are processed.
	for _, sm := range m.views {
		m.processStateMachine(sm)
	}
	m.flush()
	return m
}

// ShardID returns the shard identifier (ReplicaID + VChannel).
func (m *ShardViewManager) ShardID() qviews.ShardID {
	return m.shardID
}

// AddPreparing adds a new view in Preparing state from a builder.
//
// The manager assigns the QueryVersion automatically:
//   - If the DataVersion matches existing views, QV = max(existing QV for same DV) + 1.
//   - Otherwise, QV = 1.
//
// Preemption: If an existing view is in Preparing or Ready state, it is preempted
// (injected with synthetic Unrecoverable → Dropping).
//
// Validation: The new DataVersion must not be lower than any existing view's DataVersion.
func (m *ShardViewManager) AddPreparing(ctx context.Context, builder *qviews.QueryViewAtCoordBuilder) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	newDV := builder.DataVersion()

	// Validate no DataVersion rollback.
	if err := m.validateDataVersionLocked(newDV); err != nil {
		return err
	}

	// Preempt existing Preparing/Ready view.
	if m.preparingView != nil {
		m.preparingView.EnterUnrecoverable()
		m.processStateMachine(m.preparingView)
		// preparingView is cleared by processStateMachine (Unrecoverable case).
	}

	// Advance all Unrecoverable views (preempted or naturally failed) to
	// Dropping so their Dropped sync is batched with the new Preparing sync.
	m.advanceUnrecoverableToDropping()

	// Compute and assign QueryVersion.
	qv := m.nextQueryVersion(newDV)
	builder.SetQueryVersion(qv)

	// Build the view proto and create the state machine.
	view := builder.Build()
	sm := NewCoordQueryViewStateMachine(view)
	m.views = append(m.views, sm)
	m.preparingView = sm

	// Process: persist write-ahead + collect sync.
	m.processStateMachine(sm)

	// Flush all accumulated I/O.
	m.flush()
	return nil
}

// RequestRelease initiates teardown of all views in this shard.
//
// - Up views: transition to Down (normal teardown via SN confirmation).
// - Preparing/Ready views: force Unrecoverable → Dropping (abort immediately).
// - Down/Dropping views: already tearing down, no-op.
//
// The actual cleanup completes asynchronously through callbacks.
func (m *ShardViewManager) RequestRelease(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.preparingView != nil {
		m.preparingView.EnterUnrecoverable()
		m.processStateMachine(m.preparingView)
		// preparingView is cleared by processStateMachine (Unrecoverable case).
	}

	if m.upView != nil {
		m.upView.EnterDown()
		m.processStateMachine(m.upView)
		m.upView = nil
	}

	// Advance all Unrecoverable views (preempted or naturally failed) to Dropping.
	m.advanceUnrecoverableToDropping()

	m.flush()
	return nil
}

// processStateMachine consumes pending I/O from a state machine and handles
// cascading effects (Up-then-Down, Unrecoverable→Dropping, Dropped removal).
// I/O is collected into pendingPersists/pendingSyncs for deferred execution.
//
// Also maintains preparingView/upView pointers on state transitions.
//
// Must be called under m.mu.
func (m *ShardViewManager) processStateMachine(sm *CoordQueryViewStateMachine) {
	for {
		// 1. ConsumePersist → collect into pending batch.
		if persist := sm.ConsumePersist(); persist != nil {
			m.pendingPersists = append(m.pendingPersists, persist)
		}

		// 2. ConsumeSync → collect into pending batch.
		if views := sm.ConsumeSync(); len(views) > 0 {
			m.pendingSyncs = append(m.pendingSyncs, syncEntry{sm: sm, views: views})
		}

		// 3. Handle cascading effects based on current state.
		switch sm.State() {
		case qviews.QueryViewStatePreparing, qviews.QueryViewStateReady:
			m.preparingView = sm
			return

		case qviews.QueryViewStateUp:
			if m.preparingView == sm {
				m.preparingView = nil
			}
			m.downOlderUpView(sm)
			m.upView = sm
			return

		case qviews.QueryViewStateUnrecoverable:
			if m.preparingView == sm {
				m.preparingView = nil
			}
			if m.upView == sm {
				m.upView = nil
			}
			// Stay Unrecoverable; wait for AddPreparing or RequestRelease
			// to advance to Dropping so that Dropped sync and new Preparing
			// sync can be batched together.
			return

		case qviews.QueryViewStateDropped:
			m.removeView(sm)
			return

		default:
			return
		}
	}
}

// advanceUnrecoverableToDropping advances all Unrecoverable views to Dropping.
// This batches the Dropped sync with whatever operation triggered it
// (AddPreparing or RequestRelease), reducing the number of sync round-trips.
//
// Must be called under m.mu.
func (m *ShardViewManager) advanceUnrecoverableToDropping() {
	for _, sm := range m.views {
		if sm.State() == qviews.QueryViewStateUnrecoverable {
			sm.EnterDropping()
			m.processStateMachine(sm)
		}
	}
}

// downOlderUpView transitions the current Up view to Down if it differs from newUp.
//
// Must be called under m.mu.
func (m *ShardViewManager) downOlderUpView(newUp *CoordQueryViewStateMachine) {
	if m.upView != nil && m.upView != newUp {
		m.upView.EnterDown()
		m.processStateMachine(m.upView)
		// upView is overwritten by caller after return (m.upView = sm).
	}
}

// flush executes all accumulated I/O: first persist all, then sync all.
//
// Must be called under m.mu.
func (m *ShardViewManager) flush() {
	// 1. Persist all in a single call.
	if len(m.pendingPersists) > 0 {
		if err := m.catalog.SaveQueryViews(m.ctx, m.pendingPersists); err != nil {
			log.Ctx(m.ctx).Warn("failed to persist query views",
				zap.String("shardID", m.shardID.String()),
				zap.Int("count", len(m.pendingPersists)),
				zap.Error(err),
			)
		}
		m.pendingPersists = m.pendingPersists[:0]
	}

	// 2. Sync all in a single call.
	if len(m.pendingSyncs) > 0 {
		viewsByNode := make(map[qviews.WorkNodeKey][]syncer.SyncView)
		for _, entry := range m.pendingSyncs {
			version := qviews.FromProtoQueryViewVersion(entry.sm.View().Meta.Version)
			for _, view := range entry.views {
				key := view.WorkNode().Key()
				viewsByNode[key] = append(viewsByNode[key], syncer.SyncView{
					View:           view,
					OnSyncResponse: m.makeOnSyncResponse(version),
					OnNodeLost:     m.makeOnNodeLost(entry.sm),
				})
			}
		}
		if len(viewsByNode) > 0 {
			if err := m.syncer.SyncViews(m.ctx, syncer.SyncGroup{ViewsByNode: viewsByNode}); err != nil {
				log.Ctx(m.ctx).Warn("failed to sync views to nodes",
					zap.String("shardID", m.shardID.String()),
					zap.Error(err),
				)
			}
		}
		m.pendingSyncs = m.pendingSyncs[:0]
	}
}

// makeOnSyncResponse creates a callback that processes node responses for a view.
//
// The callback acquires m.mu, calls sm.OnNodeStateReported, calls processStateMachine.
// Returns true when the view is removed (Dropped), stopping ReliableSyncer tracking.
func (m *ShardViewManager) makeOnSyncResponse(version qviews.QueryViewVersion) func(resp qviews.QueryViewAtWorkNode) bool {
	return func(resp qviews.QueryViewAtWorkNode) bool {
		m.mu.Lock()
		defer m.mu.Unlock()

		sm := m.findByVersion(version)
		if sm == nil {
			return true // view already removed, stop tracking
		}

		sm.OnNodeStateReported(resp)
		m.processStateMachine(sm)
		m.flush()

		// If the view was removed during processing, stop tracking.
		return m.findByVersion(version) == nil
	}
}

// makeOnNodeLost creates a callback invoked when the target node is declared lost.
// It transitions the view to Unrecoverable directly.
func (m *ShardViewManager) makeOnNodeLost(sm *CoordQueryViewStateMachine) func() {
	view := sm.View()
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		version := qviews.FromProtoQueryViewVersion(view.Meta.Version)
		foundSM := m.findByVersion(version)
		if foundSM == nil {
			return // view already removed
		}

		foundSM.EnterUnrecoverable()
		m.processStateMachine(foundSM)
		m.flush()
	}
}

// findByVersion returns the state machine matching the given version, or nil.
//
// Must be called under m.mu.
func (m *ShardViewManager) findByVersion(version qviews.QueryViewVersion) *CoordQueryViewStateMachine {
	for _, sm := range m.views {
		v := qviews.FromProtoQueryViewVersion(sm.View().Meta.Version)
		if v.EQ(version) {
			return sm
		}
	}
	return nil
}

// removeView removes the state machine from the views list and clears any
// fast pointers that reference it.
//
// Must be called under m.mu.
func (m *ShardViewManager) removeView(target *CoordQueryViewStateMachine) {
	if m.preparingView == target {
		m.preparingView = nil
	}
	if m.upView == target {
		m.upView = nil
	}
	for i, sm := range m.views {
		if sm == target {
			m.views = append(m.views[:i], m.views[i+1:]...)
			return
		}
	}
}

// validateDataVersionLocked checks that the new DataVersion is not lower than
// any existing view's DataVersion.
//
// Must be called under m.mu.
func (m *ShardViewManager) validateDataVersionLocked(newDV qviews.DataVersion) error {
	for _, sm := range m.views {
		existingDV := qviews.FromProtoQueryViewVersion(sm.View().Meta.Version).DataVersion
		if existingDV.GT(newDV) {
			return errDataVersionRollback
		}
	}
	return nil
}

// nextQueryVersion computes the next QueryVersion for a given DataVersion.
// Returns max(QV for views with same DV) + 1, or 1 if no matching DV exists.
//
// Must be called under m.mu.
func (m *ShardViewManager) nextQueryVersion(newDV qviews.DataVersion) int64 {
	var maxQV int64
	for _, sm := range m.views {
		v := qviews.FromProtoQueryViewVersion(sm.View().Meta.Version)
		if v.DataVersion.EQ(newDV) && v.QueryVersion > maxQV {
			maxQV = v.QueryVersion
		}
	}
	return maxQV + 1
}
