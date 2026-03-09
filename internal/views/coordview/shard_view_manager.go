package coordview

import (
	"context"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

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
// Thread-safety: All methods are thread-safe.
type ShardViewManager struct {
	ctx     context.Context // lifecycle context for Catalog calls within callbacks
	mu      sync.Mutex
	shardID qviews.ShardID
	catalog queryview.QueryViewCatalog
	syncer  syncer.ReliableSyncer

	// Active views ordered by version (ascending).
	views []*CoordQueryViewStateMachine
}

// ioBatch accumulates persist and sync operations for batch execution.
// All persists are flushed in a single SaveQueryViews call, then all syncs
// are flushed in a single SyncViews call. This ensures atomicity at the
// persistence layer and reduces the number of I/O calls.
type ioBatch struct {
	persists []*viewpb.QueryViewOfShard
	syncs    []syncEntry
}

// syncEntry pairs a state machine with its sync proto for deferred sync dispatch.
type syncEntry struct {
	sm        *CoordQueryViewStateMachine
	syncProto *viewpb.QueryViewOfShard
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
	var batch ioBatch
	for _, sm := range m.views {
		m.processStateMachine(sm, &batch)
	}
	m.flush(&batch)

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

	var batch ioBatch

	// Find and preempt existing Preparing/Ready view.
	for _, sm := range m.views {
		if sm.State() == qviews.QueryViewStatePreparing || sm.State() == qviews.QueryViewStateReady {
			sm.OnNodeStateReported(m.buildSyntheticUnrecoverable(sm))
			m.processStateMachine(sm, &batch)
			break // at most one Preparing/Ready view
		}
	}

	// Compute and assign QueryVersion.
	qv := m.nextQueryVersion(newDV)
	builder.SetQueryVersion(qv)

	// Build the view proto and create the state machine.
	view := builder.Build()
	sm := NewCoordQueryViewStateMachine(view)
	m.views = append(m.views, sm)

	// Process: persist write-ahead + collect sync.
	m.processStateMachine(sm, &batch)

	// Flush all accumulated I/O.
	m.flush(&batch)
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

	var batch ioBatch
	for _, sm := range m.views {
		switch sm.State() {
		case qviews.QueryViewStatePreparing, qviews.QueryViewStateReady:
			// Abort: inject synthetic Unrecoverable SN response to trigger teardown.
			sm.OnNodeStateReported(m.buildSyntheticUnrecoverable(sm))
			m.processStateMachine(sm, &batch)

		case qviews.QueryViewStateUp:
			sm.EnterDown()
			m.processStateMachine(sm, &batch)
		}
	}
	m.flush(&batch)
	return nil
}

// processStateMachine consumes pending I/O from a state machine and handles
// cascading effects (Up-then-Down, Unrecoverable→Dropping, Dropped removal).
// I/O is collected into the batch for deferred execution.
//
// Must be called under m.mu.
func (m *ShardViewManager) processStateMachine(sm *CoordQueryViewStateMachine, batch *ioBatch) {
	for {
		// 1. ConsumePersist → collect into batch.
		if persist := sm.ConsumePersist(); persist != nil {
			batch.persists = append(batch.persists, persist)
		}

		// 2. ConsumeSync → collect into batch.
		if syncProto := sm.ConsumeSync(); syncProto != nil {
			batch.syncs = append(batch.syncs, syncEntry{sm: sm, syncProto: syncProto})
		}

		// 3. Handle cascading effects based on current state.
		switch sm.State() {
		case qviews.QueryViewStateUp:
			m.downOlderUpViews(sm, batch)
			return

		case qviews.QueryViewStateUnrecoverable:
			// Immediately advance to Dropping.
			sm.EnterDropping()
			continue // loop to process Dropping's pendingSync

		case qviews.QueryViewStateDropped:
			m.removeView(sm)
			return

		default:
			return
		}
	}
}

// downOlderUpViews transitions all Up views with version lower than newUp to Down.
//
// Must be called under m.mu.
func (m *ShardViewManager) downOlderUpViews(newUp *CoordQueryViewStateMachine, batch *ioBatch) {
	newVersion := qviews.FromProtoQueryViewVersion(newUp.View().Meta.Version)
	for _, sm := range m.views {
		if sm == newUp {
			continue
		}
		if sm.State() != qviews.QueryViewStateUp {
			continue
		}
		smVersion := qviews.FromProtoQueryViewVersion(sm.View().Meta.Version)
		if newVersion.GT(smVersion) {
			sm.EnterDown()
			m.processStateMachine(sm, batch)
		}
	}
}

// flush executes all accumulated I/O in the batch: first persist all, then sync all.
//
// Must be called under m.mu.
func (m *ShardViewManager) flush(batch *ioBatch) {
	// 1. Persist all in a single call.
	if len(batch.persists) > 0 {
		if err := m.catalog.SaveQueryViews(m.ctx, batch.persists); err != nil {
			log.Ctx(m.ctx).Warn("failed to persist query views",
				zap.String("shardID", m.shardID.String()),
				zap.Int("count", len(batch.persists)),
				zap.Error(err),
			)
		}
	}

	// 2. Sync all in a single call.
	if len(batch.syncs) > 0 {
		viewsByNode := make(map[qviews.WorkNodeKey][]syncer.SyncView)
		for _, entry := range batch.syncs {
			m.collectSyncViews(entry.sm, entry.syncProto, viewsByNode)
		}
		if len(viewsByNode) > 0 {
			if err := m.syncer.SyncViews(m.ctx, syncer.SyncGroup{ViewsByNode: viewsByNode}); err != nil {
				log.Ctx(m.ctx).Warn("failed to sync views to nodes",
					zap.String("shardID", m.shardID.String()),
					zap.Error(err),
				)
			}
		}
	}

	// Clear batch.
	batch.persists = nil
	batch.syncs = nil
}

// collectSyncViews appends sync views to the shared viewsByNode map based on
// the sync proto's state and routing rules.
//
// Routing table:
//
//	Preparing → SN + all QNs
//	Up        → SN only
//	Down      → SN only
//	Dropped   → SN + all QNs
//
// Must be called under m.mu.
func (m *ShardViewManager) collectSyncViews(
	sm *CoordQueryViewStateMachine,
	syncProto *viewpb.QueryViewOfShard,
	viewsByNode map[qviews.WorkNodeKey][]syncer.SyncView,
) {
	version := qviews.FromProtoQueryViewVersion(sm.View().Meta.Version)
	state := qviews.QueryViewState(syncProto.Meta.State)

	// SN is always included.
	snView := qviews.NewQueryViewAtStreamingNode(syncProto.Meta, syncProto.StreamingNode)
	snKey := snView.WorkNode().Key()
	viewsByNode[snKey] = append(viewsByNode[snKey], syncer.SyncView{
		View:           snView,
		OnSyncResponse: m.makeOnSyncResponse(version),
		OnNodeLost:     m.makeOnNodeLost(sm, snView.WorkNode()),
	})

	// QNs included for Preparing and Dropped only.
	if state == qviews.QueryViewStatePreparing || state == qviews.QueryViewStateDropped {
		for _, qn := range syncProto.QueryNode {
			qnView := qviews.NewQueryViewAtQueryNode(syncProto.Meta, qn)
			qnKey := qnView.WorkNode().Key()
			viewsByNode[qnKey] = append(viewsByNode[qnKey], syncer.SyncView{
				View:           qnView,
				OnSyncResponse: m.makeOnSyncResponse(version),
				OnNodeLost:     m.makeOnNodeLost(sm, qnView.WorkNode()),
			})
		}
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

		var batch ioBatch
		m.processStateMachine(sm, &batch)
		m.flush(&batch)

		// If the view was removed during processing, stop tracking.
		return m.findByVersion(version) == nil
	}
}

// makeOnNodeLost creates a callback invoked when the target node is declared lost.
// It injects a synthetic Unrecoverable response into the state machine.
func (m *ShardViewManager) makeOnNodeLost(sm *CoordQueryViewStateMachine, node qviews.WorkNode) func() {
	view := sm.View()
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		version := qviews.FromProtoQueryViewVersion(view.Meta.Version)
		foundSM := m.findByVersion(version)
		if foundSM == nil {
			return // view already removed
		}

		// Build and inject synthetic Unrecoverable response.
		meta := proto.Clone(view.Meta).(*viewpb.QueryViewMeta)
		meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable

		var resp qviews.QueryViewAtWorkNode
		switch n := node.(type) {
		case qviews.StreamingNode:
			resp = qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
		case qviews.QueryNode:
			resp = qviews.NewQueryViewAtQueryNode(meta, &viewpb.QueryViewOfQueryNode{NodeId: n.ID})
		default:
			panic("coordview: unknown work node type")
		}

		foundSM.OnNodeStateReported(resp)

		var batch ioBatch
		m.processStateMachine(foundSM, &batch)
		m.flush(&batch)
	}
}

// buildSyntheticUnrecoverable creates a synthetic Unrecoverable SN response
// for preemption and RequestRelease to abort Preparing/Ready views.
func (m *ShardViewManager) buildSyntheticUnrecoverable(sm *CoordQueryViewStateMachine) qviews.QueryViewAtWorkNode {
	meta := proto.Clone(sm.View().Meta).(*viewpb.QueryViewMeta)
	meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
	return qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
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

// removeView removes the state machine from the views list.
//
// Must be called under m.mu.
func (m *ShardViewManager) removeView(target *CoordQueryViewStateMachine) {
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
