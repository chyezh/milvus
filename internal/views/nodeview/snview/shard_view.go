package snview

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/views/nodeview/handler"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// snShardView manages all query view state machines for a single shard on a StreamingNode.
// All public methods are concurrent-safe via the internal mutex.
type snShardView struct {
	mu      sync.Mutex
	views   map[qviews.QueryViewVersion]*snViewEntry
	catalog StreamingNodeCatalog
}

// snViewEntry pairs an ApplyView (carrying the OnReport callback) with its state machine.
type snViewEntry struct {
	handler.ApplyView
	sm *SNQueryViewStateMachine
}

// Recover adds a recovered view entry from persisted state.
// Called during startup before any concurrent access.
func (s *snShardView) Recover(version qviews.QueryViewVersion, sm *SNQueryViewStateMachine) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.views[version] = &snViewEntry{sm: sm}
}

// ApplyViews applies a batch of coord-pushed views atomically.
func (s *snShardView) ApplyViews(views []handler.ApplyView) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range views {
		s.applyOneLocked(&views[i])
	}
}

// applyOneLocked applies a single view. Caller must hold s.mu.
func (s *snShardView) applyOneLocked(av *handler.ApplyView) {
	key := av.View.QueryViewKey()
	entry, exists := s.views[key.QueryViewVersion]
	pushedState := av.View.State()

	if !exists {
		if pushedState != qviews.QueryViewStatePreparing {
			return
		}
		// New Preparing view: create SM.
		snView := av.View.(*qviews.QueryViewAtStreamingNode)
		sm := NewSNQueryViewStateMachine(
			snView.IntoProto().Meta,
			snView.ViewOfStreamingNode(),
		)
		entry = &snViewEntry{ApplyView: *av, sm: sm}
		s.views[key.QueryViewVersion] = entry
		// SN SM constructor generates a Preparing report.
		// New entry cannot be Dropped, so no cleanup needed.
		s.consumeReport(entry)
		return
	}

	// Existing view: replace callback and deliver coord push.
	entry.ApplyView = *av
	entry.sm.OnCoordStateDelivered(pushedState)
	s.consumeReportPersistAndCleanup(key.QueryViewVersion, entry)
}

// NotifyReady is called when async resource preparation completes.
// Transitions SM from Preparing → Ready.
func (s *snShardView) NotifyReady(version qviews.QueryViewVersion) {
	s.notifyEvent(version, func(sm *SNQueryViewStateMachine) {
		sm.OnReady()
	})
}

// NotifyRecoveringDone is called after WAL catch-up during crash recovery.
// Transitions SM from UpRecovering → Up.
func (s *snShardView) NotifyRecoveringDone(version qviews.QueryViewVersion) {
	s.notifyEvent(version, func(sm *SNQueryViewStateMachine) {
		sm.OnRecoveringDone()
	})
}

// NotifyUnrecoverable is called when a fatal error occurs.
func (s *snShardView) NotifyUnrecoverable(version qviews.QueryViewVersion) {
	s.notifyEvent(version, func(sm *SNQueryViewStateMachine) {
		sm.OnUnrecoverable()
	})
}

func (s *snShardView) notifyEvent(version qviews.QueryViewVersion, fn func(*SNQueryViewStateMachine)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	fn(entry.sm)
	s.consumeReportPersistAndCleanup(version, entry)
}

// consumeReport drains pending report and invokes callback.
// Caller must hold s.mu.
func (s *snShardView) consumeReport(entry *snViewEntry) {
	report := entry.sm.ConsumeReport()
	if report != nil && entry.OnReport != nil {
		entry.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(report))
	}
}

// consumeReportPersistAndCleanup drains pending report and persist,
// invokes callbacks, and removes the entry if it has reached Dropped state.
// Caller must hold s.mu.
func (s *snShardView) consumeReportPersistAndCleanup(version qviews.QueryViewVersion, entry *snViewEntry) {
	s.consumeReport(entry)
	s.consumeAndPersist(entry)
	if entry.sm.State() == qviews.QueryViewStateDropped {
		delete(s.views, version)
	}
}

// consumeAndPersist drains pending persist and writes to catalog.
// Caller must hold s.mu.
func (s *snShardView) consumeAndPersist(entry *snViewEntry) {
	persist := entry.sm.ConsumePersist()
	if persist == nil {
		return
	}
	key := buildPersistKey(persist.Meta)
	persistState := qviews.QueryViewState(persist.Meta.State)
	switch persistState {
	case qviews.QueryViewStateUp:
		s.catalog.SaveQueryView(key, persist)
	default:
		// Down, Unrecoverable, Dropped → delete recovery info.
		s.catalog.DeleteQueryView(key)
	}
}

// buildPersistKey constructs a unique persistence key from view metadata.
func buildPersistKey(meta *viewpb.QueryViewMeta) string {
	return fmt.Sprintf("%d/%s/%d/%d/%d",
		meta.ReplicaId,
		meta.Vchannel,
		meta.Version.DataVersion.StreamingVersion,
		meta.Version.DataVersion.CompactVersion,
		meta.Version.QueryVersion,
	)
}
