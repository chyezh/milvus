package snview

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// snShardView manages all query view state machines for a single shard on a StreamingNode.
// All public methods are concurrent-safe via the internal mutex.
//
// All StreamingNodeResourceManager operations (Acquire, Release) are
// invoked while holding the shard mutex. The ResourceManager's liveness
// contracts require that all callbacks are asynchronous, so this does not
// cause deadlocks.
type snShardView struct {
	mu              sync.Mutex
	pchannel        string
	shardID         qviews.ShardID
	collectionID    int64
	hasCollectionID bool
	views           map[qviews.QueryViewVersion]*snViewEntry
	catalog         metastore.StreamingNodeCataLog
	resMgr          StreamingNodeResourceManager
	onEmpty         func() // called (under mu) when the last view entry is removed
}

// snViewEntry pairs an ApplyView (carrying the OnReport callback) with its state machine.
type snViewEntry struct {
	handler.ApplyView
	sm *snQueryViewStateMachine
}

// recoverSnShardView constructs an snShardView from pre-built recovered state machines
// and starts recovery for each view via ResourceManager (under shard lock).
// Called during handler construction.
func recoverSnShardView(
	pchannel string,
	shardID qviews.ShardID,
	views map[qviews.QueryViewVersion]*snQueryViewStateMachine,
	catalog metastore.StreamingNodeCataLog,
	resMgr StreamingNodeResourceManager,
) *snShardView {
	entries := make(map[qviews.QueryViewVersion]*snViewEntry, len(views))
	for version, sm := range views {
		// Populate ApplyView.View from SM's meta+snView so that
		// consumeAndRelease can safely call entry.View.QueryViewKey().
		view := qviews.NewQueryViewAtWorkNodeFromProto(&viewpb.QueryViewOfShard{
			Meta:          sm.Meta(),
			StreamingNode: sm.SNView(),
		})
		entries[version] = &snViewEntry{
			ApplyView: handler.ApplyView{View: view},
			sm:        sm,
		}
	}
	s := &snShardView{
		pchannel: pchannel,
		shardID:  shardID,
		views:    entries,
		catalog:  catalog,
		resMgr:   resMgr,
	}
	for _, sm := range views {
		s.setCollectionIDLocked(sm.Meta().GetCollectionId())
		break
	}

	// Start recovery for each view via ResourceManager under shard lock.
	versions := make([]qviews.QueryViewVersion, 0, len(views))
	for version := range views {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[j].GT(versions[i])
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, version := range versions {
		key := qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version}
		v := version // capture loop variable
		qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeRecoverAcquireResourceEvent{
			View: key,
		})
		resMgr.Acquire(AcquireResource{
			Key:  key,
			Meta: views[version].Meta(),
			OnReady: func() {
				s.notifyRecoveringDone(v)
			},
		})
	}

	return s
}

// ApplyViews applies a batch of coord-pushed views atomically.
// Preparing and Up views are processed first so new serving candidates are
// installed before old views are released.
func (s *snShardView) ApplyViews(views []handler.ApplyView) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range views {
		state := views[i].View.State()
		if state == qviews.QueryViewStatePreparing || state == qviews.QueryViewStateUp {
			s.applyOneLocked(&views[i])
		}
	}
	for i := range views {
		state := views[i].View.State()
		if state != qviews.QueryViewStatePreparing && state != qviews.QueryViewStateUp {
			s.applyOneLocked(&views[i])
		}
	}
}

func (s *snShardView) CloseForHandoff() {
	s.mu.Lock()
	releases := make([]qviews.QueryViewKey, 0, len(s.views))
	for version, entry := range s.views {
		key := entry.View.QueryViewKey()
		if key.QueryViewVersion == (qviews.QueryViewVersion{}) {
			key = qviews.QueryViewKey{ShardID: s.shardID, QueryViewVersion: version}
		}
		releases = append(releases, key)
	}
	s.views = make(map[qviews.QueryViewVersion]*snViewEntry)
	if s.onEmpty != nil {
		s.onEmpty()
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(releases))
	for _, key := range releases {
		k := key
		s.resMgr.Release(ReleaseResource{
			Key: k,
			OnDropped: func() {
				wg.Done()
			},
		})
	}
	wg.Wait()
}

// applyOneLocked applies a single view. Caller must hold s.mu.
func (s *snShardView) applyOneLocked(av *handler.ApplyView) {
	key := av.View.QueryViewKey()
	s.setCollectionIDLocked(av.View.IntoProto().GetMeta().GetCollectionId())
	entry, exists := s.views[key.QueryViewVersion]
	pushedState := av.View.State()

	if !exists {
		switch pushedState {
		case qviews.QueryViewStatePreparing:
			// New Preparing view: create SM and acquire resources.
			snView := av.View.(*qviews.QueryViewAtStreamingNode)
			sm := newSNQueryViewStateMachine(
				snView.IntoProto().Meta,
				snView.ViewOfStreamingNode(),
			)
			entry = &snViewEntry{ApplyView: *av, sm: sm}
			s.views[key.QueryViewVersion] = entry
			qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeAcquireResourceEvent{
				View: key,
			})
			// SN SM constructor generates a Preparing report.
			s.consumeReport(entry)

			// Tell ResourceManager to prepare resources. Callbacks will drive SM progress.
			version := key.QueryViewVersion
			s.resMgr.Acquire(AcquireResource{
				Key:  key,
				Meta: sm.Meta(),
				OnReady: func() {
					s.notifyReady(version)
				},
			})
		case qviews.QueryViewStateDropped:
			// View doesn't exist (e.g., SN restarted). Report Dropped immediately
			// so Coord can finish cleanup.
			if av.OnReport != nil {
				av.OnReport(av.View)
			}
		default:
			// View unknown to this node (e.g., state lost after restart).
			// Report Unrecoverable so Coord can generate a replacement view.
			if av.OnReport != nil {
				pb := av.View.IntoProto()
				pb.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateUnrecoverable)
				av.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(pb))
			}
		}
		return
	}

	// Existing view: replace callback and deliver coord push.
	entry.ApplyView = *av
	before := entry.sm.State()
	entry.sm.OnCoordStateDelivered(pushedState)
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeApplyCoordViewEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			View: key,
			From: before,
			To:   entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(key.QueryViewVersion, entry)
}

func (s *snShardView) setCollectionIDLocked(collectionID int64) {
	if collectionID == 0 {
		return
	}
	if s.hasCollectionID {
		return
	}
	s.collectionID = collectionID
	s.hasCollectionID = true
}

// notifyReady is called by ResourceManager callback when resource preparation
// completes. Drives the SM from Preparing → Ready.
func (s *snShardView) notifyReady(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	key := entry.View.QueryViewKey()
	before := entry.sm.State()
	entry.sm.OnReady()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeResourceReadyEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			View: key,
			From: before,
			To:   entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(version, entry)
}

// notifyRecoveringDone is called by ResourceManager callback when WAL catch-up
// completes. Drives the SM from UpRecovering → Up.
func (s *snShardView) notifyRecoveringDone(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	key := entry.View.QueryViewKey()
	before := entry.sm.State()
	entry.sm.OnRecoveringDone()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeRecoveringDoneEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			View: key,
			From: before,
			To:   entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(version, entry)
}

// consumeReport drains pending report and invokes callback.
// Caller must hold s.mu.
func (s *snShardView) consumeReport(entry *snViewEntry) {
	report := entry.sm.ConsumeReport()
	if report != nil && entry.OnReport != nil {
		qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeReportViewEvent{
			View:  entry.View.QueryViewKey(),
			State: qviews.QueryViewState(report.GetMeta().GetState()),
		})
		entry.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(report))
	}
}

// notifyDropped is called by ResourceManager callback when resource release
// completes. Drives the SM from Dropping → Dropped.
func (s *snShardView) notifyDropped(version qviews.QueryViewVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[version]
	if !exists {
		return
	}

	key := entry.View.QueryViewKey()
	before := entry.sm.State()
	entry.sm.OnDropped()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeReleaseDoneEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			View: key,
			From: before,
			To:   entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(version, entry)
}

// consumeReportPersistAndCleanup drains pending persist, report, and release,
// invokes callbacks, and removes the entry if it has reached Dropped state.
// Persist is done BEFORE report: if SN crashes after reporting but before
// persisting, Coord would believe the state advanced while SN lost it.
// Caller must hold s.mu.
func (s *snShardView) consumeReportPersistAndCleanup(version qviews.QueryViewVersion, entry *snViewEntry) {
	s.consumeAndPersist(entry)
	s.consumeReport(entry)
	s.consumeAndRelease(version, entry)
	s.cleanupIfDropped(version, entry)
}

// cleanupIfDropped removes the entry if it has reached Dropped state,
// and fires the onEmpty callback if the shard has no more entries.
// Caller must hold s.mu.
func (s *snShardView) cleanupIfDropped(version qviews.QueryViewVersion, entry *snViewEntry) {
	if entry.sm.State() != qviews.QueryViewStateDropped {
		return
	}
	delete(s.views, version)
	if len(s.views) == 0 && s.onEmpty != nil {
		s.onEmpty()
	}
}

// consumeAndPersist drains pending persist and writes to catalog.
// The catalog handles save vs delete based on the view's state.
// Caller must hold s.mu.
func (s *snShardView) consumeAndPersist(entry *snViewEntry) {
	persist := entry.sm.ConsumePersist()
	if persist == nil {
		return
	}
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodePersistViewEvent{
		View:  entry.View.QueryViewKey(),
		State: qviews.QueryViewState(persist.GetMeta().GetState()),
	})
	if err := s.catalog.SaveQueryViews(context.Background(), s.pchannel, []*viewpb.QueryViewOfShard{persist}); err != nil {
		panic(fmt.Sprintf("persist query view %s failed: %v", persist.GetMeta().GetVchannel(), err))
	}
}

// consumeAndRelease drains pending release and calls ResourceManager.Release.
// Caller must hold s.mu.
func (s *snShardView) consumeAndRelease(version qviews.QueryViewVersion, entry *snViewEntry) {
	if !entry.sm.ConsumeRelease() {
		return
	}
	key := entry.View.QueryViewKey()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeReleaseResourceEvent{
		View: key,
	})
	s.resMgr.Release(ReleaseResource{
		Key: key,
		OnDropped: func() {
			s.notifyDropped(version)
		},
	})
}
