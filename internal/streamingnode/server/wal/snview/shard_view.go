package snview

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/internal/views/viewerror"
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
	closed          bool
	pchannel        string
	shardID         qviews.ShardID
	collectionID    int64
	hasCollectionID bool
	views           map[qviews.QueryViewKey]*snViewEntry
	catalog         metastore.StreamingNodeCataLog
	resMgr          StreamingNodeResourceManager
	onEmpty         func() // called (under mu) when the last view entry is removed
}

// snViewEntry pairs an ApplyView (carrying the OnReport callback) with its state machine.
type snViewEntry struct {
	handler.ApplyView
	sm             *snQueryViewStateMachine
	recovered      bool
	queryRefs      int
	releasePending bool
}

// recoverSnShardView constructs an snShardView from pre-built recovered state machines
// and starts recovery for each view via ResourceManager (under shard lock).
// Called during handler construction.
func recoverSnShardView(
	pchannel string,
	shardID qviews.ShardID,
	views map[qviews.QueryViewKey]*snQueryViewStateMachine,
	catalog metastore.StreamingNodeCataLog,
	resMgr StreamingNodeResourceManager,
	onRecoveringDone func(),
) *snShardView {
	entries := make(map[qviews.QueryViewKey]*snViewEntry, len(views))
	for key, sm := range views {
		// Populate ApplyView.View from SM's full shard view so query planning
		// after recovery still sees QueryNode topology.
		view := qviews.NewQueryViewAtWorkNodeFromProto(&viewpb.QueryViewOfShard{
			Meta:          sm.Meta(),
			QueryNode:     sm.QueryNodes(),
			StreamingNode: sm.SNView(),
		})
		entries[key] = &snViewEntry{
			ApplyView: handler.ApplyView{View: view},
			sm:        sm,
			recovered: true,
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
	keys := make([]qviews.QueryViewKey, 0, len(views))
	for key := range views {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].QueryViewVersion.EQ(keys[j].QueryViewVersion) {
			return keys[i].WALReplicaID < keys[j].WALReplicaID
		}
		return keys[j].QueryViewVersion.GT(keys[i].QueryViewVersion)
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		k := key // capture loop variable
		qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeRecoverAcquireResourceEvent{
			View: k,
		})
		resMgr.Acquire(AcquireResource{
			Key:  k,
			Meta: views[key].Meta(),
			OnReady: func() {
				defer onRecoveringDone()
				s.notifyRecoveringDone(k)
			},
			OnUnrecoverable: func() {
				s.notifyUnrecoverable(k)
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
	if s.closed {
		return
	}

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
	s.closed = true
	releases := make([]qviews.QueryViewKey, 0, len(s.views))
	for mapKey, entry := range s.views {
		key := entry.View.QueryViewKey()
		if key.QueryViewVersion == (qviews.QueryViewVersion{}) {
			key = qviews.QueryViewKey{ShardID: s.shardID, WALReplicaID: key.WALReplicaID, QueryViewVersion: mapKey.QueryViewVersion}
		}
		releases = append(releases, key)
	}
	s.views = make(map[qviews.QueryViewKey]*snViewEntry)
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

func (s *snShardView) acquireLatestUpView(ctx context.Context) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var selected *snViewEntry
	var selectedKey qviews.QueryViewKey
	for key, entry := range s.views {
		if entry.sm.State() != qviews.QueryViewStateUp {
			continue
		}
		if selected == nil || key.QueryViewVersion.GT(selectedKey.QueryViewVersion) {
			selected = entry
			selectedKey = key
		}
	}
	if selected == nil {
		return nil, viewerror.NewViewNotFound("latest up query view %s is not found", s.shardID.String())
	}
	selected.queryRefs++
	view := proto.Clone(selected.View.IntoProto()).(*viewpb.QueryViewOfShard)
	var once sync.Once
	return &QueryViewLease{
		Version: selectedKey.QueryViewVersion,
		Meta:    proto.Clone(view.GetMeta()).(*viewpb.QueryViewMeta),
		View:    view,
		Release: func() { once.Do(func() { s.releaseQueryViewLease(selectedKey) }) },
	}, nil
}

func (s *snShardView) releaseQueryViewLease(key qviews.QueryViewKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[key]
	if !exists || entry.queryRefs == 0 {
		return
	}
	entry.queryRefs--
	if entry.queryRefs == 0 && entry.releasePending {
		s.releaseQueryResourceLocked(key, entry)
	}
}

// applyOneLocked applies a single view. Caller must hold s.mu.
func (s *snShardView) applyOneLocked(av *handler.ApplyView) {
	key := av.View.QueryViewKey()
	s.setCollectionIDLocked(av.View.IntoProto().GetMeta().GetCollectionId())
	entry, exists := s.views[key]
	pushedState := av.View.State()

	if !exists {
		switch pushedState {
		case qviews.QueryViewStatePreparing:
			// New Preparing view: create SM and acquire resources.
			snView := av.View.(*qviews.QueryViewAtStreamingNode)
			pb := snView.IntoProto()
			sm := newSNQueryViewStateMachine(
				pb.Meta,
				pb.StreamingNode,
				pb.QueryNode,
			)
			entry = &snViewEntry{ApplyView: *av, sm: sm}
			s.views[key] = entry
			qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeAcquireResourceEvent{
				View: key,
			})
			// SN SM constructor generates a Preparing report.
			s.consumeReport(entry)

			// Tell ResourceManager to prepare resources. Callbacks will drive SM progress.
			k := key
			s.resMgr.Acquire(AcquireResource{
				Key:  key,
				Meta: sm.Meta(),
				OnReady: func() {
					s.notifyReady(k)
				},
				OnUnrecoverable: func() {
					s.notifyUnrecoverable(k)
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

	if pushedState == qviews.QueryViewStateUp {
		s.retireSupersededRecoveredViewsLocked(key.QueryViewVersion)
	}

	// Existing view: replace callback and deliver coord push.
	entry.ApplyView = *av
	entry.sm.UpdateView(av.View.IntoProto())
	before := entry.sm.State()
	entry.sm.OnCoordStateDelivered(pushedState)
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeApplyCoordViewEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: collectionIDForEntry(entry),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(key, entry)
}

// retireSupersededRecoveredViewsLocked removes startup-only views that are no
// longer known by Coord. A higher Up view is sufficient proof that these older
// recovered views must not keep the shared query runtime at an obsolete
// DataVersion. Normal handoff views are not marked recovered and remain under
// Coord's lease-driven lifecycle.
func (s *snShardView) retireSupersededRecoveredViewsLocked(upVersion qviews.QueryViewVersion) {
	for key, entry := range s.views {
		if !entry.recovered || !upVersion.GT(key.QueryViewVersion) {
			continue
		}
		state := entry.sm.State()
		if state != qviews.QueryViewStateUp && state != qviews.QueryViewStateUpRecovering {
			continue
		}
		entry.sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
		qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeApplyCoordViewEvent{
			ViewStateTransition: qvobserve.ViewStateTransition{
				CollectionID: collectionIDForEntry(entry),
				View:         key,
				From:         state,
				To:           entry.sm.State(),
			},
		})
		s.consumeReportPersistAndCleanup(key, entry)
	}
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
func (s *snShardView) notifyReady(key qviews.QueryViewKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[key]
	if !exists {
		return
	}

	before := entry.sm.State()
	entry.sm.OnReady()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeResourceReadyEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: collectionIDForEntry(entry),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(key, entry)
}

// notifyUnrecoverable is called by ResourceManager when the requested
// resources can no longer be reconstructed at the QueryView DataVersion.
func (s *snShardView) notifyUnrecoverable(key qviews.QueryViewKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[key]
	if !exists {
		return
	}
	entry.sm.OnUnrecoverable()
	s.consumeReportPersistAndCleanup(key, entry)
}

// notifyRecoveringDone is called by ResourceManager callback when WAL catch-up
// completes. Drives the SM from UpRecovering → Up.
func (s *snShardView) notifyRecoveringDone(key qviews.QueryViewKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[key]
	if !exists {
		return
	}

	before := entry.sm.State()
	entry.sm.OnRecoveringDone()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeRecoveringDoneEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: collectionIDForEntry(entry),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(key, entry)
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
func (s *snShardView) notifyDropped(key qviews.QueryViewKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[key]
	if !exists {
		return
	}

	before := entry.sm.State()
	entry.sm.OnDropped()
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeReleaseDoneEvent{
		ViewStateTransition: qvobserve.ViewStateTransition{
			CollectionID: collectionIDForEntry(entry),
			View:         key,
			From:         before,
			To:           entry.sm.State(),
		},
	})
	s.consumeReportPersistAndCleanup(key, entry)
}

// consumeReportPersistAndCleanup drains pending persist, report, and release,
// invokes callbacks, and removes the entry if it has reached Dropped state.
// Persist is done BEFORE report: if SN crashes after reporting but before
// persisting, Coord would believe the state advanced while SN lost it.
// Caller must hold s.mu.
func (s *snShardView) consumeReportPersistAndCleanup(key qviews.QueryViewKey, entry *snViewEntry) {
	s.consumeAndPersist(entry)
	s.consumeReport(entry)
	s.consumeAndRelease(key, entry)
	s.cleanupIfDropped(key, entry)
}

// cleanupIfDropped removes the entry if it has reached Dropped state,
// and fires the onEmpty callback if the shard has no more entries.
// Caller must hold s.mu.
func (s *snShardView) cleanupIfDropped(key qviews.QueryViewKey, entry *snViewEntry) {
	if entry.sm.State() != qviews.QueryViewStateDropped {
		return
	}
	delete(s.views, key)
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
func (s *snShardView) consumeAndRelease(key qviews.QueryViewKey, entry *snViewEntry) {
	if !entry.sm.ConsumeRelease() {
		return
	}
	if entry.queryRefs > 0 {
		entry.releasePending = true
		return
	}
	s.releaseQueryResourceLocked(key, entry)
}

func (s *snShardView) releaseQueryResourceLocked(key qviews.QueryViewKey, entry *snViewEntry) {
	entry.releasePending = false
	qvobserve.Observe(context.TODO(), qvobserve.StreamingNodeReleaseResourceEvent{
		View: key,
	})
	s.resMgr.Release(ReleaseResource{
		Key: key,
		OnDropped: func() {
			s.notifyDropped(key)
		},
	})
}

func collectionIDForEntry(entry *snViewEntry) int64 {
	return entry.sm.Meta().GetCollectionId()
}
