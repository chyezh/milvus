package syncer

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

var (
	_ ReliableSyncer = (*reliableSyncer)(nil)

	// ErrSyncerClosed is returned when SyncViews is called on a closed ReliableSyncer.
	ErrSyncerClosed = errors.New("reliable syncer is closed")
)

type reliableSyncer struct {
	client ViewSyncClient

	mu               sync.Mutex
	resumableSyncers map[qviews.WorkNodeKey]*resumableSyncer
	closed           bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewReliableSyncer creates a new ReliableSyncer.
func NewReliableSyncer(client ViewSyncClient) ReliableSyncer {
	ctx, cancel := context.WithCancel(context.Background())
	s := &reliableSyncer{
		client:           client,
		resumableSyncers: make(map[qviews.WorkNodeKey]*resumableSyncer),
		ctx:              ctx,
		cancel:           cancel,
	}
	client.RegisterNodeChangedNotifier(s.drainRemovedNodes)
	return s
}

func (s *reliableSyncer) SyncViews(ctx context.Context, group SyncGroup) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	for nodeKey, views := range group.ViewsByNode {
		rs, closed := s.getOrCreateSyncer(ctx, nodeKey, views)
		if closed {
			return ErrSyncerClosed
		}
		if rs != nil {
			rs.Sync(views)
			continue
		}
		// Node not found — notify views immediately.
		for _, sv := range views {
			notifyQueryNodeLost(sv)
		}
	}
	return nil
}

func notifyQueryNodeLost(sv SyncView) {
	if sv.OnQueryNodeLost == nil {
		return
	}
	qn, ok := sv.View.WorkNode().(qviews.QueryNode)
	if !ok {
		return
	}
	sv.OnQueryNodeLost(qn)
}

// getOrCreateSyncer returns the existing ResumableSyncer for the node,
// creates one if the node is alive, or returns (nil, false) if the node is not found.
// Returns (nil, true) if the syncer is closed.
func (s *reliableSyncer) getOrCreateSyncer(ctx context.Context, nodeKey qviews.WorkNodeKey, views []SyncView) (rs *resumableSyncer, closed bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, true
	}
	if rs, ok := s.resumableSyncers[nodeKey]; ok {
		s.mu.Unlock()
		return rs, false
	}
	if len(views) == 0 {
		s.mu.Unlock()
		return nil, false
	}
	node := views[0].View.WorkNode()
	s.mu.Unlock()

	if !s.client.IsNodeAlive(ctx, node) {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, true
	}
	if rs, ok := s.resumableSyncers[nodeKey]; ok {
		return rs, false
	}
	mlog.Info(ctx, "ReliableSyncer: node discovered on demand, creating ResumableSyncer",
		mlog.String("node", nodeKey))
	rs = newResumableSyncer(s.ctx, node, s.client)
	s.resumableSyncers[nodeKey] = rs
	return rs, false
}

func (s *reliableSyncer) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	s.cancel()

	// Close all remaining ResumableSyncers (graceful shutdown, no drain).
	s.mu.Lock()
	syncers := s.resumableSyncers
	s.resumableSyncers = nil
	s.mu.Unlock()

	for _, rs := range syncers {
		rs.Close()
	}
	return nil
}

// drainRemovedNodes drains ResumableSyncers whose target nodes are no longer alive.
// It does NOT create ResumableSyncers for new nodes — that is done lazily by tryCreateSyncer.
func (s *reliableSyncer) drainRemovedNodes() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	syncers := make(map[qviews.WorkNodeKey]*resumableSyncer, len(s.resumableSyncers))
	for nodeKey, rs := range s.resumableSyncers {
		syncers[nodeKey] = rs
	}
	s.mu.Unlock()

	// Find removed nodes — collect ResumableSyncers to close.
	var removed []removedNode
	for nodeKey, rs := range syncers {
		if !s.client.IsNodeAlive(s.ctx, rs.node) {
			removed = append(removed, removedNode{key: nodeKey, syncer: rs})
		}
	}
	if len(removed) == 0 {
		return
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	kept := removed[:0]
	for _, r := range removed {
		if s.resumableSyncers[r.key] == r.syncer {
			delete(s.resumableSyncers, r.key)
			kept = append(kept, r)
		}
	}
	removed = kept
	s.mu.Unlock()

	// Close removed ResumableSyncers and drain pending views (node lost).
	for _, r := range removed {
		mlog.Info(s.ctx, "ReliableSyncer: node removed, closing ResumableSyncer",
			mlog.String("node", r.key))
		r.syncer.Close()
		r.syncer.DrainPendingIfNodeLost()
	}
}

type removedNode struct {
	key    string
	syncer *resumableSyncer
}
