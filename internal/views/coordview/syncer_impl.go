package coordview

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/log"
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
	wg     sync.WaitGroup
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
	s.wg.Add(1)
	go s.watchNodes()
	return s
}

func (s *reliableSyncer) SyncViews(ctx context.Context, group SyncGroup) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSyncerClosed
	}
	s.mu.Unlock()

	for nodeKey, views := range group.ViewsByNode {
		// Fast path: ResumableSyncer already exists.
		s.mu.Lock()
		rs, ok := s.resumableSyncers[nodeKey]
		s.mu.Unlock()

		if ok {
			rs.Sync(views)
			continue
		}

		// Slow path: no syncer — try to find the node via service discovery.
		rs = s.tryCreateSyncer(ctx, nodeKey, views)
		if rs != nil {
			rs.Sync(views)
			continue
		}

		// Node not found — drain views immediately.
		drainViews(views)
	}
	return nil
}

// tryCreateSyncer attempts to find the node via service discovery and create
// a ResumableSyncer for it. Returns nil if the node does not exist.
func (s *reliableSyncer) tryCreateSyncer(ctx context.Context, nodeKey qviews.WorkNodeKey, views []SyncView) *resumableSyncer {
	if len(views) == 0 {
		return nil
	}

	node := views[0].View.WorkNode()

	if !s.client.IsNodeAlive(ctx, node) {
		return nil
	}

	// Double-check under write lock (another goroutine may have created it).
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	if rs, ok := s.resumableSyncers[nodeKey]; ok {
		return rs
	}

	log.Info("ReliableSyncer: node discovered on demand, creating ResumableSyncer",
		zap.String("node", nodeKey))
	rs := newResumableSyncer(s.ctx, node, s.client)
	s.resumableSyncers[nodeKey] = rs
	return rs
}

// drainViews immediately invokes OnNodeLost for each view.
func drainViews(views []SyncView) {
	for _, sv := range views {
		sv.OnNodeLost()
	}
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
	s.wg.Wait()

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

// watchNodes watches node membership changes and drains ResumableSyncers for removed nodes.
func (s *reliableSyncer) watchNodes() {
	defer s.wg.Done()

	for s.ctx.Err() == nil {
		ch, err := s.client.WatchNodeChanged(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			log.Warn("ReliableSyncer: WatchNodeChanged failed, retrying", zap.Error(err))
			continue
		}

		// Initial sync.
		s.drainRemovedNodes()

		// Watch for changes.
		for {
			select {
			case <-s.ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					// Channel closed, re-watch.
					break
				}
				s.drainRemovedNodes()
				continue
			}
			break
		}
	}
}

// drainRemovedNodes fetches the current node set and drains ResumableSyncers for removed nodes.
// It does NOT create ResumableSyncers for new nodes — that is done lazily by tryCreateSyncer.
func (s *reliableSyncer) drainRemovedNodes() {
	nodes, err := s.client.GetAllNodes(s.ctx)
	if err != nil {
		if s.ctx.Err() != nil {
			return
		}
		log.Warn("ReliableSyncer: GetAllNodes failed", zap.Error(err))
		return
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	// Find removed nodes — collect ResumableSyncers to close.
	var removed []removedNode
	for nodeKey, rs := range s.resumableSyncers {
		if _, exists := nodes[nodeKey]; !exists {
			removed = append(removed, removedNode{key: nodeKey, syncer: rs})
			delete(s.resumableSyncers, nodeKey)
		}
	}
	s.mu.Unlock()

	// Close removed ResumableSyncers and drain pending views (node lost).
	for _, r := range removed {
		log.Info("ReliableSyncer: node removed, closing ResumableSyncer",
			zap.String("node", r.key))
		r.syncer.Close()
		r.syncer.DrainPendingIfNodeLost()
	}
}

type removedNode struct {
	key    string
	syncer *resumableSyncer
}
