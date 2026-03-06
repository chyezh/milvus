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
	snClient ViewSyncClient
	qnClient ViewSyncClient

	mu               sync.RWMutex
	resumableSyncers map[qviews.WorkNodeKey]*resumableSyncer
	closed           bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewReliableSyncer creates a new ReliableSyncer.
// snClient: ViewSyncClient for StreamingNode service discovery and stream creation.
// qnClient: ViewSyncClient for QueryNode service discovery and stream creation.
func NewReliableSyncer(snClient ViewSyncClient, qnClient ViewSyncClient) ReliableSyncer {
	ctx, cancel := context.WithCancel(context.Background())
	s := &reliableSyncer{
		snClient:         snClient,
		qnClient:         qnClient,
		resumableSyncers: make(map[qviews.WorkNodeKey]*resumableSyncer),
		ctx:              ctx,
		cancel:           cancel,
	}
	s.wg.Add(2)
	go s.watchNodes(snClient, qviews.NodeTypeStreamingNode)
	go s.watchNodes(qnClient, qviews.NodeTypeQueryNode)
	return s
}

func (s *reliableSyncer) SyncViews(ctx context.Context, group SyncGroup) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrSyncerClosed
	}
	s.mu.RUnlock()

	for nodeKey, views := range group.ViewsByNode {
		// Fast path: ResumableSyncer already exists.
		s.mu.RLock()
		rs, ok := s.resumableSyncers[nodeKey]
		s.mu.RUnlock()

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

	nodeType := views[0].View.WorkNode().NodeType()
	client := s.clientForNodeType(nodeType)

	nodes, err := client.GetAllNodes(ctx)
	if err != nil {
		log.Warn("ReliableSyncer: GetAllNodes failed during on-demand sync",
			zap.String("node", nodeKey), zap.Error(err))
		return nil
	}

	node, exists := nodes[nodeKey]
	if !exists {
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
	rs := newResumableSyncer(s.ctx, node, client)
	s.resumableSyncers[nodeKey] = rs
	return rs
}

func (s *reliableSyncer) clientForNodeType(nodeType qviews.NodeType) ViewSyncClient {
	switch nodeType {
	case qviews.NodeTypeStreamingNode:
		return s.snClient
	case qviews.NodeTypeQueryNode:
		return s.qnClient
	default:
		panic("unknown node type")
	}
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

	// Close all remaining ResumableSyncers (each drains its own pending views).
	s.mu.Lock()
	syncers := s.resumableSyncers
	s.resumableSyncers = nil
	s.mu.Unlock()

	for _, rs := range syncers {
		rs.Close()
	}
	return nil
}

// watchNodes watches node changes from a ViewSyncClient and manages ResumableSyncers.
func (s *reliableSyncer) watchNodes(client ViewSyncClient, nodeType qviews.NodeType) {
	defer s.wg.Done()

	for s.ctx.Err() == nil {
		ch, err := client.WatchNodeChanged(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			log.Warn("ReliableSyncer: WatchNodeChanged failed, retrying",
				zap.Int("nodeType", int(nodeType)), zap.Error(err))
			continue
		}

		// Initial sync.
		s.reconcileNodes(client, nodeType)

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
				s.reconcileNodes(client, nodeType)
				continue
			}
			break
		}
	}
}

// reconcileNodes fetches the current node set and creates/destroys ResumableSyncers accordingly.
func (s *reliableSyncer) reconcileNodes(client ViewSyncClient, nodeType qviews.NodeType) {
	nodes, err := client.GetAllNodes(s.ctx)
	if err != nil {
		if s.ctx.Err() != nil {
			return
		}
		log.Warn("ReliableSyncer: GetAllNodes failed",
			zap.Int("nodeType", int(nodeType)), zap.Error(err))
		return
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	// Find new nodes — create ResumableSyncers.
	for nodeKey, node := range nodes {
		if _, exists := s.resumableSyncers[nodeKey]; !exists {
			log.Info("ReliableSyncer: node discovered, creating ResumableSyncer",
				zap.String("node", nodeKey))
			s.resumableSyncers[nodeKey] = newResumableSyncer(s.ctx, node, client)
		}
	}

	// Find removed nodes — collect ResumableSyncers to close.
	var removed []removedNode
	for nodeKey, rs := range s.resumableSyncers {
		if rs.node.NodeType() != nodeType {
			continue
		}
		if _, exists := nodes[nodeKey]; !exists {
			removed = append(removed, removedNode{key: nodeKey, syncer: rs})
			delete(s.resumableSyncers, nodeKey)
		}
	}
	s.mu.Unlock()

	// Close removed ResumableSyncers (each drains its own pending views).
	for _, r := range removed {
		log.Info("ReliableSyncer: node removed, closing ResumableSyncer",
			zap.String("node", r.key))
		r.syncer.Close()
	}
}

type removedNode struct {
	key    string
	syncer *resumableSyncer
}
