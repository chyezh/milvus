package coordview

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

var (
	_ ReliableSyncer = (*reliableSyncer)(nil)

	// ErrSyncerClosed is returned when SyncViews is called on a closed ReliableSyncer.
	ErrSyncerClosed = errors.New("reliable syncer is closed")
)

// ReliableSyncerConfig holds configuration for the ReliableSyncer.
type ReliableSyncerConfig struct {
	// SendBufferSize is the capacity of the per-node send channel.
	SendBufferSize int
}

type reliableSyncer struct {
	pending  *onDispatchingQueryView
	snClient NodeClient
	qnClient NodeClient
	config   ReliableSyncerConfig

	mu               sync.RWMutex
	resumableSyncers map[qviews.WorkNodeKey]*resumableSyncer
	closed           bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewReliableSyncer creates a new ReliableSyncer.
// snClient: NodeClient for StreamingNode service discovery and stream creation.
// qnClient: NodeClient for QueryNode service discovery and stream creation.
func NewReliableSyncer(snClient NodeClient, qnClient NodeClient, config ReliableSyncerConfig) ReliableSyncer {
	ctx, cancel := context.WithCancel(context.Background())
	s := &reliableSyncer{
		pending:          newOnDispatchingQueryView(),
		snClient:         snClient,
		qnClient:         qnClient,
		config:           config,
		resumableSyncers: make(map[qviews.WorkNodeKey]*resumableSyncer),
		ctx:              ctx,
		cancel:           cancel,
	}
	s.wg.Add(2)
	go s.watchNodes(snClient, "SN")
	go s.watchNodes(qnClient, "QN")
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

	// Group views by target node and enqueue.
	grouped := make(map[qviews.WorkNodeKey][]*SyncView)
	for i := range group.Views {
		sv := &group.Views[i]
		nodeKey := sv.View.WorkNode().Key()
		grouped[nodeKey] = append(grouped[nodeKey], sv)
	}

	for nodeKey, views := range grouped {
		// Upsert into pending and collect protos.
		protos := make([]*viewpb.QueryViewOfShard, 0, len(views))
		for _, sv := range views {
			proto := s.pending.Upsert(*sv)
			protos = append(protos, proto)
		}

		// Find ResumableSyncer for this node and enqueue.
		s.mu.RLock()
		rs, ok := s.resumableSyncers[nodeKey]
		s.mu.RUnlock()

		if !ok {
			// No ResumableSyncer for this node — node not yet discovered or already lost.
			// The pending entries are tracked; if the node appears later,
			// re-push will deliver them. If it never appears, the caller should
			// handle this via other mechanisms (e.g., reassignment).
			log.Warn("ReliableSyncer: no ResumableSyncer for node, views tracked but not sent",
				zap.String("node", nodeKey))
			continue
		}

		rs.Enqueue(protos)
	}
	return nil
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

	// Close all remaining ResumableSyncers.
	s.mu.Lock()
	syncers := make(map[string]*resumableSyncer, len(s.resumableSyncers))
	for k, v := range s.resumableSyncers {
		syncers[k] = v
	}
	s.resumableSyncers = nil
	s.mu.Unlock()

	for _, rs := range syncers {
		rs.Close()
	}
	return nil
}

// watchNodes watches node changes from a NodeClient and manages ResumableSyncers.
func (s *reliableSyncer) watchNodes(client NodeClient, label string) {
	defer s.wg.Done()

	for s.ctx.Err() == nil {
		ch, err := client.WatchNodeChanged(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			log.Warn("ReliableSyncer: WatchNodeChanged failed, retrying",
				zap.String("label", label), zap.Error(err))
			continue
		}

		// Initial sync.
		s.reconcileNodes(client, label)

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
				s.reconcileNodes(client, label)
				continue
			}
			break
		}
	}
}

// reconcileNodes fetches the current node set and creates/destroys ResumableSyncers accordingly.
func (s *reliableSyncer) reconcileNodes(client NodeClient, label string) {
	nodes, err := client.GetAllNodes(s.ctx)
	if err != nil {
		if s.ctx.Err() != nil {
			return
		}
		log.Warn("ReliableSyncer: GetAllNodes failed",
			zap.String("label", label), zap.Error(err))
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
				zap.String("label", label), zap.String("node", nodeKey))
			s.resumableSyncers[nodeKey] = newResumableSyncer(
				s.ctx, node, client, s.pending, s.config.SendBufferSize,
			)
		}
	}

	// Find removed nodes — collect ResumableSyncers to close.
	var removed []removedNode
	for nodeKey, rs := range s.resumableSyncers {
		if _, exists := nodes[nodeKey]; !exists {
			// Only remove if this syncer was created by this client type.
			// Check by seeing if the node key matches the label pattern.
			if isNodeForClient(nodeKey, label) {
				removed = append(removed, removedNode{key: nodeKey, syncer: rs})
				delete(s.resumableSyncers, nodeKey)
			}
		}
	}
	s.mu.Unlock()

	// Close removed ResumableSyncers and drain pending entries outside the lock.
	for _, r := range removed {
		log.Info("ReliableSyncer: node removed, closing ResumableSyncer",
			zap.String("label", label), zap.String("node", r.key))
		r.syncer.Close()
		s.pending.DrainByNode(r.syncer.node)
	}
}

type removedNode struct {
	key    string
	syncer *resumableSyncer
}

// isNodeForClient checks if a node key belongs to the given client type.
// SN node keys start with "sn@", QN node keys start with "qn@".
func isNodeForClient(nodeKey string, label string) bool {
	switch label {
	case "SN":
		return len(nodeKey) > 3 && nodeKey[:3] == "sn@"
	case "QN":
		return len(nodeKey) > 3 && nodeKey[:3] == "qn@"
	default:
		return false
	}
}
