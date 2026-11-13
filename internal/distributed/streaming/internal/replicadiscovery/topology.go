package replicadiscovery

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// ReplicaStreamFactory resolves the member's current local runtime, then opens
// a watch bound to that exact identity. It must not fall back to the writer.
type ReplicaStreamFactory func(context.Context, string, *streamingpb.WALReplicaAssignment) (Stream, qviews.StreamingNode, error)

// NewTopologyClient shares one topology stream per PChannel and one resumable
// physical watch per member. A failed member invalidates only its own candidates.
func NewTopologyClient(watcher types.AssignmentDiscoverWatcher, factory ReplicaStreamFactory) *Client {
	return NewResumingClient(func(ctx context.Context, pc string) (Stream, error) {
		return newTopologyStream(ctx, pc, watcher, factory), nil
	})
}

type (
	memberKey struct {
		replicaID uint64
		nodeID    int64
	}
	topologyMember struct {
		key    memberKey
		stream *resumableStream
	}
	topologyUpdate struct{ plan *streamingpb.WALReplicaPlan }
)

type topologyStream struct {
	ctx           context.Context
	cancel        context.CancelFunc
	pchannel      string
	factory       ReplicaStreamFactory
	latest        atomic.Pointer[topologyUpdate]
	planWake      chan struct{}
	dispatchWake  chan struct{}
	done          chan struct{}
	watchDone     chan struct{}
	mu            sync.Mutex
	known         bool
	members       map[memberKey]*topologyMember
	subscriptions map[*topologySubscription]struct{}
	pending       map[*topologySubscription]struct{}
}

type topologySubscription struct {
	owner     *topologyStream
	vchannel  string
	handler   Handler
	closed    bool
	children  map[*topologyMember]*Subscription
	snapshots map[*topologyMember]Snapshot
}

func newTopologyStream(parent context.Context, pc string, watcher types.AssignmentDiscoverWatcher, factory ReplicaStreamFactory) *topologyStream {
	ctx, cancel := context.WithCancel(parent)
	s := &topologyStream{ctx: ctx, cancel: cancel, pchannel: pc, factory: factory, planWake: make(chan struct{}, 1), dispatchWake: make(chan struct{}, 1), done: make(chan struct{}), watchDone: make(chan struct{}), members: make(map[memberKey]*topologyMember), subscriptions: make(map[*topologySubscription]struct{}), pending: make(map[*topologySubscription]struct{})}
	go func() {
		defer close(s.watchDone)
		err := watcher.AssignmentDiscover(ctx, func(assignment *types.VersionedStreamingNodeAssignments) error {
			if assignment.Version.Global < 0 {
				return nil
			}
			s.latest.Store(&topologyUpdate{plan: proto.CloneOf(assignment.WALReplicaPlans[pc])})
			select {
			case s.planWake <- struct{}{}:
			default:
			}
			return nil
		})
		if err != nil && ctx.Err() == nil {
			// Losing control-plane discovery does not invalidate healthy local
			// View streams. Each member still enforces its serving lifetime.
			mlog.Warn(ctx, "WAL replica topology watch stopped", mlog.FieldPChannel(pc), mlog.Err(err))
		}
	}()
	go s.run()
	return s
}

func (s *topologyStream) Subscribe(ctx context.Context, vc string, handler Handler) (wal.ReplicaWatchSubscription, error) {
	if handler == nil || vc == "" || funcutil.ToPhysicalChannel(vc) != s.pchannel {
		return nil, status.NewInvalidRequestSeq("replica subscription requires a VChannel in this PChannel")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	sub := &topologySubscription{owner: s, vchannel: vc, handler: handler, children: make(map[*topologyMember]*Subscription), snapshots: make(map[*topologyMember]Snapshot)}
	s.subscriptions[sub] = struct{}{}
	for _, member := range s.members {
		sub.attachLocked(member)
	}
	s.enqueueLocked(sub)
	return sub, nil
}

func (s *topologySubscription) attachLocked(member *topologyMember) {
	child, err := member.stream.subscribe(s.vchannel, func(snapshot Snapshot) {
		owner := s.owner
		owner.mu.Lock()
		defer owner.mu.Unlock()
		if s.closed || owner.members[member.key] != member {
			return
		}
		s.snapshots[member] = snapshot
		owner.enqueueLocked(s)
	})
	if err == nil {
		s.children[member] = child
	}
}

func (s *topologySubscription) snapshotLocked() Snapshot {
	owner := s.owner
	result := Snapshot{Valid: owner.known && !s.closed && owner.ctx.Err() == nil}
	if !result.Valid {
		return result
	}
	allValid := true
	for _, member := range owner.members {
		snapshot := s.snapshots[member]
		allValid = allValid && snapshot.Valid
		if snapshot.Valid {
			result.Replicas = append(result.Replicas, snapshot.Replicas...)
		}
	}
	// A ready member is immediately usable. Empty is authoritative only when
	// every desired member has supplied a valid local empty directory.
	result.Valid = allValid || len(result.Replicas) > 0
	slices.SortFunc(result.Replicas, func(a, b Replica) int {
		if n := cmp.Compare(a.StreamingNode.NodeID, b.StreamingNode.NodeID); n != 0 {
			return n
		}
		if n := cmp.Compare(a.StreamingNode.WALReplicaID, b.StreamingNode.WALReplicaID); n != 0 {
			return n
		}
		return cmp.Compare(a.ReplicaID, b.ReplicaID)
	})
	return result
}

func (s *topologySubscription) Snapshot() Snapshot {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	return s.snapshotLocked()
}

func (s *topologySubscription) Close() error {
	owner := s.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.handler = nil
	delete(owner.subscriptions, s)
	delete(owner.pending, s)
	for _, child := range s.children {
		_ = child.Close()
	}
	s.children = nil
	s.snapshots = nil
	return nil
}

func (s *topologyStream) enqueueLocked(sub *topologySubscription) {
	s.pending[sub] = struct{}{}
	select {
	case s.dispatchWake <- struct{}{}:
	default:
	}
}

func (s *topologyStream) applyPlan(plan *streamingpb.WALReplicaPlan) {
	targets := make(map[memberKey]*streamingpb.WALReplicaAssignment)
	for _, member := range plan.GetReplicas() {
		if member.GetReplicaId() > 0 && member.GetNode().GetServerId() > 0 {
			targets[memberKey{member.ReplicaId, member.Node.ServerId}] = member
		}
	}
	s.mu.Lock()
	s.known = true
	var retired []*topologyMember
	for key, member := range s.members {
		if _, ok := targets[key]; ok {
			continue
		}
		delete(s.members, key)
		retired = append(retired, member)
		for sub := range s.subscriptions {
			if child := sub.children[member]; child != nil {
				_ = child.Close()
			}
			delete(sub.children, member)
			delete(sub.snapshots, member)
		}
	}
	for key, target := range targets {
		if s.members[key] != nil {
			continue
		}
		member := &topologyMember{key: key}
		member.stream = newResumableStream(s.ctx, s.pchannel, func(ctx context.Context) (Stream, error) {
			stream, identity, err := s.factory(ctx, s.pchannel, target)
			if err != nil {
				return nil, err
			}
			return &runtimeStream{Stream: stream, identity: identity}, nil
		})
		s.members[key] = member
		for sub := range s.subscriptions {
			sub.attachLocked(member)
		}
	}
	for sub := range s.subscriptions {
		s.enqueueLocked(sub)
	}
	s.mu.Unlock()
	// Child callbacks can be entering the topology lock. Join only after
	// removing their membership and releasing that lock.
	for _, member := range retired {
		member.stream.Close()
	}
}

func (s *topologyStream) dispatch() {
	for s.ctx.Err() == nil {
		s.mu.Lock()
		var handler Handler
		var snapshot Snapshot
		for sub := range s.pending {
			delete(s.pending, sub)
			handler = sub.handler
			snapshot = sub.snapshotLocked()
			break
		}
		s.mu.Unlock()
		if handler == nil {
			return
		}
		handler(snapshot)
	}
}

func (s *topologyStream) run() {
	defer close(s.done)
	defer func() {
		s.mu.Lock()
		members := s.members
		s.members = nil
		s.mu.Unlock()
		for _, member := range members {
			member.stream.Close()
		}
	}()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.planWake:
			s.applyPlan(s.latest.Load().plan)
		case <-s.dispatchWake:
			s.dispatch()
		}
	}
}
func (s *topologyStream) Done() <-chan struct{} { return s.done }
func (s *topologyStream) Error() error          { return s.ctx.Err() }
func (s *topologyStream) Close() error          { s.cancel(); <-s.done; <-s.watchDone; return nil }

// runtimeStream stamps local directory candidates with the exact identity
// resolved for this physical attempt. A reconnect can acquire a new generation.
type (
	runtimeStream struct {
		Stream
		identity qviews.StreamingNode
	}
	runtimeSubscription struct {
		wal.ReplicaWatchSubscription
		identity qviews.StreamingNode
	}
)

func bindRuntime(snapshot Snapshot, identity qviews.StreamingNode) Snapshot {
	snapshot.Replicas = slices.Clone(snapshot.Replicas)
	for i := range snapshot.Replicas {
		snapshot.Replicas[i].StreamingNode = identity
	}
	return snapshot
}

func (s *runtimeStream) Subscribe(ctx context.Context, vc string, handler Handler) (wal.ReplicaWatchSubscription, error) {
	sub, err := s.Stream.Subscribe(ctx, vc, func(snapshot Snapshot) { handler(bindRuntime(snapshot, s.identity)) })
	if err != nil {
		return nil, err
	}
	return &runtimeSubscription{ReplicaWatchSubscription: sub, identity: s.identity}, nil
}

func (s *runtimeSubscription) Snapshot() Snapshot {
	return bindRuntime(s.ReplicaWatchSubscription.Snapshot(), s.identity)
}
