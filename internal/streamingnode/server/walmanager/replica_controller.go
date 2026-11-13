package walmanager

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// replicaController admits complete plans independently of runtime preparation.
// One worker owns open/update/close, while read access keeps the current runtime
// alive during an upstream change. WALTerm fences a writer; ReplicaRevision
// orders replacement of a local instance, including replacement within a term.
type replicaController struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	wake     chan struct{}
	opener   wal.Opener
	nodeID   int64
	pchannel string

	mu               sync.Mutex
	plan             *streamingpb.WALReplicaPlan
	operationCancel  context.CancelFunc
	current          wal.WAL
	assignment       *streamingpb.WALReplicaAssignment
	sourceTerm       int64
	sourceNode       types.StreamingNodeInfo
	appliedRevision  uint64
	failedWriterTerm int64
	lastErr          error
}

func newReplicaController(ctx context.Context, nodeID int64, pchannel string, opener wal.Opener) *replicaController {
	ctx, cancel := context.WithCancel(ctx)
	c := &replicaController{ctx: ctx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1), opener: opener, nodeID: nodeID, pchannel: pchannel}
	go c.run()
	return c
}

func validateReplicaPlan(plan *streamingpb.WALReplicaPlan) error {
	if plan.GetRevision() == 0 || plan.GetWalTerm() <= 0 {
		return merr.WrapErrServiceInternalMsg("WAL replica plan has no revision or writer term")
	}
	ids := make(map[uint64]struct{})
	nodes := make(map[int64]struct{})
	primaries := 0
	for _, member := range plan.Replicas {
		if member.GetReplicaId() == 0 || member.GetReplicaRevision() == 0 || member.GetResourceGroup() == "" {
			return merr.WrapErrServiceInternalMsg("WAL replica plan has an incomplete member identity")
		}
		if _, exists := ids[member.ReplicaId]; exists {
			return merr.WrapErrServiceInternalMsg("WAL replica plan has duplicate member identities")
		}
		ids[member.ReplicaId] = struct{}{}
		if member.Node != nil {
			if _, exists := nodes[member.Node.ServerId]; exists || member.Node.ServerId <= 0 || member.Node.Address == "" {
				return merr.WrapErrServiceInternalMsg("WAL replica plan has an invalid node placement")
			}
			nodes[member.Node.ServerId] = struct{}{}
		}
		switch member.AccessMode {
		case streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE:
			if member.Node == nil {
				return merr.WrapErrServiceUnavailable("WAL replica plan has no primary endpoint")
			}
			primaries++
		case streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY:
		default:
			return merr.WrapErrServiceInternalMsg("WAL replica plan has an unknown access mode")
		}
	}
	if primaries != 1 {
		return merr.WrapErrServiceInternalMsg("WAL replica plan must contain exactly one writer")
	}
	return nil
}

func (c *replicaController) reconcile(plan *streamingpb.WALReplicaPlan) (*streamingpb.ReconcileWALReplicasResponse, error) {
	if err := validateReplicaPlan(plan); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	if plan.Revision == c.plan.GetRevision() && !proto.Equal(plan, c.plan) {
		return nil, merr.WrapErrServiceInternalMsg("conflicting WAL replica plans at the same revision")
	}
	if plan.Revision > c.plan.GetRevision() {
		if plan.WalTerm < c.plan.GetWalTerm() {
			return nil, merr.WrapErrServiceInternalMsg("WAL replica plan regresses the writer term")
		}
		changed := !c.sameLocalTarget(c.plan, plan)
		c.plan = proto.CloneOf(plan)
		if changed && c.operationCancel != nil {
			c.operationCancel()
		}
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
	return c.observationLocked(), nil
}

func (c *replicaController) observationLocked() *streamingpb.ReconcileWALReplicasResponse {
	response := &streamingpb.ReconcileWALReplicasResponse{PlanRevision: c.plan.GetRevision()}
	for _, member := range c.plan.GetReplicas() {
		if member.GetNode().GetServerId() != c.nodeID {
			continue
		}
		state := streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING
		if (c.appliedRevision == c.plan.Revision && c.lastErr != nil) || (c.current != nil && !c.current.IsAvailable()) {
			state = streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED
		} else if c.current != nil && c.current.IsAvailable() && c.appliedRevision == c.plan.Revision {
			state = streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY
		}
		response.Replicas = append(response.Replicas, c.observation(member, state))
	}
	for _, retired := range c.plan.GetRetiringReplicas() {
		if retired.GetNode().GetServerId() != c.nodeID {
			continue
		}
		state := streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED
		if c.assignment != nil && c.assignment.ReplicaId == retired.ReplicaId && c.assignment.ReplicaRevision == retired.ReplicaRevision {
			state = streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING
		}
		response.Replicas = append(response.Replicas, c.observation(retired, state))
	}
	return response
}

func (c *replicaController) observation(member *streamingpb.WALReplicaAssignment, state streamingpb.WALReplicaState) *streamingpb.WALReplicaObservation {
	return &streamingpb.WALReplicaObservation{ReplicaId: member.ReplicaId, ReplicaRevision: member.ReplicaRevision, NodeId: c.nodeID, State: state, SourceTerm: c.sourceTerm}
}

func (c *replicaController) getWAL() wal.WAL {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *replicaController) close() { c.cancel(); <-c.done }

func (c *replicaController) run() {
	defer close(c.done)
	defer c.closeCurrent()
	retry := time.NewTicker(time.Second)
	defer retry.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.wake:
		case <-retry.C:
		}
		c.mu.Lock()
		plan := c.plan
		if plan == nil || (c.appliedRevision == plan.Revision && c.lastErr == nil && (c.current == nil || c.current.IsAvailable())) {
			c.mu.Unlock()
			continue
		}
		ctx, cancel := context.WithCancel(c.ctx)
		c.operationCancel = cancel
		c.mu.Unlock()
		err := c.apply(ctx, plan)
		cancel()
		c.mu.Lock()
		if c.plan.Revision == plan.Revision {
			c.appliedRevision, c.lastErr = plan.Revision, err
		}
		c.mu.Unlock()
	}
}

func (c *replicaController) closeCurrent() {
	c.mu.Lock()
	old := c.current
	c.current = nil
	c.mu.Unlock()
	if old != nil {
		old.Close()
	}
	c.mu.Lock()
	c.assignment = nil
	c.mu.Unlock()
}

func (c *replicaController) apply(ctx context.Context, plan *streamingpb.WALReplicaPlan) error {
	var local, primary *streamingpb.WALReplicaAssignment
	for _, member := range plan.Replicas {
		if member.GetNode().GetServerId() == c.nodeID {
			local = member
		}
		if member.AccessMode == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
			primary = member
		}
	}
	if local == nil {
		c.closeCurrent()
		return nil
	}
	source := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: c.pchannel, Term: plan.WalTerm, AccessMode: types.AccessModeRW},
		Node:    types.NewStreamingNodeInfoFromProto(primary.Node),
	}
	c.mu.Lock()
	current, assignment, sourceTerm, sourceNode := c.current, c.assignment, c.sourceTerm, c.sourceNode
	c.mu.Unlock()
	if current != nil && current.IsAvailable() && assignment.ReplicaId == local.ReplicaId && assignment.ReplicaRevision == local.ReplicaRevision {
		if sourceTerm == plan.WalTerm && sourceNode == source.Node {
			return nil
		}
		updater, ok := current.(wal.ReplicaSourceUpdater)
		if local.AccessMode != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY || !ok {
			return merr.WrapErrServiceInternalMsg("WAL replica cannot change source without replacing its local revision")
		}
		if err := updater.UpdateReplicaSource(ctx, source); err != nil {
			return err
		}
		c.mu.Lock()
		c.sourceTerm = plan.WalTerm
		c.sourceNode = source.Node
		c.mu.Unlock()
		return nil
	}
	c.closeCurrent()
	if local.AccessMode == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
		if c.failedWriterTerm >= plan.WalTerm {
			return merr.WrapErrServiceUnavailable("failed WAL writer requires a new term before reopening")
		}
		// Even a canceled open may have created an event source. Never reuse
		// its sequence zero within this term after a partially successful open.
		c.failedWriterTerm = plan.WalTerm
	}
	c.mu.Lock()
	c.assignment = proto.CloneOf(local)
	c.mu.Unlock()
	option := &wal.OpenOption{
		Channel: types.PChannelInfo{Name: c.pchannel, Term: plan.WalTerm, AccessMode: types.AccessMode(local.AccessMode)},
		Replica: proto.CloneOf(local), Primary: &source,
	}
	var opened wal.WAL
	var err error
	if local.AccessMode == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY {
		if opener, ok := c.opener.(wal.ReadOnlyReplicaOpener); ok {
			opened, err = opener.OpenReadOnlyReplica(ctx, option)
		} else {
			err = merr.WrapErrServiceUnavailable("WAL opener does not provide a read-only replica runtime")
		}
	} else {
		opened, err = c.opener.Open(ctx, option)
	}
	if err != nil {
		c.closeCurrent()
		return err
	}
	c.mu.Lock()
	if ctx.Err() != nil || !c.sameLocalTarget(c.plan, plan) {
		c.mu.Unlock()
		opened.Close()
		c.closeCurrent()
		return context.Canceled
	}
	c.current, c.assignment, c.sourceTerm = opened, proto.CloneOf(local), plan.WalTerm
	c.sourceNode = source.Node
	c.mu.Unlock()
	return nil
}

// Changes elsewhere in the replica set do not cancel an accepted local open.
// In particular, canceling a writer open would burn its term unnecessarily.
func (c *replicaController) sameLocalTarget(a, b *streamingpb.WALReplicaPlan) bool {
	if a.GetWalTerm() != b.GetWalTerm() {
		return false
	}
	local := func(plan *streamingpb.WALReplicaPlan) *streamingpb.WALReplicaAssignment {
		for _, member := range plan.GetReplicas() {
			if member.GetNode().GetServerId() == c.nodeID {
				return member
			}
		}
		return nil
	}
	primary := func(plan *streamingpb.WALReplicaPlan) *streamingpb.StreamingNodeInfo {
		for _, member := range plan.GetReplicas() {
			if member.AccessMode == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
				return member.Node
			}
		}
		return nil
	}
	return proto.Equal(local(a), local(b)) && proto.Equal(primary(a), primary(b))
}
