package balancer

import (
	"context"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type replicaPlanResult struct {
	response *streamingpb.ReconcileWALReplicasResponse
	err      error
}

// sendReplicaPlan uses the same session resolver and identity interceptors as
// writer management. A missing node is distinct from a live node timing out.
func sendReplicaPlan(ctx context.Context, nodeID int64, req *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
	client, ok := resource.Resource().StreamingNodeManagerClient().(manager.ReplicaPlanClient)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("streaming manager client does not support WAL replica plans")
	}
	return client.ReconcileWALReplicas(ctx, nodeID, req)
}

func (b *balancerImpl) reconcileReplicaPlan(ctx context.Context, meta *channel.PChannelMeta) error {
	plan := meta.ReplicaPlan()
	if plan == nil {
		return merr.WrapErrServiceInternalMsg("WAL %s has no persisted replica plan", meta.Name())
	}
	primaryID := meta.CurrentServerID()
	results := make(map[int64]chan replicaPlanResult)
	for _, members := range [][]*streamingpb.WALReplicaAssignment{plan.Replicas, plan.RetiringReplicas} {
		for _, member := range members {
			if member.Node != nil && member.Node.ServerId != primaryID {
				results[member.Node.ServerId] = make(chan replicaPlanResult, 1)
			}
		}
	}
	request := &streamingpb.ReconcileWALReplicasRequest{Pchannel: meta.Name(), Plan: plan}
	operationTimeout := paramtable.Get().StreamingCfg.WALBalancerOperationTimeout.GetAsDurationByParse()
	send := func(nodeID int64) replicaPlanResult {
		opCtx, cancel := context.WithTimeout(ctx, operationTimeout)
		defer cancel()
		response, err := sendReplicaPlan(opCtx, nodeID, request)
		if err == nil && response.GetPlanRevision() != plan.Revision {
			err = merr.WrapErrServiceUnavailable("WAL replica reported a different plan revision")
		}
		if err == nil {
			_, err = b.channelMetaManager.ApplyReplicaObservations(ctx, meta.ChannelID(), nodeID, response)
		}
		return replicaPlanResult{response: response, err: err}
	}
	// Start the writer independently of unrelated RO RPCs. Only former writers
	// on other nodes can delay activation. The local controller serializes a
	// same-node close/open, including an RO-to-RW transition.
	writerResult := make(chan replicaPlanResult, 1)
	go func() {
		lost := false
		for _, retired := range plan.RetiringReplicas {
			if meta.IsAssigned() || retired.AccessMode != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE || retired.Node.ServerId == primaryID {
				continue
			}
			var result replicaPlanResult
			select {
			case result = <-results[retired.Node.ServerId]:
				// More than one old incarnation may reside on the same node.
				results[retired.Node.ServerId] <- result
			case <-ctx.Done():
				writerResult <- replicaPlanResult{err: ctx.Err()}
				return
			}
			if errors.Is(result.err, types.ErrNotAlive) {
				lost = true
				continue
			}
			if result.err != nil {
				writerResult <- replicaPlanResult{err: merr.Wrap(result.err, "reconcile previous WAL writer")}
				return
			}
			if !replicaClosed(result.response, retired) {
				writerResult <- replicaPlanResult{err: merr.WrapErrServiceUnavailable("previous WAL writer has not drained")}
				return
			}
		}
		if lost {
			if err := b.waitNodeLostGracePeriod(ctx, meta); err != nil {
				writerResult <- replicaPlanResult{err: err}
				return
			}
		}
		writerResult <- send(primaryID)
	}()
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for nodeID, result := range results {
		g.Go(func() error {
			r := send(nodeID)
			result <- r
			if r.err != nil {
				b.Logger().Warn(ctx, "WAL replica plan reconciliation pending", mlog.String("pchannel", meta.Name()), mlog.Int64("nodeID", nodeID), mlog.Err(r.err))
			}
			return nil
		})
	}
	g.Wait()
	primary := <-writerResult
	// Keep absent or failed placements in the durable retiring set. Session
	// loss permits the existing writer-fencing grace, but is not a drain ACK.
	reports := make([]*streamingpb.WALReplicaObservation, 0)
	if primary.err == nil {
		reports = append(reports, primary.response.Replicas...)
	}
	for _, result := range results {
		r := <-result
		if r.err == nil {
			reports = append(reports, r.response.Replicas...)
		}
	}
	if err := b.channelMetaManager.CompleteReplicaRetirements(ctx, meta.ChannelID(), reports); err != nil {
		return err
	}
	return primary.err
}

func replicaClosed(response *streamingpb.ReconcileWALReplicasResponse, member *streamingpb.WALReplicaAssignment) bool {
	for _, report := range response.GetReplicas() {
		if report.ReplicaId == member.ReplicaId && report.ReplicaRevision == member.ReplicaRevision &&
			report.NodeId == member.Node.ServerId && report.State == streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED {
			return true
		}
	}
	return false
}
