package balancer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func replicaReconcileFixture(t *testing.T, names ...string) (*balancerImpl, channel.ReplicaPlanTarget) {
	t.Helper()
	paramtable.Init()
	catalog := catalogkv.NewCataLog(nil)
	session := &sessionutil.Session{}
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(session))
	patches := []*mockey.Mocker{
		mockey.Mock(mockey.GetMethod(catalog, "GetVersion")).Return(nil, nil).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "GetCChannel")).Return(&streamingpb.CChannelMeta{Pchannel: names[0]}, nil).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "GetReplicateConfiguration")).Return(nil, nil).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "ListPChannel")).Return(nil, nil).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "SavePChannels")).Return(nil).Build(),
		mockey.Mock((*sessionutil.Session).GetRegisteredRevision).Return(int64(1)).Build(),
	}
	t.Cleanup(func() {
		for _, patch := range patches {
			patch.UnPatch()
		}
	})
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager(names)
	cm, err := channel.RecoverChannelManager(t.Context(), names...)
	require.NoError(t, err)
	cm.SetLogger(mlog.With())
	b := &balancerImpl{channelMetaManager: cm}
	b.SetLogger(mlog.With())
	target := channel.ReplicaPlanTarget{PrimaryResourceGroup: "a", Nodes: map[string]*types.StreamingNodeInfo{
		"a": {ServerID: 1, Address: "a"}, "b": {ServerID: 2, Address: "b"}, "c": {ServerID: 3, Address: "c"},
	}}
	plans := make(map[types.ChannelID]channel.ReplicaPlanTarget)
	for _, name := range names {
		plans[types.ChannelID{Name: name}] = target
	}
	_, err = cm.AssignReplicaPlans(t.Context(), plans)
	require.NoError(t, err)
	return b, target
}

func localReadyResponse(nodeID int64, req *streamingpb.ReconcileWALReplicasRequest) *streamingpb.ReconcileWALReplicasResponse {
	resp := &streamingpb.ReconcileWALReplicasResponse{PlanRevision: req.Plan.Revision}
	for i, members := range [][]*streamingpb.WALReplicaAssignment{req.Plan.Replicas, req.Plan.RetiringReplicas} {
		for _, member := range members {
			if member.GetNode().GetServerId() != nodeID {
				continue
			}
			state := streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY
			if i == 1 {
				state = streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED
			}
			resp.Replicas = append(resp.Replicas, &streamingpb.WALReplicaObservation{NodeId: nodeID, ReplicaId: member.ReplicaId, ReplicaRevision: member.ReplicaRevision, SourceTerm: req.Plan.WalTerm, State: state})
		}
	}
	return resp
}

func TestReplicaReconcileWriterReadinessIndependentOfFollowers(t *testing.T) {
	b, _ := replicaReconcileFixture(t, "p")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	releaseFollower := make(chan struct{})
	patch := mockey.Mock(sendReplicaPlan).To(func(ctx context.Context, nodeID int64, req *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
		if nodeID == 2 {
			select {
			case <-releaseFollower:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return nil, merr.ErrServiceUnavailable
		}
		return localReadyResponse(nodeID, req), nil
	}).Build()
	defer patch.UnPatch()
	done := make(chan error, 1)
	go func() {
		done <- b.applyBalanceResultToStreamingNode(ctx, b.channelMetaManager.CurrentPChannelsView().Channels)
	}()
	require.Eventually(t, func() bool {
		return b.channelMetaManager.CurrentPChannelsView().Channels[types.ChannelID{Name: "p"}].IsAssigned()
	}, time.Second, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("follower was not pending: %v", err)
	default:
	}
	close(releaseFollower)
	require.NoError(t, <-done)
}

func TestReplicaReconcileFencesFormerWriter(t *testing.T) {
	for _, outcome := range []string{"closed", "preparing", "lost", "timeout", "wrong-revision", "foreign-report"} {
		t.Run(outcome, func(t *testing.T) {
			b, target := replicaReconcileFixture(t, "p")
			id := types.ChannelID{Name: "p"}
			require.NoError(t, b.channelMetaManager.AssignPChannelsDone(t.Context(), []types.ChannelID{id}))
			target.PrimaryResourceGroup = "b"
			updated, err := b.channelMetaManager.AssignReplicaPlans(t.Context(), map[types.ChannelID]channel.ReplicaPlanTarget{id: target})
			require.NoError(t, err)
			grace := &paramtable.Get().StreamingCfg.WALBalancerNodeLostGracePeriod
			previous := grace.SwapTempValue("1ms")
			defer grace.SwapTempValue(previous)
			var writerCalls atomic.Int64
			patch := mockey.Mock(sendReplicaPlan).To(func(_ context.Context, nodeID int64, req *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
				resp := localReadyResponse(nodeID, req)
				if nodeID == 2 {
					writerCalls.Add(1)
				}
				if nodeID == 1 {
					switch outcome {
					case "lost":
						return nil, types.ErrNotAlive
					case "timeout":
						return nil, context.DeadlineExceeded
					case "wrong-revision":
						resp.PlanRevision--
					case "foreign-report":
						resp.Replicas[0].NodeId = 2
					case "preparing":
						for _, r := range resp.Replicas {
							r.State = streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING
						}
					}
				}
				return resp, nil
			}).Build()
			defer patch.UnPatch()
			err = b.applyBalanceResultToStreamingNode(t.Context(), updated)
			if outcome == "closed" || outcome == "lost" {
				require.NoError(t, err)
				require.EqualValues(t, 1, writerCalls.Load())
			} else {
				switch outcome {
				case "timeout":
					require.ErrorIs(t, err, context.DeadlineExceeded)
				case "foreign-report":
					require.ErrorIs(t, err, merr.ErrServiceInternal)
				default:
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				}
				require.Zero(t, writerCalls.Load())
			}
			if outcome == "lost" {
				require.NotEmpty(t, b.channelMetaManager.CurrentPChannelsView().Channels[id].ReplicaPlan().RetiringReplicas)
			}
		})
	}
}

func prepareManualReplicaTransfer(t *testing.T, names ...string) *balancerImpl {
	b, _ := replicaReconcileFixture(t, names...)
	ids := make([]types.ChannelID, 0, len(names))
	for _, name := range names {
		ids = append(ids, types.ChannelID{Name: name})
	}
	require.NoError(t, b.channelMetaManager.AssignPChannelsDone(t.Context(), ids))
	patches := []*mockey.Mocker{
		mockey.Mock((*balancerImpl).GetAllStreamingNodes).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {}}, nil).Build(),
		mockey.Mock((*balancerImpl).fetchStreamingNodeStatus).Return(map[int64]*types.StreamingNodeStatus{4: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 4, Address: "a4"}, ResourceGroup: "a"}}, nil).Build(),
	}
	t.Cleanup(func() {
		for _, patch := range patches {
			patch.UnPatch()
		}
	})
	return b
}

func TestTransferChannelsWaitsForWALConfirmation(t *testing.T) {
	for _, failure := range []string{"", "persist-intent", "close", "open", "persist-ready", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			b := prepareManualReplicaTransfer(t, "p")
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			opening, confirmed := make(chan struct{}), make(chan struct{})
			if failure == "persist-intent" {
				patch := mockey.Mock((*channel.ChannelManager).AssignReplicaPlans).Return(nil, merr.ErrServiceUnavailable).Build()
				defer patch.UnPatch()
			}
			if failure == "persist-ready" {
				patch := mockey.Mock((*channel.ChannelManager).ApplyReplicaObservations).To(func(_ context.Context, _ types.ChannelID, nodeID int64, _ *streamingpb.ReconcileWALReplicasResponse) (bool, error) {
					if nodeID == 4 {
						return false, merr.ErrServiceUnavailable
					}
					return false, nil
				}).Build()
				defer patch.UnPatch()
			}
			patch := mockey.Mock(sendReplicaPlan).To(func(ctx context.Context, nodeID int64, req *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
				if nodeID == 1 && failure == "close" {
					return nil, merr.ErrServiceUnavailable
				}
				if nodeID == 4 {
					if failure == "open" {
						return nil, merr.ErrServiceUnavailable
					}
					close(opening)
					select {
					case <-confirmed:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return localReadyResponse(nodeID, req), nil
			}).Build()
			defer patch.UnPatch()
			done := make(chan error, 1)
			go func() { done <- b.transferChannels(ctx, TransferChannelsRequest{SourceNodeID: 1, TargetNodeID: 4}) }()
			if failure == "" || failure == "cancel" {
				select {
				case <-opening:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case err := <-done:
					t.Fatalf("returned before writer readiness: %v", err)
				default:
				}
				if failure == "cancel" {
					cancel()
				} else {
					close(confirmed)
				}
			} else {
				close(confirmed)
			}
			err := <-done
			switch failure {
			case "":
				require.NoError(t, err)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			default:
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			}
		})
	}
}

func TestTransferChannelsPartialAssignmentCanResume(t *testing.T) {
	b := prepareManualReplicaTransfer(t, "p1", "p2")
	var fail atomic.Bool
	fail.Store(true)
	patch := mockey.Mock(sendReplicaPlan).To(func(_ context.Context, nodeID int64, req *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
		if nodeID == 4 && req.Pchannel == "p2" && fail.Load() {
			return nil, merr.ErrServiceUnavailable
		}
		return localReadyResponse(nodeID, req), nil
	}).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, b.transferChannels(t.Context(), TransferChannelsRequest{SourceNodeID: 1, TargetNodeID: 4}), merr.ErrServiceUnavailable)
	view := b.channelMetaManager.CurrentPChannelsView()
	require.True(t, view.Channels[types.ChannelID{Name: "p1"}].IsAssigned())
	require.False(t, view.Channels[types.ChannelID{Name: "p2"}].IsAssigned())
	fail.Store(false)
	require.NoError(t, b.waitReplicaWritersReady(t.Context(), view.Channels))
	require.True(t, b.channelMetaManager.CurrentPChannelsView().Channels[types.ChannelID{Name: "p2"}].IsAssigned())
}

func TestWaitReplicaWriterPreparation(t *testing.T) {
	for _, outcome := range []string{"ready", "failed", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			b, _ := replicaReconcileFixture(t, "p")
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			var attempts atomic.Int64
			patch := mockey.Mock(sendReplicaPlan).To(func(_ context.Context, nodeID int64, req *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
				resp := localReadyResponse(nodeID, req)
				if nodeID == 1 {
					n := attempts.Add(1)
					if n == 1 {
						resp.Replicas[0].State = streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING
					}
					if outcome == "failed" {
						resp.Replicas[0].State = streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED
					}
					if outcome == "canceled" {
						cancel()
					}
				}
				return resp, nil
			}).Build()
			defer patch.UnPatch()
			err := b.waitReplicaWritersReady(ctx, b.channelMetaManager.CurrentPChannelsView().Channels)
			switch outcome {
			case "ready":
				require.NoError(t, err)
				require.EqualValues(t, 2, attempts.Load())
			case "failed":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			case "canceled":
				require.ErrorIs(t, err, context.Canceled)
			}
			require.NoError(t, b.waitReplicaWritersReady(t.Context(), nil))
		})
	}
}
