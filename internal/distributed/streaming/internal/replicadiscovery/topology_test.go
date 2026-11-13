package replicadiscovery

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type topologyTestWatcher struct {
	types.AssignmentDiscoverWatcher
}

func (*topologyTestWatcher) AssignmentDiscover(context.Context, func(*types.VersionedStreamingNodeAssignments) error) error {
	panic("mockey")
}

func TestTopologyMembersResumeIndependently(t *testing.T) {
	mockey.PatchConvey("slow or disconnected replicas cannot hide a healthy directory", t, func() {
		updates := make(chan *types.VersionedStreamingNodeAssignments, 8)
		mockey.Mock((*topologyTestWatcher).AssignmentDiscover).To(func(ctx context.Context, callback func(*types.VersionedStreamingNodeAssignments) error) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case update := <-updates:
					if err := callback(update); err != nil {
						return err
					}
				}
			}
		}).Build()
		primary, readonly := NewDirectory(), NewDirectory()
		defer primary.Close()
		defer readonly.Close()
		primary.Set("p_1v0", 7, &qviews.QueryViewVersion{}, false)
		readonly.Set("p_1v0", 7, &qviews.QueryViewVersion{}, false)
		var calls [5]atomic.Int64
		var primaryFailed atomic.Bool
		manager := NewTopologyClient(&topologyTestWatcher{}, func(ctx context.Context, pc string, member *streamingpb.WALReplicaAssignment) (Stream, qviews.StreamingNode, error) {
			node := member.Node.ServerId
			attempt := calls[node].Add(1)
			identity := qviews.StreamingNode{PChannel: pc, NodeID: node, WALReplicaID: member.ReplicaId, WALReplicaRevision: member.ReplicaRevision, RuntimeID: uint64(attempt), WALTerm: 7, ReadOnly: node != 1}
			switch node {
			case 1:
				if !primaryFailed.Load() {
					return primary.AcquireStream(ctx, pc, nil), identity, nil
				}
			case 2:
				return readonly.AcquireStream(ctx, pc, nil), identity, nil
			}
			return nil, qviews.StreamingNode{}, merr.WrapErrServiceUnavailable("member preparing")
		})
		defer manager.Close()
		var requireHealthy, lostHealthy atomic.Bool
		var firstReady sync.Once
		readyCallback := make(chan struct{})
		sub, err := manager.Subscribe("p_1v0", func(snapshot Snapshot) {
			if snapshot.Valid && len(snapshot.Replicas) == 2 {
				firstReady.Do(func() { requireHealthy.Store(true); close(readyCallback) })
			}
			if requireHealthy.Load() && (!snapshot.Valid || len(snapshot.Replicas) == 0) {
				lostHealthy.Store(true)
			}
		})
		require.NoError(t, err)
		defer sub.Close()
		updates <- &types.VersionedStreamingNodeAssignments{Version: typeutil.VersionInt64Pair{Global: -1}}
		require.False(t, sub.Snapshot().Valid)
		plan := &streamingpb.WALReplicaPlan{Revision: 1, WalTerm: 7, Replicas: []*streamingpb.WALReplicaAssignment{
			{ReplicaId: 1, ReplicaRevision: 1, Node: &streamingpb.StreamingNodeInfo{ServerId: 1}},
			{ReplicaId: 2, ReplicaRevision: 1, Node: &streamingpb.StreamingNodeInfo{ServerId: 2}, AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY},
			{ReplicaId: 3, ReplicaRevision: 1, Node: &streamingpb.StreamingNodeInfo{ServerId: 3}, AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY},
			{ReplicaId: 4},
		}}
		publish := func() {
			updates <- &types.VersionedStreamingNodeAssignments{Version: typeutil.VersionInt64Pair{Global: 1, Local: int64(plan.Revision)}, WALReplicaPlans: map[string]*streamingpb.WALReplicaPlan{"p": proto.CloneOf(plan)}}
		}
		publish()
		require.Eventually(t, func() bool { s := sub.Snapshot(); return s.Valid && len(s.Replicas) == 2 }, 5*time.Second, time.Millisecond)
		candidates := sub.Snapshot().Replicas
		require.EqualValues(t, 7, candidates[0].ReplicaID)
		require.EqualValues(t, 7, candidates[1].ReplicaID)
		require.NotEqual(t, candidates[0].StreamingNode, candidates[1].StreamingNode)
		select {
		case <-readyCallback:
		case <-time.After(time.Second):
			t.Fatal("ready callback was not delivered")
		}
		primaryFailed.Store(true)
		primary.Close()
		require.Eventually(t, func() bool {
			s := sub.Snapshot()
			return s.Valid && len(s.Replicas) == 1 && s.Replicas[0].StreamingNode.NodeID == 2
		}, 5*time.Second, time.Millisecond)
		plan.Revision++
		plan.WalTerm = 8
		plan.Replicas[0].Node.ServerId = 4
		publish()
		require.Eventually(t, func() bool { return calls[4].Load() > 0 }, time.Second, time.Millisecond)
		require.EqualValues(t, 1, calls[2].Load(), "a source term change does not reopen a healthy RO watch")
		require.EqualValues(t, 7, sub.Snapshot().Replicas[0].StreamingNode.WALTerm)
		require.False(t, lostHealthy.Load())
		requireHealthy.Store(false)
		same, err := manager.Subscribe("p_2v0", func(Snapshot) {})
		require.NoError(t, err)
		defer same.Close()
		require.EqualValues(t, 1, calls[2].Load(), "VChannels share the member's physical stream")
		plan.Revision++
		plan.Replicas = plan.Replicas[2:3]
		publish()
		require.Eventually(t, func() bool { return !sub.Snapshot().Valid }, time.Second, time.Millisecond)
		plan.Revision++
		plan.Replicas = nil
		publish()
		require.Eventually(t, func() bool { s := sub.Snapshot(); return s.Valid && len(s.Replicas) == 0 }, time.Second, time.Millisecond)
		plan.Revision++
		plan.Replicas = []*streamingpb.WALReplicaAssignment{{ReplicaId: 2, ReplicaRevision: 2, Node: &streamingpb.StreamingNodeInfo{ServerId: 2}}}
		publish()
		require.Eventually(t, func() bool {
			s := sub.Snapshot()
			return s.Valid && len(s.Replicas) == 1 && s.Replicas[0].StreamingNode.RuntimeID == 2
		}, time.Second, time.Millisecond)
		manager.Close()
		require.False(t, sub.Snapshot().Valid)
	})
}

func TestTopologySubscriptionValidationAndRuntimeOwnership(t *testing.T) {
	mockey.PatchConvey("runtime metadata is owned and closed subscriptions stay invalid", t, func() {
		mockey.Mock((*topologyTestWatcher).AssignmentDiscover).To(func(ctx context.Context, _ func(*types.VersionedStreamingNodeAssignments) error) error {
			<-ctx.Done()
			return ctx.Err()
		}).Build()
		directory := NewDirectory()
		defer directory.Close()
		directory.Set("p_1v0", 1, &qviews.QueryViewVersion{}, false)
		stream := newTopologyStream(t.Context(), "p", &topologyTestWatcher{}, nil)
		defer stream.Close()
		_, err := stream.Subscribe(t.Context(), "other_1v0", func(Snapshot) {})
		require.Error(t, err)
		_, err = stream.Subscribe(t.Context(), "p_1v0", nil)
		require.Error(t, err)
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = stream.Subscribe(canceled, "p_1v0", func(Snapshot) {})
		require.ErrorIs(t, err, context.Canceled)
		sub, err := stream.Subscribe(t.Context(), "p_1v0", func(Snapshot) {})
		require.NoError(t, err)
		require.False(t, sub.Snapshot().Valid)
		require.NoError(t, sub.Close())
		require.NoError(t, sub.Close())
		require.False(t, sub.Snapshot().Valid)
		require.NoError(t, stream.Close())
		require.Error(t, stream.Error())
		_, err = stream.Subscribe(t.Context(), "p_1v0", func(Snapshot) {})
		require.Error(t, err)
		raw := directory.AcquireStream(t.Context(), "p", nil)
		defer raw.Close()
		bound := &runtimeStream{Stream: raw, identity: qviews.StreamingNode{NodeID: 2, RuntimeID: 9}}
		local, err := bound.Subscribe(t.Context(), "p_1v0", func(Snapshot) {})
		require.NoError(t, err)
		defer local.Close()
		require.Eventually(t, func() bool { return local.Snapshot().Valid }, time.Second, time.Millisecond)
		snapshot := local.Snapshot()
		require.EqualValues(t, 9, snapshot.Replicas[0].StreamingNode.RuntimeID)
		snapshot.Replicas[0].StreamingNode.RuntimeID = 100
		require.EqualValues(t, 9, local.Snapshot().Replicas[0].StreamingNode.RuntimeID)
		_, err = bound.Subscribe(canceled, "p_1v0", func(Snapshot) {})
		require.Error(t, err)
	})
}
