package walmanager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type controllerTestOpener struct{ wal.Opener }

func (o *controllerTestOpener) OpenReadOnlyReplica(ctx context.Context, option *wal.OpenOption) (wal.WAL, error) {
	return o.Open(ctx, option)
}

func (*controllerTestOpener) Open(context.Context, *wal.OpenOption) (wal.WAL, error) { panic("mockey") }

type controllerTestWAL struct{ wal.WAL }

func (*controllerTestWAL) IsAvailable() bool { panic("mockey") }
func (*controllerTestWAL) Close()            { panic("mockey") }
func (*controllerTestWAL) UpdateReplicaSource(context.Context, types.PChannelInfoAssigned) error {
	panic("mockey")
}

func controllerTestPlan() *streamingpb.WALReplicaPlan {
	return &streamingpb.WALReplicaPlan{Revision: 1, WalTerm: 7, Replicas: []*streamingpb.WALReplicaAssignment{
		{ReplicaId: 1, ReplicaRevision: 1, ResourceGroup: "a", Node: &streamingpb.StreamingNodeInfo{ServerId: 1, Address: "a"}, AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE},
		{ReplicaId: 2, ReplicaRevision: 1, ResourceGroup: "b", Node: &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "b"}, AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY},
	}}
}

func awaitReplicaState(t *testing.T, c *replicaController, plan *streamingpb.WALReplicaPlan, state streamingpb.WALReplicaState) {
	t.Helper()
	require.Eventually(t, func() bool {
		response, err := c.reconcile(plan)
		require.NoError(t, err)
		return len(response.Replicas) > 0 && response.Replicas[0].State == state
	}, 5*time.Second, time.Millisecond)
}

func TestReplicaControllerKeepsRuntimeAcrossSourceTerms(t *testing.T) {
	mockey.PatchConvey("primary replacement updates the source of the retained RO runtime", t, func() {
		instance := &controllerTestWAL{}
		opened := mockey.Mock((*controllerTestOpener).Open).To(func(_ *controllerTestOpener, _ context.Context, option *wal.OpenOption) (wal.WAL, error) {
			require.Equal(t, types.AccessModeRO, option.Channel.AccessMode)
			require.EqualValues(t, 2, option.Replica.ReplicaId)
			require.EqualValues(t, 1, option.Primary.Node.ServerID)
			return instance, nil
		}).Build()
		mockey.Mock((*controllerTestWAL).IsAvailable).Return(true).Build()
		closed := mockey.Mock((*controllerTestWAL).Close).Return().Build()
		updated := mockey.Mock((*controllerTestWAL).UpdateReplicaSource).To(func(_ *controllerTestWAL, _ context.Context, primary types.PChannelInfoAssigned) error {
			require.EqualValues(t, 8, primary.Channel.Term)
			return nil
		}).Build()
		c := newReplicaController(t.Context(), 2, "p", &controllerTestOpener{})
		defer c.close()
		plan := controllerTestPlan()
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		plan.Revision, plan.WalTerm = 2, 8
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		require.Same(t, instance, c.getWAL())
		require.Equal(t, 1, opened.Times())
		require.Equal(t, 1, updated.Times())
		require.Zero(t, closed.Times())
		stale := controllerTestPlan()
		response, err := c.reconcile(stale)
		require.NoError(t, err)
		require.EqualValues(t, 2, response.PlanRevision)
		conflict := proto.CloneOf(plan)
		conflict.WalTerm++
		_, err = c.reconcile(conflict)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		conflict.Revision++
		conflict.WalTerm = 6
		_, err = c.reconcile(conflict)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		c.close()
		_, err = c.reconcile(plan)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestReplicaControllerDoesNotBurnWriterTermForUnrelatedMembership(t *testing.T) {
	mockey.PatchConvey("full-plan updates elsewhere do not cancel an admitted writer open", t, func() {
		entered := make(chan context.Context, 1)
		finish := make(chan struct{})
		mockey.Mock((*controllerTestOpener).Open).To(func(_ *controllerTestOpener, ctx context.Context, _ *wal.OpenOption) (wal.WAL, error) {
			entered <- ctx
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-finish:
				return &controllerTestWAL{}, nil
			}
		}).Build()
		mockey.Mock((*controllerTestWAL).IsAvailable).Return(true).Build()
		mockey.Mock((*controllerTestWAL).Close).Return().Build()
		c := newReplicaController(t.Context(), 1, "p", &controllerTestOpener{})
		defer c.close()
		plan := controllerTestPlan()
		response, err := c.reconcile(plan)
		require.NoError(t, err)
		require.Equal(t, streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING, response.Replicas[0].State)
		opening := <-entered
		plan.Revision++
		plan.Replicas[1].Node = nil
		_, err = c.reconcile(plan)
		require.NoError(t, err)
		require.NoError(t, opening.Err())
		close(finish)
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
	})
}

func TestReplicaControllerRetirementWaitsForClose(t *testing.T) {
	mockey.PatchConvey("an exact retired placement reports closed only after runtime drain", t, func() {
		mockey.Mock((*controllerTestOpener).Open).Return(&controllerTestWAL{}, nil).Build()
		mockey.Mock((*controllerTestWAL).IsAvailable).Return(true).Build()
		closing, drained := make(chan struct{}), make(chan struct{})
		mockey.Mock((*controllerTestWAL).Close).To(func(*controllerTestWAL) { close(closing); <-drained }).Build()
		c := newReplicaController(t.Context(), 2, "p", &controllerTestOpener{})
		defer c.close()
		plan := controllerTestPlan()
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		plan.Revision++
		plan.RetiringReplicas = []*streamingpb.WALReplicaAssignment{proto.CloneOf(plan.Replicas[1]), proto.CloneOf(plan.Replicas[0])}
		plan.Replicas = plan.Replicas[:1]
		_, err := c.reconcile(plan)
		require.NoError(t, err)
		<-closing
		response, err := c.reconcile(plan)
		require.NoError(t, err)
		require.Len(t, response.Replicas, 1)
		require.Equal(t, streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING, response.Replicas[0].State)
		close(drained)
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED)
		require.Nil(t, c.getWAL())
	})
}

func TestReplicaControllerFailedWriterNeverReopensTheSameTerm(t *testing.T) {
	mockey.PatchConvey("partial source creation requires a fresh writer term", t, func() {
		var attempts atomic.Int64
		mockey.Mock((*controllerTestOpener).Open).To(func(_ *controllerTestOpener, _ context.Context, _ *wal.OpenOption) (wal.WAL, error) {
			attempts.Add(1)
			return nil, merr.WrapErrServiceUnavailable("open failed after source creation")
		}).Build()
		c := newReplicaController(t.Context(), 1, "p", &controllerTestOpener{})
		defer c.close()
		plan := controllerTestPlan()
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED)
		plan.Revision++
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED)
		require.EqualValues(t, 1, attempts.Load())
		plan.Revision++
		plan.WalTerm++
		plan.Replicas[0].ReplicaRevision++
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED)
		require.EqualValues(t, 2, attempts.Load())
	})
}

func TestReplicaPlanAdmissionValidation(t *testing.T) {
	for _, mutate := range []func(*streamingpb.WALReplicaPlan){
		func(p *streamingpb.WALReplicaPlan) { p.Revision = 0 },
		func(p *streamingpb.WALReplicaPlan) { p.Replicas[0].ReplicaRevision = 0 },
		func(p *streamingpb.WALReplicaPlan) { p.Replicas[1].ReplicaId = 1 },
		func(p *streamingpb.WALReplicaPlan) { p.Replicas[1].Node.ServerId = 1 },
		func(p *streamingpb.WALReplicaPlan) { p.Replicas[1].Node.Address = "" },
		func(p *streamingpb.WALReplicaPlan) { p.Replicas[0].Node = nil },
		func(p *streamingpb.WALReplicaPlan) { p.Replicas[0].AccessMode = 99 },
		func(p *streamingpb.WALReplicaPlan) {
			p.Replicas[1].AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE
		},
	} {
		p := controllerTestPlan()
		mutate(p)
		_, err := (&replicaController{}).reconcile(p)
		require.Error(t, err)
	}
	p := controllerTestPlan()
	p.Replicas[1].Node = nil
	require.NoError(t, validateReplicaPlan(p))
}

func TestReplicaControllerRetriesReadOnlySourceWithoutClosing(t *testing.T) {
	mockey.PatchConvey("a failed source reconciliation leaves local resources available", t, func() {
		instance := &controllerTestWAL{}
		mockey.Mock((*controllerTestOpener).Open).Return(instance, nil).Build()
		mockey.Mock((*controllerTestWAL).IsAvailable).Return(true).Build()
		closed := mockey.Mock((*controllerTestWAL).Close).Return().Build()
		failure := merr.WrapErrServiceUnavailable("primary is recovering")
		var attempts atomic.Int64
		mockey.Mock((*controllerTestWAL).UpdateReplicaSource).To(func(_ *controllerTestWAL, _ context.Context, _ types.PChannelInfoAssigned) error {
			if attempts.Add(1) == 1 {
				return failure
			}
			return nil
		}).Build()
		c := newReplicaController(t.Context(), 2, "p", &controllerTestOpener{})
		defer c.close()
		plan := controllerTestPlan()
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		plan.Revision++
		plan.WalTerm++
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED)
		require.Same(t, instance, c.getWAL())
		require.Zero(t, closed.Times())
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		require.EqualValues(t, 2, attempts.Load())
	})
}

func TestReplicaControllerSupersededOpenCannotBecomeReady(t *testing.T) {
	mockey.PatchConvey("a successful but canceled open is closed before the retired instance is acknowledged", t, func() {
		entered := make(chan struct{})
		mockey.Mock((*controllerTestOpener).Open).To(func(_ *controllerTestOpener, ctx context.Context, _ *wal.OpenOption) (wal.WAL, error) {
			close(entered)
			<-ctx.Done()
			return &controllerTestWAL{}, nil
		}).Build()
		mockey.Mock((*controllerTestWAL).IsAvailable).Return(true).Build()
		closed := mockey.Mock((*controllerTestWAL).Close).Return().Build()
		c := newReplicaController(t.Context(), 2, "p", &controllerTestOpener{})
		defer c.close()
		plan := controllerTestPlan()
		_, err := c.reconcile(plan)
		require.NoError(t, err)
		<-entered
		plan.Revision++
		plan.RetiringReplicas = []*streamingpb.WALReplicaAssignment{proto.CloneOf(plan.Replicas[1])}
		plan.Replicas = plan.Replicas[:1]
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED)
		require.Nil(t, c.getWAL())
		require.Equal(t, 1, closed.Times())
	})
}

func promotedControllerPlan(previous *streamingpb.WALReplicaPlan) *streamingpb.WALReplicaPlan {
	plan := proto.CloneOf(previous)
	plan.Revision++
	plan.WalTerm++
	for _, member := range plan.Replicas {
		plan.RetiringReplicas = append(plan.RetiringReplicas, proto.CloneOf(member))
		member.ReplicaRevision++
	}
	plan.Replicas[0].AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY
	plan.Replicas[1].AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE
	return plan
}

func TestReplicaControllerPromotionKeepsQueriesUntilWriterIsPrepared(t *testing.T) {
	mockey.PatchConvey("promotion retains the old RO while the RW candidate opens and tracks its eventual drain", t, func() {
		old, writer := &controllerTestWAL{}, &controllerTestWAL{}
		opening, prepared := make(chan struct{}), make(chan struct{})
		closing, drained := make(chan struct{}), make(chan struct{})
		mockey.Mock((*controllerTestOpener).Open).To(func(_ *controllerTestOpener, ctx context.Context, option *wal.OpenOption) (wal.WAL, error) {
			if option.Channel.AccessMode == types.AccessModeRO {
				return old, nil
			}
			close(opening)
			select {
			case <-prepared:
				return writer, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Build()
		mockey.Mock((*controllerTestWAL).IsAvailable).Return(true).Build()
		mockey.Mock((*controllerTestWAL).Close).To(func(w *controllerTestWAL) {
			if w == old {
				close(closing)
				<-drained
			}
		}).Build()
		c := newReplicaController(t.Context(), 2, "p", &controllerTestOpener{})
		defer c.close()
		plan := controllerTestPlan()
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		plan = promotedControllerPlan(plan)
		_, err := c.reconcile(plan)
		require.NoError(t, err)
		<-opening
		require.Same(t, old, c.getWAL(), "existing query resources remain available during writer recovery")
		select {
		case <-closing:
			t.Fatal("RO closed before writer preparation")
		default:
		}
		close(prepared)
		<-closing
		require.Same(t, writer, c.getWAL())
		response, err := c.reconcile(plan)
		require.NoError(t, err)
		require.Len(t, response.Replicas, 2)
		require.Equal(t, streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING, response.Replicas[1].State, "replacement publication is not the old runtime's drain acknowledgement")
		close(drained)
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		response, err = c.reconcile(plan)
		require.NoError(t, err)
		require.Equal(t, streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED, response.Replicas[1].State)
	})
}

func TestReplicaControllerFailedPromotionPreservesReadOnlyRuntime(t *testing.T) {
	mockey.PatchConvey("failed writer preparation neither destroys the RO nor reuses the failed term", t, func() {
		old := &controllerTestWAL{}
		var attempts atomic.Int64
		mockey.Mock((*controllerTestOpener).Open).To(func(_ *controllerTestOpener, _ context.Context, option *wal.OpenOption) (wal.WAL, error) {
			if option.Channel.AccessMode == types.AccessModeRO {
				return old, nil
			}
			attempts.Add(1)
			return nil, merr.ErrServiceUnavailable
		}).Build()
		mockey.Mock((*controllerTestWAL).IsAvailable).Return(true).Build()
		closed := mockey.Mock((*controllerTestWAL).Close).Return().Build()
		c := newReplicaController(t.Context(), 2, "p", &controllerTestOpener{})
		defer c.close()
		plan := controllerTestPlan()
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		plan = promotedControllerPlan(plan)
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED)
		require.Same(t, old, c.getWAL())
		plan.Revision++
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED)
		require.EqualValues(t, 1, attempts.Load())
		require.Zero(t, closed.Times())
	})
}

func TestReplicaControllerCanceledPromotionMustDrainItsCandidate(t *testing.T) {
	mockey.PatchConvey("superseded promotion cannot acknowledge its candidate closed before Close returns", t, func() {
		old, candidate := &controllerTestWAL{}, &controllerTestWAL{}
		opening, closing, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
		mockey.Mock((*controllerTestOpener).Open).To(func(_ *controllerTestOpener, ctx context.Context, option *wal.OpenOption) (wal.WAL, error) {
			if option.Channel.AccessMode == types.AccessModeRO {
				return old, nil
			}
			close(opening)
			<-ctx.Done()
			return candidate, nil
		}).Build()
		mockey.Mock((*controllerTestWAL).IsAvailable).Return(true).Build()
		mockey.Mock((*controllerTestWAL).Close).To(func(w *controllerTestWAL) {
			if w == candidate {
				close(closing)
				<-drained
			}
		}).Build()
		c := newReplicaController(t.Context(), 2, "p", &controllerTestOpener{})
		defer c.close()
		plan := controllerTestPlan()
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
		plan = promotedControllerPlan(plan)
		_, err := c.reconcile(plan)
		require.NoError(t, err)
		<-opening
		plan.Revision++
		plan.WalTerm++
		plan.RetiringReplicas = append(plan.RetiringReplicas, proto.CloneOf(plan.Replicas[1]))
		plan.Replicas = plan.Replicas[:1]
		plan.Replicas[0].AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE
		plan.Replicas[0].ReplicaRevision++
		_, err = c.reconcile(plan)
		require.NoError(t, err)
		<-closing
		require.Same(t, old, c.getWAL())
		response, err := c.reconcile(plan)
		require.NoError(t, err)
		require.Len(t, response.Replicas, 2)
		for _, report := range response.Replicas {
			require.Equal(t, streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING, report.State)
		}
		close(drained)
		awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED)
	})
}
