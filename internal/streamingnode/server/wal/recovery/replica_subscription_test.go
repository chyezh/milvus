package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type subscriptionClient struct {
	streamingpb.StreamingNodeReplicaServiceClient
}

func (*subscriptionClient) SubscribeWALEvents(context.Context, *streamingpb.WALReplicaSubscribeRequest, ...grpc.CallOption) (streamingpb.StreamingNodeReplicaService_SubscribeWALEventsClient, error) {
	panic("mockey")
}

type subscriptionStream struct {
	streamingpb.StreamingNodeReplicaService_SubscribeWALEventsClient
	ctx context.Context
}

func (*subscriptionStream) Recv() (*streamingpb.WALReplicaEvent, error) { panic("mockey") }

func TestReplicaSubscriptionSupersedesAttemptsAndCloseCancelsReceive(t *testing.T) {
	mockey.PatchConvey("a superseded connection cannot apply to the retained runtime", t, func() {
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		ro, err := NewReadOnlyRecoveryStorage(readOnlyTestConfig(), WithNodeScheduler(scheduler),
			WithQueryViewEventApplier(func(context.Context, []*viewpb.QueryViewOfShard) error { return nil }))
		require.NoError(t, err)
		defer ro.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		opened := make(chan context.Context, 2)
		mockey.Mock((*subscriptionClient).SubscribeWALEvents).To(func(_ *subscriptionClient, ctx context.Context, req *streamingpb.WALReplicaSubscribeRequest, _ ...grpc.CallOption) (streamingpb.StreamingNodeReplicaService_SubscribeWALEventsClient, error) {
			require.Equal(t, ro.Applied().IntoProto(), req.After)
			opened <- ctx
			return &subscriptionStream{ctx: ctx}, nil
		}).Build()
		mockey.Mock((*subscriptionStream).Recv).To(func(s *subscriptionStream) (*streamingpb.WALReplicaEvent, error) {
			<-s.ctx.Done()
			return nil, s.ctx.Err()
		}).Build()
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { first <- ro.SubscribeAndApply(ctx, &subscriptionClient{}, ro.channel) }()
		firstCtx := <-opened
		go func() { second <- ro.SubscribeAndApply(ctx, &subscriptionClient{}, ro.channel) }()
		secondCtx := <-opened
		require.Error(t, <-first)
		require.ErrorIs(t, firstCtx.Err(), context.Canceled)
		event := walevent.Event{Cursor: walevent.Cursor{Term: 1, Seq: 1}, Payload: walevent.QueryViewIntentBatch{}}
		require.ErrorIs(t, ro.applySubscribedEvent(ctx, 1, event), context.Canceled)
		require.EqualValues(t, 0, ro.Applied().Seq)
		require.NoError(t, ro.applySubscribedEvent(ctx, 2, event))
		ro.Close()
		require.Error(t, <-second)
		require.ErrorIs(t, secondCtx.Err(), context.Canceled)
		require.ErrorIs(t, ro.SubscribeAndApply(ctx, &subscriptionClient{}, ro.channel), context.Canceled)
	})
}

func TestReplicaSubscriptionOpenAndDecodeFailuresPreserveCursor(t *testing.T) {
	mockey.PatchConvey("failed transport input does not advance the local prefix", t, func() {
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		ro, err := NewReadOnlyRecoveryStorage(readOnlyTestConfig(), WithNodeScheduler(scheduler))
		require.NoError(t, err)
		defer ro.Close()
		open := mockey.Mock((*subscriptionClient).SubscribeWALEvents).Return(nil, context.DeadlineExceeded).Build()
		require.Error(t, ro.SubscribeAndApply(t.Context(), &subscriptionClient{}, ro.channel))
		open.UnPatch()
		mockey.Mock((*subscriptionClient).SubscribeWALEvents).Return(&subscriptionStream{}, nil).Build()
		mockey.Mock((*subscriptionStream).Recv).Return(&streamingpb.WALReplicaEvent{}, nil).Build()
		require.Error(t, ro.SubscribeAndApply(t.Context(), &subscriptionClient{}, ro.channel))
		require.Equal(t, readOnlyTestConfig().Applied, ro.Applied())
	})
}
