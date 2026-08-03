package handler

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_assignment"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_consumer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_resolver"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	handlerregistry "github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	transformlogclient "github.com/milvus-io/milvus/internal/streamingnode/client/handler/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_types"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type shutdownWALManager struct{}

func (m shutdownWALManager) GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	return m.GetAvailableWALReplica(channel, 0)
}

func (m shutdownWALManager) GetAvailableWALReplica(types.PChannelInfo, int64) (wal.WAL, error) {
	return nil, status.NewOnShutdownError("wal manager is closed")
}

func (m shutdownWALManager) Metrics() (*types.StreamingNodeMetrics, error) {
	return nil, status.NewOnShutdownError("wal manager is closed")
}

type handlerFakeWALReplicaRebalanceTrigger struct {
	reportedPChannelAssignment *types.PChannelInfo
	reportedWALAssignment      *types.PChannelInfoAssigned
}

func (t *handlerFakeWALReplicaRebalanceTrigger) ReportAssignmentError(ctx context.Context, pchannel types.PChannelInfo, err error) error {
	t.reportedPChannelAssignment = &pchannel
	return nil
}

func (t *handlerFakeWALReplicaRebalanceTrigger) ReportWALReplicaAssignmentError(ctx context.Context, assignment types.PChannelInfoAssigned, err error) error {
	t.reportedWALAssignment = &assignment
	return nil
}

func TestHandlerClient(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}

	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
	handlerServiceClient := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	handlerServiceClient.EXPECT().GetReplicateCheckpoint(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, grcr *streamingpb.GetReplicateCheckpointRequest, co ...grpc.CallOption) (*streamingpb.GetReplicateCheckpointResponse, error) {
		serverID, ok := contextutil.GetPickServerID(ctx)
		assert.True(t, ok)
		assert.Equal(t, serverID, assignment.Node.ServerID)
		return &streamingpb.GetReplicateCheckpointResponse{
			Checkpoint: &commonpb.ReplicateCheckpoint{
				ClusterId: "pchannel",
				Pchannel:  "pchannel",
				MessageId: nil,
				TimeTick:  0,
			},
		}, nil
	})
	service.EXPECT().GetService(mock.Anything).Return(handlerServiceClient, nil)
	rb := mock_resolver.NewMockBuilder(t)
	rb.EXPECT().Close().Run(func() {})
	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Close().Run(func() {})

	p := mock_producer.NewMockProducer(t)
	p.EXPECT().Register(mock.Anything).Return()
	p.EXPECT().Close().RunAndReturn(func() {})
	c := mock_consumer.NewMockConsumer(t)
	c.EXPECT().Close().RunAndReturn(func() error { return nil })

	rebalanceTrigger := mock_types.NewMockAssignmentRebalanceTrigger(t)
	rebalanceTrigger.EXPECT().ReportAssignmentError(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	pK := 0
	handler := &handlerClientImpl{
		lifetime:         typeutil.NewLifetime(),
		service:          service,
		rb:               rb,
		watcher:          w,
		rebalanceTrigger: rebalanceTrigger,
		newProducer: func(ctx context.Context, opts *producer.ProducerOptions, handler streamingpb.StreamingNodeHandlerServiceClient) (Producer, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			assert.True(t, ok)
			assert.Equal(t, serverID, assignment.Node.ServerID)
			if pK == 0 {
				pK++
				return nil, status.NewUnmatchedChannelTerm("pchannel", 1, 2)
			}
			return p, nil
		},
		newConsumer: func(ctx context.Context, opts *consumer.ConsumerOptions, handlerClient streamingpb.StreamingNodeHandlerServiceClient) (Consumer, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			assert.True(t, ok)
			assert.Equal(t, serverID, assignment.Node.ServerID)
			return c, nil
		},
	}
	ctx := context.Background()

	k := 0
	w.EXPECT().Get(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) *types.PChannelInfoAssigned {
		if k == 0 {
			k++
			return nil
		}
		return assignment
	})
	w.EXPECT().Watch(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	producer, err := handler.CreateProducer(ctx, &ProducerOptions{PChannel: "pchannel"})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	producer2, err := handler.CreateProducer(ctx, &ProducerOptions{PChannel: "pchannel"})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	producer3, err := handler.CreateProducer(ctx, &ProducerOptions{PChannel: "pchannel"})
	assert.NoError(t, err)
	assert.NotNil(t, producer3)
	producer.Close()
	producer2.Close()
	producer3.Close()

	rcp, err := handler.GetReplicateCheckpoint(ctx, "pchannel")
	assert.NoError(t, err)
	assert.NotNil(t, rcp)

	handler.GetLatestMVCCTimestampIfLocal(ctx, "pchannel")
	producer4, err := handler.CreateProducer(ctx, &ProducerOptions{PChannel: "pchannel"})
	assert.NoError(t, err)
	assert.NotNil(t, producer4)
	producer4.Close()

	consumer, err := handler.CreateConsumer(ctx, &ConsumerOptions{
		PChannel:      "pchannel",
		VChannel:      "vchannel",
		DeliverPolicy: options.DeliverPolicyAll(),
		DeliverFilters: []options.DeliverFilter{
			options.DeliverFilterTimeTickGT(10),
			options.DeliverFilterTimeTickGTE(10),
		},
		MessageHandler: make(adaptor.ChanMessageHandler),
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	consumer.Close()

	service.EXPECT().Close().Return()
	handler.Close()
	producer, err = handler.CreateProducer(ctx, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
	assert.Nil(t, producer)

	consumer, err = handler.CreateConsumer(ctx, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
	assert.Nil(t, consumer)

	handler.GetLatestMVCCTimestampIfLocal(ctx, "pchannel")
}

func TestHandlerClient_GetSalvageCheckpoint(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}

	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
	rb := mock_resolver.NewMockBuilder(t)
	rb.EXPECT().Close().Run(func() {})
	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Close().Run(func() {})
	// Always return the assignment so the create func is invoked.
	w.EXPECT().Get(mock.Anything, mock.Anything).Return(assignment)
	// Watch returns context.Canceled to break the backoff retry loop.
	w.EXPECT().Watch(mock.Anything, mock.Anything, mock.Anything).Return(context.Canceled)
	rebalanceTrigger := mock_types.NewMockAssignmentRebalanceTrigger(t)

	handler := &handlerClientImpl{
		lifetime:         typeutil.NewLifetime(),
		service:          service,
		rb:               rb,
		watcher:          w,
		rebalanceTrigger: rebalanceTrigger,
	}
	ctx := context.Background()

	// Remote WAL returns "not implemented"; Watch returns Canceled to exit the loop.
	service.EXPECT().GetService(mock.Anything).Return(nil, errors.New("not implemented"))
	cps, err := handler.GetSalvageCheckpoint(ctx, "pchannel")
	assert.Error(t, err)
	assert.Nil(t, cps)

	// After close, GetSalvageCheckpoint returns ErrClientClosed immediately.
	service.EXPECT().Close().Return()
	handler.Close()
	cps, err = handler.GetSalvageCheckpoint(ctx, "pchannel")
	assert.ErrorIs(t, err, ErrClientClosed)
	assert.Nil(t, cps)
}

func TestHandlerClientReadOnlyAssignmentWaitsForNextAssignment(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRO},
		Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}

	testcases := []struct {
		name string
		run  func(*handlerClientImpl) error
	}{
		{
			name: "replicate checkpoint",
			run: func(handler *handlerClientImpl) error {
				cp, err := handler.GetReplicateCheckpoint(context.Background(), "pchannel")
				assert.Nil(t, cp)
				return err
			},
		},
		{
			name: "salvage checkpoint",
			run: func(handler *handlerClientImpl) error {
				cps, err := handler.GetSalvageCheckpoint(context.Background(), "pchannel")
				assert.Nil(t, cps)
				return err
			},
		},
		{
			name: "producer",
			run: func(handler *handlerClientImpl) error {
				producer, err := handler.CreateProducer(context.Background(), &ProducerOptions{PChannel: "pchannel"})
				assert.Nil(t, producer)
				return err
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			w := mock_assignment.NewMockWatcher(t)
			w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)
			w.EXPECT().Watch(mock.Anything, "pchannel", assignment).Return(context.Canceled)

			handler := &handlerClientImpl{
				lifetime: typeutil.NewLifetime(),
				watcher:  w,
			}

			err := tc.run(handler)
			assert.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestHandlerClientCreateProducerUsesLocalWALReplica(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)
	handlerregistry.ResetRegisterLocalWALManager()
	t.Cleanup(resetLocalWALManagerToNoStreamingNodeDeployed)

	assignment := &types.PChannelInfoAssigned{
		Channel:      types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRW},
		WALReplicaID: 2,
		Node:         types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}
	localWAL := mock_wal.NewMockWAL(t)
	localWAL.EXPECT().Register(mock.Anything).Return()
	handlerregistry.RegisterLocalWALManager(handlerFakeWALManager{
		wal:          localWAL,
		walReplicaID: 2,
	})

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)
	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		service:  service,
		watcher:  w,
	}

	producer, err := handler.CreateProducer(context.Background(), &ProducerOptions{PChannel: "pchannel"})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
}

func TestHandlerClientGetLatestMVCCTimestampIfLocalUsesWALReplica(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)
	handlerregistry.ResetRegisterLocalWALManager()
	t.Cleanup(resetLocalWALManagerToNoStreamingNodeDeployed)

	assignment := &types.PChannelInfoAssigned{
		Channel:      types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRW},
		WALReplicaID: 2,
		Node:         types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}
	localWAL := mock_wal.NewMockWAL(t)
	localWAL.EXPECT().Channel().Return(assignment.Channel)
	localWAL.EXPECT().GetLatestMVCCTimestamp(mock.Anything, "pchannel_100v0").Return(uint64(100), nil)
	handlerregistry.RegisterLocalWALManager(handlerFakeWALManager{
		wal:          localWAL,
		walReplicaID: 2,
	})

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		watcher:  w,
	}

	ts, err := handler.GetLatestMVCCTimestampIfLocal(context.Background(), "pchannel_100v0")
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), ts)
}

func TestHandlerClientGetReplicateCheckpointUsesLocalWALReplica(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)
	handlerregistry.ResetRegisterLocalWALManager()
	t.Cleanup(resetLocalWALManagerToNoStreamingNodeDeployed)

	assignment := &types.PChannelInfoAssigned{
		Channel:      types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRW},
		WALReplicaID: 2,
		Node:         types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}
	checkpoint := &wal.ReplicateCheckpoint{
		ClusterID: "cluster",
		PChannel:  "pchannel",
		TimeTick:  100,
	}
	localWAL := mock_wal.NewMockWAL(t)
	localWAL.EXPECT().GetReplicateCheckpoint().Return(checkpoint, nil)
	handlerregistry.RegisterLocalWALManager(handlerFakeWALManager{
		wal:          localWAL,
		walReplicaID: 2,
	})

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)
	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		service:  service,
		watcher:  w,
	}

	actual, err := handler.GetReplicateCheckpoint(context.Background(), "pchannel")
	assert.NoError(t, err)
	assert.Same(t, checkpoint, actual)
}

func TestHandlerClientGetReplicateCheckpointSendsWALReplicaIDInRequest(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel:      types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRW},
		WALReplicaID: 2,
		Node:         types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}

	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
	handlerServiceClient := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	handlerServiceClient.EXPECT().GetReplicateCheckpoint(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *streamingpb.GetReplicateCheckpointRequest, opts ...grpc.CallOption) (*streamingpb.GetReplicateCheckpointResponse, error) {
			assert.Equal(t, int64(2), req.GetWalReplicaId())
			assert.Equal(t, "pchannel", req.GetPchannel().GetName())
			return &streamingpb.GetReplicateCheckpointResponse{
				Checkpoint: &commonpb.ReplicateCheckpoint{
					ClusterId: "cluster",
					Pchannel:  "pchannel",
					TimeTick:  100,
				},
			}, nil
		})
	service.EXPECT().GetService(mock.Anything).Return(handlerServiceClient, nil)

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		service:  service,
		watcher:  w,
	}

	checkpoint, err := handler.GetReplicateCheckpoint(context.Background(), "pchannel")
	assert.NoError(t, err)
	assert.NotNil(t, checkpoint)
	assert.Equal(t, uint64(100), checkpoint.TimeTick)
}

func TestHandlerClientGetSalvageCheckpointSendsWALReplicaIDInRequest(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel:      types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRW},
		WALReplicaID: 2,
		Node:         types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}

	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
	handlerServiceClient := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	handlerServiceClient.EXPECT().GetSalvageCheckpoint(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *streamingpb.GetSalvageCheckpointRequest, opts ...grpc.CallOption) (*streamingpb.GetSalvageCheckpointResponse, error) {
			assert.Equal(t, int64(2), req.GetWalReplicaId())
			assert.Equal(t, "pchannel", req.GetPchannel().GetName())
			return &streamingpb.GetSalvageCheckpointResponse{
				Checkpoints: []*commonpb.ReplicateCheckpoint{
					{
						ClusterId: "cluster",
						Pchannel:  "pchannel",
						TimeTick:  100,
					},
				},
			}, nil
		})
	service.EXPECT().GetService(mock.Anything).Return(handlerServiceClient, nil)

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		service:  service,
		watcher:  w,
	}

	checkpoints, err := handler.GetSalvageCheckpoint(context.Background(), "pchannel")
	assert.NoError(t, err)
	assert.Len(t, checkpoints, 1)
	assert.Equal(t, uint64(100), checkpoints[0].TimeTick)
}

func TestHandlerClient_PrepareReleaseManualFlushIfLocal(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRO},
		Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}
	vchannel := "pchannel_100v0"
	releaseSegmentIDs := []int64{1001}

	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
	service.EXPECT().Close().Return()

	rb := mock_resolver.NewMockBuilder(t)
	rb.EXPECT().Close().Run(func() {})
	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)
	w.EXPECT().Close().Run(func() {})

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		service:  service,
		rb:       rb,
		watcher:  w,
	}

	prepared, err := handler.PrepareReleaseManualFlushIfLocal(context.Background(), 100, vchannel, releaseSegmentIDs)
	assert.Error(t, err)
	assert.False(t, prepared)

	handler.Close()
	prepared, err = handler.PrepareReleaseManualFlushIfLocal(context.Background(), 100, vchannel, releaseSegmentIDs)
	assert.ErrorIs(t, err, ErrClientClosed)
	assert.False(t, prepared)
}

func TestHandlerClientPrepareReleaseManualFlushIfLocalUsesWALReplica(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)
	handlerregistry.ResetRegisterLocalWALManager()
	t.Cleanup(resetLocalWALManagerToNoStreamingNodeDeployed)

	assignment := &types.PChannelInfoAssigned{
		Channel:      types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRW},
		WALReplicaID: 2,
		Node:         types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}
	localWAL := mock_wal.NewMockWAL(t)
	localWAL.EXPECT().Channel().Return(assignment.Channel)
	handlerregistry.RegisterLocalWALManager(handlerFakeWALManager{
		wal:          localWAL,
		walReplicaID: 2,
	})
	preparer := &handlerFakeReleaseManualFlushPreparer{prepared: true}
	handlerregistry.RegisterLocalReleaseManualFlushPreparer(preparer)

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		watcher:  w,
	}

	prepared, err := handler.PrepareReleaseManualFlushIfLocal(context.Background(), 100, "pchannel_100v0", []int64{1001})
	assert.NoError(t, err)
	assert.True(t, prepared)
	assert.Equal(t, assignment.Channel, preparer.pchannel)
	assert.Equal(t, int64(2), preparer.walReplicaID)
}

func TestHandlerClient_PrepareReleaseManualFlushIfLocalReturnsLocalWALShutdown(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)
	handlerregistry.ResetRegisterLocalWALManager()
	t.Cleanup(resetLocalWALManagerToNoStreamingNodeDeployed)
	handlerregistry.RegisterLocalWALManager(shutdownWALManager{})

	assignment := &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRW},
		Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}
	vchannel := "pchannel_100v0"
	releaseSegmentIDs := []int64{1001}

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)
	// Watch is intentionally not expected: local WAL shutdown is returned to
	// the caller like GetLatestMVCCTimestampIfLocal, not retried on assignment.

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		watcher:  w,
	}

	prepared, err := handler.PrepareReleaseManualFlushIfLocal(context.Background(), 100, vchannel, releaseSegmentIDs)
	assert.True(t, status.AsStreamingError(err).IsOnShutdown())
	assert.False(t, prepared)
}

func TestHandlerClient_PrepareReleaseManualFlushIfLocalWaitsForAssignmentReady(t *testing.T) {
	vchannel := "pchannel_100v0"
	releaseSegmentIDs := []int64{1001}

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(nil)
	w.EXPECT().Watch(mock.Anything, "pchannel", (*types.PChannelInfoAssigned)(nil)).Return(context.Canceled)

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		watcher:  w,
	}

	prepared, err := handler.PrepareReleaseManualFlushIfLocal(context.Background(), 100, vchannel, releaseSegmentIDs)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, prepared)
}

func TestHandlerClientAcquireTransformLogStreamUsesLocalWAL(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)
	handlerregistry.ResetRegisterLocalWALManager()
	t.Cleanup(resetLocalWALManagerToNoStreamingNodeDeployed)

	assignment := &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}
	localStream := &handlerFakeTransformLogStream{done: make(chan struct{})}
	localManager := &handlerFakeTransformLogStreamManager{stream: localStream}
	localWAL := mock_wal.NewMockWAL(t)
	localWAL.EXPECT().Channel().Return(assignment.Channel).Maybe()
	localWAL.EXPECT().TransformLog().Return(localManager)
	handlerregistry.RegisterLocalWALManager(handlerFakeWALManager{wal: localWAL})

	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().GetWALReplica(mock.Anything, types.ChannelID{Name: "pchannel"}).Return(assignment)

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		service:  service,
		watcher:  w,
	}

	stream, err := handler.AcquireTransformLogStream(context.Background(), "pchannel")
	assert.NoError(t, err)
	assert.Same(t, localStream, stream)
	assert.Equal(t, 1, localManager.acquireCount)
}

func TestHandlerClientAcquireTransformLogStreamRoutesWALReplica(t *testing.T) {
	paramtable.Init()

	assignment := &types.PChannelInfoAssigned{
		Channel:      types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRO},
		WALReplicaID: 2,
		Node:         types.StreamingNodeInfo{ServerID: 2, Address: "localhost"},
	}
	localStream := &handlerFakeTransformLogStream{done: make(chan struct{})}

	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
	handlerServiceClient := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	service.EXPECT().GetService(mock.Anything).Return(handlerServiceClient, nil)
	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().GetWALReplica(mock.Anything, types.ChannelID{Name: "pchannel", WALReplicaID: 2}).Return(assignment)

	handler := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		service:  service,
		watcher:  w,
		newTransformLogEventStream: func(ctx context.Context, opts *transformlogclient.EventStreamOptions, handlerClient streamingpb.StreamingNodeHandlerServiceClient) (*transformlogclient.EventStream, error) {
			assert.Equal(t, assignment, opts.Assignment)
			assert.Equal(t, int64(2), opts.Assignment.WALReplicaID)
			assert.Equal(t, handlerServiceClient, handlerClient)
			return &transformlogclient.EventStream{}, nil
		},
	}

	stream, err := handler.AcquireTransformLogStream(context.Background(), "pchannel", 2)
	assert.NoError(t, err)
	assert.NotNil(t, stream)
	assert.NotSame(t, localStream, stream)
}

func TestHandlerClientReportsWALReplicaAssignmentError(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel:         types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRO},
		WALReplicaID:    2,
		AssignmentEpoch: 7,
		Node:            types.StreamingNodeInfo{ServerID: 2, Address: "localhost"},
	}
	channelID := types.ChannelID{Name: "pchannel", WALReplicaID: 2}

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().GetWALReplica(mock.Anything, channelID).Return(assignment)
	w.EXPECT().WatchWALReplica(mock.Anything, channelID, assignment).Return(context.Canceled)

	trigger := &handlerFakeWALReplicaRebalanceTrigger{}
	handler := &handlerClientImpl{
		lifetime:         typeutil.NewLifetime(),
		watcher:          w,
		rebalanceTrigger: trigger,
	}

	_, err := handler.createHandlerAfterWALReplicaReady(
		context.Background(),
		mlog.With(),
		channelID,
		func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
			return nil, status.NewChannelNotExist("pchannel")
		},
	)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, trigger.reportedPChannelAssignment)
	assert.Equal(t, assignment, trigger.reportedWALAssignment)
}

func TestHandlerClientReportsReadWriteWALReplicaErrorWithReplicaIdentity(t *testing.T) {
	assignment := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{
			Name:       "pchannel",
			Term:       11,
			AccessMode: types.AccessModeRW,
		},
		WALReplicaID:    2,
		AssignmentEpoch: 7,
		Node:            types.StreamingNodeInfo{ServerID: 2, Address: "localhost"},
	}
	trigger := &handlerFakeWALReplicaRebalanceTrigger{}
	handler := &handlerClientImpl{
		rebalanceTrigger: trigger,
	}

	err := handler.reportWALReplicaAssignmentError(context.Background(), assignment, status.NewChannelNotExist("pchannel"))

	assert.NoError(t, err)
	assert.Nil(t, trigger.reportedPChannelAssignment)
	assert.Equal(t, &assignment, trigger.reportedWALAssignment)
}

func TestHandlerClientStreamingNodeReadyReportsReadWriteWALReplicaErrorWithReplicaIdentity(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{
			Name:       "pchannel",
			Term:       11,
			AccessMode: types.AccessModeRW,
		},
		WALReplicaID:    2,
		AssignmentEpoch: 7,
		Node:            types.StreamingNodeInfo{ServerID: 2, Address: "localhost"},
	}

	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Get(mock.Anything, "pchannel").Return(assignment)
	w.EXPECT().Watch(mock.Anything, "pchannel", assignment).Return(context.Canceled)

	trigger := &handlerFakeWALReplicaRebalanceTrigger{}
	handler := &handlerClientImpl{
		lifetime:         typeutil.NewLifetime(),
		watcher:          w,
		rebalanceTrigger: trigger,
	}

	_, err := handler.createHandlerAfterStreamingNodeReady(
		context.Background(),
		mlog.With(),
		"pchannel",
		func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
			return nil, status.NewChannelNotExist("pchannel")
		},
	)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, trigger.reportedPChannelAssignment)
	assert.Equal(t, assignment, trigger.reportedWALAssignment)
}

func resetLocalWALManagerToNoStreamingNodeDeployed() {
	handlerregistry.ResetRegisterLocalWALManager()
	handlerregistry.RegisterLocalWALManager(handlerFakeWALManager{err: handlerregistry.ErrNoStreamingNodeDeployed})
}

func TestHandlerClient_GetReplicateCheckpointReplicateViolation(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}

	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
	handlerServiceClient := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	// Remote WAL reports a replicate violation: the target is no longer a secondary
	// cluster (e.g. after force_promote). This is unrecoverable for the current WAL
	// role, so it must be returned immediately rather than retried to the deadline.
	handlerServiceClient.EXPECT().GetReplicateCheckpoint(mock.Anything, mock.Anything).Return(
		nil, status.NewReplicateViolation("wal is not a secondary cluster in replicating topology"))
	service.EXPECT().GetService(mock.Anything).Return(handlerServiceClient, nil)

	w := mock_assignment.NewMockWatcher(t)
	// Always return the assignment so the create func is invoked.
	w.EXPECT().Get(mock.Anything, mock.Anything).Return(assignment)
	// Watch is intentionally NOT expected: an immediate return must not enter the
	// backoff retry loop. If it did, the mock would fail on an unexpected Watch call.
	rebalanceTrigger := mock_types.NewMockAssignmentRebalanceTrigger(t)

	handler := &handlerClientImpl{
		lifetime:         typeutil.NewLifetime(),
		service:          service,
		watcher:          w,
		rebalanceTrigger: rebalanceTrigger,
	}

	cp, err := handler.GetReplicateCheckpoint(context.Background(), "pchannel")
	assert.Error(t, err)
	assert.Nil(t, cp)
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
}

func TestDial(t *testing.T) {
	paramtable.Init()

	w := mock_types.NewMockAssignmentDiscoverWatcher(t)
	w.EXPECT().AssignmentDiscover(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, f func(*types.VersionedStreamingNodeAssignments) error) error {
			return context.Canceled
		},
	)
	handler := NewHandlerClient(w)
	assert.NotNil(t, handler)
	time.Sleep(100 * time.Millisecond)
	handler.Close()
}

type handlerFakeSubscribeTransformClient struct {
	ctx     context.Context
	sendCh  chan *streamingpb.TransformRequest
	recvCh  chan *streamingpb.TransformResponse
	closeCh chan struct{}
	once    sync.Once
}

func newHandlerFakeSubscribeTransformClient(ctx context.Context) *handlerFakeSubscribeTransformClient {
	return &handlerFakeSubscribeTransformClient{
		ctx:     ctx,
		sendCh:  make(chan *streamingpb.TransformRequest, 16),
		recvCh:  make(chan *streamingpb.TransformResponse, 16),
		closeCh: make(chan struct{}),
	}
}

func (f *handlerFakeSubscribeTransformClient) Send(req *streamingpb.TransformRequest) error {
	select {
	case f.sendCh <- req:
		return nil
	case <-f.closeCh:
		return io.EOF
	}
}

func (f *handlerFakeSubscribeTransformClient) Recv() (*streamingpb.TransformResponse, error) {
	select {
	case resp := <-f.recvCh:
		return resp, nil
	case <-f.closeCh:
		return nil, io.EOF
	}
}

func (f *handlerFakeSubscribeTransformClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (f *handlerFakeSubscribeTransformClient) Trailer() metadata.MD {
	return nil
}

func (f *handlerFakeSubscribeTransformClient) CloseSend() error {
	f.once.Do(func() {
		close(f.closeCh)
	})
	return nil
}

func (f *handlerFakeSubscribeTransformClient) Context() context.Context {
	return f.ctx
}

func (f *handlerFakeSubscribeTransformClient) SendMsg(m interface{}) error {
	return nil
}

func (f *handlerFakeSubscribeTransformClient) RecvMsg(m interface{}) error {
	return nil
}

var (
	_ streamingpb.StreamingNodeHandlerService_SubscribeTransformClient = (*handlerFakeSubscribeTransformClient)(nil)
	_ grpc.ClientStream                                                = (*handlerFakeSubscribeTransformClient)(nil)
)

type handlerFakeWALManager struct {
	wal          wal.WAL
	err          error
	walReplicaID int64
}

func (m handlerFakeWALManager) GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	return m.GetAvailableWALReplica(channel, 0)
}

func (m handlerFakeWALManager) GetAvailableWALReplica(_ types.PChannelInfo, walReplicaID int64) (wal.WAL, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.walReplicaID != walReplicaID {
		return nil, handlerregistry.ErrNoStreamingNodeDeployed
	}
	return m.wal, nil
}

func (m handlerFakeWALManager) Metrics() (*types.StreamingNodeMetrics, error) {
	return &types.StreamingNodeMetrics{}, nil
}

type handlerFakeReleaseManualFlushPreparer struct {
	pchannel     types.PChannelInfo
	walReplicaID int64
	prepared     bool
}

func (p *handlerFakeReleaseManualFlushPreparer) PrepareReleaseManualFlush(ctx context.Context, pchannel types.PChannelInfo, walReplicaID int64, collectionID int64, vchannel string, releaseSegmentIDs []int64) (bool, error) {
	p.pchannel = pchannel
	p.walReplicaID = walReplicaID
	return p.prepared, nil
}

type handlerFakeTransformLogStreamManager struct {
	stream       wal.TransformLogStream
	acquireCount int
}

func (m *handlerFakeTransformLogStreamManager) AcquireStream(context.Context, string, int64) (wal.TransformLogStream, error) {
	m.acquireCount++
	return m.stream, nil
}

type handlerFakeTransformLogStream struct {
	done chan struct{}
}

func (s *handlerFakeTransformLogStream) Subscribe(context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	return nil, errors.New("not implemented")
}

func (s *handlerFakeTransformLogStream) Done() <-chan struct{} {
	return s.done
}

func (s *handlerFakeTransformLogStream) Error() error {
	return nil
}

func (s *handlerFakeTransformLogStream) Close() error {
	return nil
}
