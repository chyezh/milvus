package transformlog

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestCreateSubscribeServerUsesWALReplicaID(t *testing.T) {
	ctx := contextutil.WithCreateTransformStream(context.Background(), &streamingpb.CreateTransformStreamRequest{
		Pchannel: &streamingpb.PChannelInfo{
			Name:       "pchannel",
			Term:       1,
			AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
		},
		WalReplicaId: 2,
	})
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	logStream := newFakeTransformLogStream()
	walManager := &fakeCreateSubscribeWALManager{logStream: logStream}
	stream := newFakeSubscribeTransformServer(metadata.NewIncomingContext(context.Background(), md))

	server, err := CreateSubscribeServer(walManager, stream)
	require.NoError(t, err)
	require.Same(t, logStream, server.logStream)
	require.Equal(t, types.PChannelInfo{Name: "pchannel", Term: 1, AccessMode: types.AccessModeRO}, walManager.channel)
	require.Equal(t, int64(2), walManager.walReplicaID)
}

func TestSubscribeServerMultiplexesSubscriptionsOnSingleTransformStream(t *testing.T) {
	ctx := contextutil.WithCreateTransformStream(context.Background(), &streamingpb.CreateTransformStreamRequest{
		Pchannel: &streamingpb.PChannelInfo{
			Name:       "pchannel",
			Term:       1,
			AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
		},
		WalReplicaId: 2,
	})
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	logStream := newFakeTransformLogStream()
	walManager := &fakeCreateSubscribeWALManager{logStream: logStream}
	stream := newFakeSubscribeTransformServer(metadata.NewIncomingContext(context.Background(), md))
	server, err := CreateSubscribeServer(walManager, stream)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Execute()
	}()

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_Create{
			Create: &streamingpb.CreateTransformSubscriptionRequest{
				SubscriptionId:     10,
				Vchannel:           "pchannel_100v0",
				StartAfterTimeTick: 100,
			},
		},
	})
	createResp := stream.sent(t)
	require.Equal(t, int64(10), createResp.GetCreate().GetSubscriptionId())

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_Create{
			Create: &streamingpb.CreateTransformSubscriptionRequest{
				SubscriptionId:     11,
				Vchannel:           "pchannel_101v0",
				StartAfterTimeTick: 200,
			},
		},
	})
	createResp = stream.sent(t)
	require.Equal(t, int64(11), createResp.GetCreate().GetSubscriptionId())

	require.Equal(t, 1, walManager.acquireCount)
	require.Equal(t, "pchannel", walManager.acquirePChannel)
	require.Equal(t, int64(2), walManager.acquireWALReplicaID)
	require.Equal(t, "pchannel_100v0", logStream.subscription(10).VChannel())
	require.Equal(t, "pchannel_101v0", logStream.subscription(11).VChannel())

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_CloseStream{
			CloseStream: &streamingpb.CloseTransformStreamRequest{},
		},
	})
	require.NotNil(t, stream.sent(t).GetCloseStream())
	require.NoError(t, <-errCh)
}

func TestSubscribeServerEncodesUnavailableTransformLogAsChannelNotExist(t *testing.T) {
	stream := newFakeSubscribeTransformServer(context.Background())
	server := &SubscribeServer{stream: stream}

	err := server.sendSubscriptionError(10, "pchannel_100v0", wal.ErrTransformLogVChannelUnavailable)

	require.NoError(t, err)
	resp := stream.sent(t)
	subErr := resp.GetSubscriptionError()
	require.NotNil(t, subErr)
	require.Equal(t, int64(10), subErr.GetSubscriptionId())
	require.Equal(t, "pchannel_100v0", subErr.GetVchannel())
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST, subErr.GetError().GetCode())
}

func TestSubscribeServerCloseSubscriptionAck(t *testing.T) {
	stream := newFakeSubscribeTransformServer(context.Background())
	logStream := newFakeTransformLogStream()
	server := &SubscribeServer{
		logStream: logStream,
		stream:    stream,
		subs:      make(map[int64]wal.TransformLogSubscription),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Execute()
	}()

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_Create{
			Create: &streamingpb.CreateTransformSubscriptionRequest{
				SubscriptionId:     10,
				Vchannel:           "v1",
				StartAfterTimeTick: 100,
			},
		},
	})
	createResp := stream.sent(t)
	require.Equal(t, int64(10), createResp.GetCreate().GetSubscriptionId())

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_CloseSubscription{
			CloseSubscription: &streamingpb.CloseTransformSubscriptionRequest{SubscriptionId: 10},
		},
	})
	closeResp := stream.sent(t)
	require.Equal(t, int64(10), closeResp.GetCloseSubscription().GetSubscriptionId())
	require.Equal(t, "v1", closeResp.GetCloseSubscription().GetVchannel())
	require.True(t, logStream.subscription(10).closed)

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_CloseStream{
			CloseStream: &streamingpb.CloseTransformStreamRequest{},
		},
	})
	closeStreamResp := stream.sent(t)
	require.NotNil(t, closeStreamResp.GetCloseStream())
	require.NoError(t, <-errCh)
}

func TestSubscribeServerClosesLogStreamOnCreateSendError(t *testing.T) {
	stream := newFakeSubscribeTransformServer(context.Background())
	stream.sendErr = io.ErrClosedPipe
	logStream := newFakeTransformLogStream()
	server := &SubscribeServer{
		logStream: logStream,
		stream:    stream,
		subs:      make(map[int64]wal.TransformLogSubscription),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Execute()
	}()

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_Create{
			Create: &streamingpb.CreateTransformSubscriptionRequest{
				SubscriptionId:     10,
				Vchannel:           "v1",
				StartAfterTimeTick: 100,
			},
		},
	})

	require.ErrorIs(t, <-errCh, io.ErrClosedPipe)
	require.True(t, logStream.closed)
	require.True(t, logStream.subscription(10).closed)
}

func TestSubscribeServerExitsWhenLogStreamCloses(t *testing.T) {
	stream := newFakeSubscribeTransformServer(context.Background())
	logStream := newFakeTransformLogStream()
	server := &SubscribeServer{
		logStream: logStream,
		stream:    stream,
		subs:      make(map[int64]wal.TransformLogSubscription),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Execute()
	}()

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_Create{
			Create: &streamingpb.CreateTransformSubscriptionRequest{
				SubscriptionId:     10,
				Vchannel:           "v1",
				StartAfterTimeTick: 100,
			},
		},
	})
	createResp := stream.sent(t)
	require.Equal(t, int64(10), createResp.GetCreate().GetSubscriptionId())

	logStream.finish(nil)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("subscribe server did not exit after log stream closed")
	}
	require.True(t, logStream.closed)
	require.True(t, logStream.subscription(10).closed)
}

type fakeCreateSubscribeWALManager struct {
	walmanager.Manager

	channel      types.PChannelInfo
	walReplicaID int64
	logStream    wal.TransformLogStream

	acquireCount        int
	acquirePChannel     string
	acquireWALReplicaID int64
}

func (m *fakeCreateSubscribeWALManager) GetAvailableWALReplica(channel types.PChannelInfo, walReplicaID int64) (wal.WAL, error) {
	m.channel = channel
	m.walReplicaID = walReplicaID
	return fakeCreateSubscribeWAL{manager: m}, nil
}

type fakeCreateSubscribeWAL struct {
	wal.WAL

	manager *fakeCreateSubscribeWALManager
}

func (w fakeCreateSubscribeWAL) TransformLog() wal.TransformLogAccesser {
	return fakeTransformLogAccesser{manager: w.manager}
}

type fakeTransformLogAccesser struct {
	manager *fakeCreateSubscribeWALManager
}

func (a fakeTransformLogAccesser) AcquireStream(_ context.Context, pchannel string, walReplicaID int64) (wal.TransformLogStream, error) {
	a.manager.acquireCount++
	a.manager.acquirePChannel = pchannel
	a.manager.acquireWALReplicaID = walReplicaID
	return a.manager.logStream, nil
}

type fakeTransformLogStream struct {
	mu        sync.Mutex
	subs      map[int64]*fakeTransformLogSubscription
	done      chan struct{}
	err       error
	closed    bool
	closeOnce sync.Once
}

func newFakeTransformLogStream() *fakeTransformLogStream {
	return &fakeTransformLogStream{
		subs: make(map[int64]*fakeTransformLogSubscription),
		done: make(chan struct{}),
	}
}

func (s *fakeTransformLogStream) Subscribe(_ context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	sub := &fakeTransformLogSubscription{
		id:       opt.SubscriptionID,
		vchannel: opt.VChannel,
	}
	s.mu.Lock()
	s.subs[sub.id] = sub
	s.mu.Unlock()
	return sub, nil
}

func (s *fakeTransformLogStream) Done() <-chan struct{} {
	return s.done
}

func (s *fakeTransformLogStream) Error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *fakeTransformLogStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.finish(nil)
	return s.Error()
}

func (s *fakeTransformLogStream) finish(err error) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		close(s.done)
	})
}

func (s *fakeTransformLogStream) subscription(id int64) *fakeTransformLogSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subs[id]
}

type fakeTransformLogSubscription struct {
	id       int64
	vchannel string
	closed   bool
}

func (s *fakeTransformLogSubscription) ID() int64 {
	return s.id
}

func (s *fakeTransformLogSubscription) VChannel() string {
	return s.vchannel
}

func (s *fakeTransformLogSubscription) Close() error {
	s.closed = true
	return nil
}

type fakeSubscribeTransformServer struct {
	ctx     context.Context
	recvCh  chan *streamingpb.TransformRequest
	sendCh  chan *streamingpb.TransformResponse
	sendErr error
}

func newFakeSubscribeTransformServer(ctx context.Context) *fakeSubscribeTransformServer {
	return &fakeSubscribeTransformServer{
		ctx:    ctx,
		recvCh: make(chan *streamingpb.TransformRequest, 16),
		sendCh: make(chan *streamingpb.TransformResponse, 16),
	}
}

func (s *fakeSubscribeTransformServer) recv(req *streamingpb.TransformRequest) {
	s.recvCh <- req
}

func (s *fakeSubscribeTransformServer) sent(t *testing.T) *streamingpb.TransformResponse {
	t.Helper()
	select {
	case resp := <-s.sendCh:
		return resp
	case <-time.After(time.Second):
		t.Fatal("timeout waiting response")
		return nil
	}
}

func (s *fakeSubscribeTransformServer) Send(resp *streamingpb.TransformResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sendCh <- resp
	return nil
}

func (s *fakeSubscribeTransformServer) Recv() (*streamingpb.TransformRequest, error) {
	req, ok := <-s.recvCh
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

func (s *fakeSubscribeTransformServer) SetHeader(metadata.MD) error {
	return nil
}

func (s *fakeSubscribeTransformServer) SendHeader(metadata.MD) error {
	return nil
}

func (s *fakeSubscribeTransformServer) SetTrailer(metadata.MD) {
}

func (s *fakeSubscribeTransformServer) Context() context.Context {
	return s.ctx
}

func (s *fakeSubscribeTransformServer) SendMsg(m interface{}) error {
	return nil
}

func (s *fakeSubscribeTransformServer) RecvMsg(m interface{}) error {
	return nil
}

var _ streamingpb.StreamingNodeHandlerService_SubscribeTransformServer = (*fakeSubscribeTransformServer)(nil)
