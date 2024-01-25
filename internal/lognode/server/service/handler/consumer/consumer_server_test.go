package consumer

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	mock_wal "github.com/milvus-io/milvus/internal/mocks/lognode/server/wal"
	mock_walmanager "github.com/milvus-io/milvus/internal/mocks/lognode/server/walmanager"
	mock_logpb "github.com/milvus-io/milvus/internal/mocks/proto/logpb"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"
)

func TestCreateConsumerServer(t *testing.T) {
	manager := mock_walmanager.NewMockManager(t)
	grpcConsumerServer := mock_logpb.NewMockLogNodeHandlerService_ConsumeServer(t)

	// No metadata in context should report error
	grpcConsumerServer.EXPECT().Context().Return(context.Background())
	assertCreateConsumerServerFail(t, manager, grpcConsumerServer)

	// wal not exist should report error.
	meta, _ := metadata.FromOutgoingContext(contextutil.WithCreateConsumer(context.Background(), &logpb.CreateConsumerRequest{
		ChannelName:   "test",
		Term:          1,
		DeliverPolicy: logpb.NewDeliverAll(),
	}))
	ctx := metadata.NewIncomingContext(context.Background(), meta)
	grpcConsumerServer.ExpectedCalls = nil
	grpcConsumerServer.EXPECT().Context().Return(ctx)
	manager.EXPECT().GetAvailableWAL("test", int64(1)).Return(nil, errors.New("wal not exist"))
	assertCreateConsumerServerFail(t, manager, grpcConsumerServer)

	// Return error if create scanner failed.
	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(nil, errors.New("create scanner failed"))
	manager.ExpectedCalls = nil
	manager.EXPECT().GetAvailableWAL("test", int64(1)).Return(l, nil)
	assertCreateConsumerServerFail(t, manager, grpcConsumerServer)

	// Return error if send created failed.
	grpcConsumerServer.EXPECT().Send(mock.Anything).Return(errors.New("send created failed"))
	l.ExpectedCalls = nil
	s := mock_wal.NewMockScanner(t)
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(s, nil)
	assertCreateConsumerServerFail(t, manager, grpcConsumerServer)

	// Passed.
	grpcConsumerServer.EXPECT().Send(mock.Anything).Unset()
	grpcConsumerServer.EXPECT().Send(mock.Anything).Return(nil)

	l.EXPECT().Channel().Return(&logpb.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	server, err := CreateConsumeServer(manager, grpcConsumerServer)
	assert.NoError(t, err)
	assert.NotNil(t, server)
}

func TestConsumerServerExecute(t *testing.T) {
	grpcConsumerServer := mock_logpb.NewMockLogNodeHandlerService_ConsumeServer(t)
	server := &ConsumeServer{
		grpcStreamServerHelper: &consumeGrpcServerHelper{
			LogNodeHandlerService_ConsumeServer: grpcConsumerServer,
		},
		logger:           log.With(),
		cancelConsumerCh: make(chan struct{}),
	}
	recvCh := make(chan *logpb.ConsumeRequest)
	grpcConsumerServer.EXPECT().Recv().RunAndReturn(func() (*logpb.ConsumeRequest, error) {
		req, ok := <-recvCh
		if ok {
			return req, nil
		}
		return nil, io.EOF
	})

	// Test recv arm
	ch := make(chan error)
	go func() {
		ch <- server.recvLoop()
	}()

	// should be blocked.
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)
	testChannelShouldBeBlocked(t, server.cancelConsumerCh, 500*time.Millisecond)

	// cancelConsumerCh should be closed after receiving close request.
	recvCh <- &logpb.ConsumeRequest{
		Request: &logpb.ConsumeRequest_Close{},
	}
	<-server.cancelConsumerCh
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)

	close(recvCh)
	assert.NoError(t, <-ch)
}

func assertCreateConsumerServerFail(t *testing.T, manager walmanager.Manager, grpcConsumerServer logpb.LogNodeHandlerService_ConsumeServer) {
	server, err := CreateConsumeServer(manager, grpcConsumerServer)
	assert.Nil(t, server)
	assert.Error(t, err)
}

func testChannelShouldBeBlocked[T any](t *testing.T, ch <-chan T, d time.Duration) {
	// should be blocked.
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	select {
	case _ = <-ch:
		t.Errorf("should be block")
	case <-ctx.Done():
	}
}
