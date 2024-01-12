package consumer

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	mock_walmanager "github.com/milvus-io/milvus/internal/mocks/lognode/server/walmanager"
	mock_logpb "github.com/milvus-io/milvus/internal/mocks/proto/logpb"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestCreateConsumerServer(t *testing.T) {
	manager := mock_walmanager.NewMockManager(t)
	grpcConsumerServer := mock_logpb.NewMockLogNodeHandlerService_ConsumeServer(t)

	// No metadata in context should report error
	grpcConsumerServer.EXPECT().Context().Return(context.Background())
	assertCreateConsumerServerFail(t, manager, grpcConsumerServer)

	// No create consumer information in metadata should report error
	meta, _ := metadata.FromOutgoingContext(contextutil.WithCreateConsumer(context.Background(), &logpb.CreateConsumerRequest{
		ChannelName:   "test",
		Term:          1,
		DeliverPolicy: logpb.NewDeliverAll(),
	}))
	ctx := metadata.NewIncomingContext(context.Background(), meta)
	grpcConsumerServer.ExpectedCalls = nil
	grpcConsumerServer.EXPECT().Context().Return(ctx)
	assertCreateConsumerServerFail(t, manager, grpcConsumerServer)
}

func assertCreateConsumerServerFail(t *testing.T, manager walmanager.Manager, grpcConsumerServer logpb.LogNodeHandlerService_ConsumeServer) {
	server, err := CreateConsumeServer(manager, grpcConsumerServer)
	assert.Nil(t, server)
	assert.Error(t, err)
}
