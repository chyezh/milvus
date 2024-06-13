package timestamp

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

func TestTimestampAllocator(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	client := NewMockRootCoordClient(t)
	allocator := NewAllocator(client)

	for i := 0; i < 5000; i++ {
		ts, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.NotZero(t, ts)
	}

	for i := 0; i < 100; i++ {
		ts, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.NotZero(t, ts)
		time.Sleep(time.Millisecond * 1)
		allocator.Sync()
	}

	// error test
	client.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Unset()
	client.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, atr *rootcoordpb.AllocTimestampRequest, co ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
			return &rootcoordpb.AllocTimestampResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_ForceDeny,
				},
			}, nil
		},
	)
	allocator = NewAllocator(client)
	_, err := allocator.Allocate(context.Background())
	assert.Error(t, err)
}
