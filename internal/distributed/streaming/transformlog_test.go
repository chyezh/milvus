//go:build test && dynamic

package streaming

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestTransformLogStreamManagerPassesWALReplicaID(t *testing.T) {
	underlying := &distributedTestTransformLogStream{}
	handlerClient := &distributedTestHandlerClient{transformLogStream: underlying}

	manager := (&walAccesserImpl{
		lifetime:      typeutil.NewLifetime(),
		handlerClient: handlerClient,
	}).TransformLogStreamManager()
	stream, err := manager.AcquireStream(context.Background(), "p0", 7)
	require.NoError(t, err)
	defer stream.Close()

	sub, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{
		VChannel: "p0_1v0",
		Handler:  nopTransformLogEventHandler{},
	})
	require.NoError(t, err)
	require.NotNil(t, sub)
	require.Equal(t, "p0", handlerClient.acquireTransformLogPChannel)
	require.Equal(t, []int64{7}, handlerClient.acquireTransformLogWALReplicaIDs)
}

type distributedTestHandlerClient struct {
	transformLogStream               wal.TransformLogStream
	acquireTransformLogPChannel      string
	acquireTransformLogWALReplicaIDs []int64
}

func (c *distributedTestHandlerClient) GetLatestMVCCTimestampIfLocal(context.Context, string) (uint64, error) {
	return 0, nil
}

func (c *distributedTestHandlerClient) GetReplicateCheckpoint(context.Context, string) (*wal.ReplicateCheckpoint, error) {
	return nil, nil
}

func (c *distributedTestHandlerClient) GetSalvageCheckpoint(context.Context, string) ([]*wal.ReplicateCheckpoint, error) {
	return nil, nil
}

func (c *distributedTestHandlerClient) PrepareReleaseManualFlushIfLocal(context.Context, int64, string, []int64) (bool, error) {
	return false, nil
}

func (c *distributedTestHandlerClient) GetWALMetricsIfLocal(context.Context) (*types.StreamingNodeMetrics, error) {
	return nil, nil
}

func (c *distributedTestHandlerClient) CreateProducer(context.Context, *handler.ProducerOptions) (handler.Producer, error) {
	return nil, nil
}

func (c *distributedTestHandlerClient) CreateConsumer(context.Context, *handler.ConsumerOptions) (handler.Consumer, error) {
	return nil, nil
}

func (c *distributedTestHandlerClient) AcquireTransformLogStream(_ context.Context, pchannel string, walReplicaIDs ...int64) (wal.TransformLogStream, error) {
	c.acquireTransformLogPChannel = pchannel
	c.acquireTransformLogWALReplicaIDs = append([]int64(nil), walReplicaIDs...)
	return c.transformLogStream, nil
}

func (c *distributedTestHandlerClient) QueryViewClient() handler.QueryViewClient {
	return nil
}

func (c *distributedTestHandlerClient) QueryViewSyncClient() handler.QueryViewSyncClient {
	return nil
}

func (c *distributedTestHandlerClient) Close() {}

type distributedTestTransformLogStream struct{}

func (s *distributedTestTransformLogStream) Subscribe(_ context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	return distributedTestTransformLogSubscription{id: opt.SubscriptionID, vchannel: opt.VChannel}, nil
}

func (s *distributedTestTransformLogStream) Done() <-chan struct{} {
	return nil
}

func (s *distributedTestTransformLogStream) Error() error {
	return nil
}

func (s *distributedTestTransformLogStream) Close() error {
	return nil
}

type distributedTestTransformLogSubscription struct {
	id       int64
	vchannel string
}

func (s distributedTestTransformLogSubscription) ID() int64 {
	return s.id
}

func (s distributedTestTransformLogSubscription) VChannel() string {
	return s.vchannel
}

func (s distributedTestTransformLogSubscription) Close() error {
	return nil
}

type nopTransformLogEventHandler struct{}

func (nopTransformLogEventHandler) Handle(wal.TransformLogStreamEvent) error {
	return nil
}

func (nopTransformLogEventHandler) Close() {}
