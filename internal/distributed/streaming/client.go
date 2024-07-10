package streaming

import (
	"github.com/milvus-io/milvus/internal/distributed/streaming/consumer"
	"github.com/milvus-io/milvus/internal/distributed/streaming/producer"
	"github.com/milvus-io/milvus/internal/streamingcoord/client"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var _ Client = (*clientImpl)(nil)

type (
	Producer = producer.ResumableProducer
	Consumer = consumer.ResumableConsumer
)

// Client is the interface of streamingservice client.
// Client can be used to create new producer and new consumer to interact with streamingservice.
type Client interface {
	// CreateProducer creates a new producer.
	// 1. Producer is running on a grpc stream.
	// 2. Producer of same PChannel will be shared by different instance.
	//    In antoer word, A producer of a pchannel only hold a grpc stream.
	// 3. Producer will preform auto reconnection when connection is broken.
	//    So when streaming node is down, producer will try to reconnect to another streaming node, and send the message again.
	//    Duplicate message writen will be generated in current implementation.
	CreateProducer(opts *options.ProducerOptions) Producer

	// CreateConsumer creates a consumer.
	// 1. Consumer is runing on a grpc stream.
	// 2. Consumer will consume messages from the specified consume option.
	// 3. Current implementation, one Consumer will always ocuppy one grpc-stream.
	//    So huge count of grpc stream will be generated if there's many vchannel on pchannel.
	// TODO: Optimize the consumer to share the underlying grpc stream.
	// Perform a server dispatching and client dispatching together.
	CreateConsumer(opts *options.ConsumerOptions) Consumer

	// Close closes the handler client.
	// Close will also stop all producer and consumer.
	Close()
}

// NewClient create a new streamingservice client.
func NewClient(etcdCli *clientv3.Client) Client {
	// Create a new streaming coord client.
	streamingCoordClient := client.NewClient(etcdCli)
	// Create a new streamingnode handler client.
	handlerClient := handler.NewHandlerClient(streamingCoordClient.Assignment())
	return &clientImpl{
		streamingCoordAssignmentClient: streamingCoordClient,
		handlerClient:                  handlerClient,
	}
}
