package coord

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus/pkg/v2/json"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/client/coord/assignment"
	"github.com/milvus-io/milvus/pkg/v2/streaming/client/coord/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/streaming/client/internal/util"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/service/balancer/picker"
	streamingserviceinterceptor "github.com/milvus-io/milvus/pkg/v2/streaming/util/service/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/service/resolver"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/tracer"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ Client = (*clientImpl)(nil)

// AssignmentService is the interface of assignment service.
type AssignmentService interface {
	// AssignmentDiscover is used to watches the assignment discovery.
	types.AssignmentDiscoverWatcher
}

// BroadcastService is the interface of broadcast service.
type BroadcastService interface {
	// Broadcast sends a broadcast message to the streaming service.
	Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error)

	// Ack sends a broadcast ack to the streaming service.
	Ack(ctx context.Context, req types.BroadcastAckRequest) error
}

// Client is the interface of log service client.
type Client interface {
	// Broadcast access broadcast service.
	// Broadcast service will always be available.
	// When streaming service is enabled, the broadcast will use the streaming service.
	// When streaming service is disabled, the broadcast will use legacy msgstream.
	Broadcast() BroadcastService

	// Assignment access assignment service.
	// Assignment service will only be available when streaming service is enabled.
	Assignment() AssignmentService

	// Close close the client.
	Close()
}

type Config = util.Config

// NewClient creates a new client.
func NewClient(cfg *Config) (Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// StreamingCoord is deployed on DataCoord node.
	role := sessionutil.GetSessionPrefixByRoleWithRootPath(cfg.RootPath, typeutil.MixCoordRole)
	rb := resolver.NewSessionExclusiveBuilder(cfg.ETCDClient, role, ">=2.6.0-dev")
	dialOptions := getDialOptions(rb, cfg)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
		defer cancel()
		return grpc.DialContext(
			ctx,
			resolver.SessionResolverScheme+":///"+typeutil.MixCoordRole,
			dialOptions...,
		)
	})
	assignmentService := lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingCoordAssignmentServiceClient)
	assignmentServiceImpl := assignment.NewAssignmentService(assignmentService)
	broadcastService := lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingCoordBroadcastServiceClient)
	return &clientImpl{
		conn:              conn,
		rb:                rb,
		assignmentService: assignmentServiceImpl,
		broadcastService:  broadcast.NewGRPCBroadcastService(cfg.WALName, broadcastService),
	}, nil
}

// getDialOptions returns grpc dial options.
func getDialOptions(rb resolver.Builder, cfg *Config) []grpc.DialOption {
	retryPolicy := make(map[string]interface{}, len(cfg.GRPCRetryPolicy)+1)
	for k, v := range cfg.GRPCRetryPolicy {
		retryPolicy[k] = v
	}
	retryPolicy["retryableStatusCodes"] = []string{"UNAVAILABLE"}
	defaultServiceConfig := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{picker.ServerIDPickerBalancerName: map[string]interface{}{}},
		},
		"methodConfig": []map[string]interface{}{
			{
				"name": []map[string]string{
					{"service": "milvus.proto.streaming.StreamingCoordAssignmentService"},
				},
				"waitForReady": true,
				"retryPolicy":  retryPolicy,
			},
		},
	}
	defaultServiceConfigJSON, err := json.Marshal(defaultServiceConfig)
	if err != nil {
		panic(err)
	}
	dialOptions := make([]grpc.DialOption, 0, len(cfg.ExtraGRPCDialOptions)+10)
	dialOptions = append(dialOptions,
		grpc.WithBlock(),
		grpc.WithResolvers(rb),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(
			otelgrpc.UnaryClientInterceptor(tracer.GetInterceptorOpts()...),
			interceptor.ClusterInjectionUnaryClientInterceptor(cfg.ClusterPrefix),
			streamingserviceinterceptor.NewStreamingServiceUnaryClientInterceptor(),
		),
		grpc.WithChainStreamInterceptor(
			otelgrpc.StreamClientInterceptor(tracer.GetInterceptorOpts()...),
			interceptor.ClusterInjectionStreamClientInterceptor(cfg.ClusterPrefix),
			streamingserviceinterceptor.NewStreamingServiceStreamClientInterceptor(),
		),
		grpc.WithReturnConnectionError(),
		grpc.WithDefaultServiceConfig(string(defaultServiceConfigJSON)),
	)
	for _, opt := range cfg.ExtraGRPCDialOptions {
		dialOptions = append(dialOptions, opt)
	}
	return dialOptions
}
