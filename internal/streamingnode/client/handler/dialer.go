package handler

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazyconn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/interceptor"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// DialContext dials a log node service.
func DialContext(ctx context.Context, w discoverer.AssignmentDiscoverWatcher) HandlerClient {
	rb := resolver.NewChannelAssignmentBuilder(w)
	/// TODO: balancer grpc configuration.
	conn := lazyconn.NewLazyGRPCConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		return grpc.DialContext(
			ctx,
			resolver.ChannelAssignmentResolverScheme+":///"+typeutil.StreamingNodeRole,
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithResolvers(rb),
			grpc.WithDefaultCallOptions(),
			grpc.WithChainUnaryInterceptor(
				otelgrpc.UnaryClientInterceptor(tracer.GetInterceptorOpts()...),
				interceptor.ClusterInjectionUnaryClientInterceptor(),
				streamingserviceinterceptor.NewStreamingServiceUnaryClientInterceptor(),
			),
			grpc.WithChainStreamInterceptor(
				otelgrpc.StreamClientInterceptor(tracer.GetInterceptorOpts()...),
				interceptor.ClusterInjectionStreamClientInterceptor(),
				streamingserviceinterceptor.NewStreamingServiceStreamClientInterceptor(),
			),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 5 * time.Second,
			}),
			grpc.WithReturnConnectionError(),
			grpc.WithDefaultServiceConfig(`{
			"loadBalancingConfig": [{"`+balancer.ServerIDPickerBalancerName+`":{}}]
		}`)) // TODO: we should use dynamic service config in future by add it to resolver.
	})
	watcher := assignment.NewWatcher(rb.Resolver())
	return &handlerClientImpl{
		lifetime: lifetime.NewLifetime(lifetime.Working),
		conn:     conn,
		rb:       rb,
		watcher:  watcher,
	}
}
