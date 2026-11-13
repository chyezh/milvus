package connection

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/tracer"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NewSession discovers every StreamingNode session, including query replicas
// absent from writer assignment discovery. The caller closes both returned owners.
func NewSession(etcdCli *clientv3.Client, services ...string) (lazygrpc.Conn, resolver.Builder) {
	role := sessionutil.GetSessionPrefixByRole(typeutil.StreamingNodeRole)
	rb := resolver.NewSessionBuilder(etcdCli, discoverer.OptSDPrefix(role), discoverer.OptSDVersionRange(">=2.6.0-dev"))
	dialTimeout := paramtable.Get().StreamingNodeGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	dialOptions := getDialOptions(rb, services)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(ctx, resolver.SessionResolverScheme+":///"+typeutil.StreamingNodeRole, dialOptions...)
	})
	return conn, rb
}

// getDialOptions returns grpc dial options.
func getDialOptions(rb resolver.Builder, services []string) []grpc.DialOption {
	cfg := &paramtable.Get().StreamingNodeGrpcClientCfg
	tlsCfg := &paramtable.Get().InternalTLSCfg
	retryPolicy := cfg.GetDefaultRetryPolicy()
	retryPolicy["retryableStatusCodes"] = []string{"UNAVAILABLE"}
	names := make([]map[string]string, 0, len(services))
	for _, service := range services {
		names = append(names, map[string]string{"service": service})
	}
	defaultServiceConfig := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{picker.ServerIDPickerBalancerName: map[string]interface{}{}},
		},
		"methodConfig": []map[string]interface{}{
			{
				"name":         names,
				"waitForReady": true,
				"retryPolicy":  retryPolicy,
			},
		},
	}
	defaultServiceConfigJSON, err := json.Marshal(defaultServiceConfig)
	if err != nil {
		panic(err)
	}
	creds, err := tlsCfg.GetClientCreds(context.Background())
	if err != nil {
		panic(err)
	}
	dialOptions := cfg.GetDialOptionsFromConfig()
	dialOptions = append(dialOptions,
		grpc.WithBlock(),
		grpc.WithResolvers(rb),
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(
			mlog.UnaryClientInterceptor(),
			otelgrpc.UnaryClientInterceptor(tracer.GetInterceptorOpts()...),
			interceptor.ClusterInjectionUnaryClientInterceptor(),
			streamingserviceinterceptor.NewServiceUnaryClientInterceptor(typeutil.StreamingNodeRole),
		),
		grpc.WithChainStreamInterceptor(
			mlog.StreamClientInterceptor(),
			otelgrpc.StreamClientInterceptor(tracer.GetInterceptorOpts()...),
			interceptor.ClusterInjectionStreamClientInterceptor(),
			streamingserviceinterceptor.NewServiceStreamClientInterceptor(typeutil.StreamingNodeRole),
		),
		grpc.WithReturnConnectionError(),
		grpc.WithDefaultServiceConfig(string(defaultServiceConfigJSON)),
	)
	return dialOptions
}
