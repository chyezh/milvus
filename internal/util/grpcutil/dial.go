// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcutil

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	grpcresolver "google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/grpcutil/balancer/picker"
	"github.com/milvus-io/milvus/pkg/v2/tracer"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// GetDialOptions builds the common gRPC dial options used by all migrated clients.
// It configures retry policy (retryable on UNAVAILABLE), ServerIDPicker balancer,
// waitForReady, TLS credentials, otel tracing + cluster injection interceptors,
// and grpc.WithBlock + grpc.WithReturnConnectionError.
//
// Parameters:
//   - cfg: the gRPC client config from paramtable
//   - serviceName: the fully-qualified gRPC service name for method config
//   - rb: optional resolver.Builder; if non-nil, added via grpc.WithResolvers
//   - extraUnaryInterceptors: additional unary interceptors appended after the defaults
//   - extraStreamInterceptors: additional stream interceptors appended after the defaults
func GetDialOptions(
	cfg *paramtable.GrpcClientConfig,
	serviceName string,
	rb grpcresolver.Builder,
	extraUnaryInterceptors []grpc.UnaryClientInterceptor,
	extraStreamInterceptors []grpc.StreamClientInterceptor,
) []grpc.DialOption {
	tlsCfg := &paramtable.Get().InternalTLSCfg
	retryPolicy := cfg.GetDefaultRetryPolicy()
	retryPolicy["retryableStatusCodes"] = []string{"UNAVAILABLE"}
	defaultServiceConfig := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{picker.ServerIDPickerBalancerName: map[string]interface{}{}},
		},
		"methodConfig": []map[string]interface{}{
			{
				"name": []map[string]string{
					{"service": serviceName},
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
	creds, err := tlsCfg.GetClientCreds(context.Background())
	if err != nil {
		panic(err)
	}

	// Build unary interceptors: otel + cluster injection + extras.
	unaryInterceptors := []grpc.UnaryClientInterceptor{
		otelgrpc.UnaryClientInterceptor(tracer.GetInterceptorOpts()...),
		interceptor.ClusterInjectionUnaryClientInterceptor(),
	}
	unaryInterceptors = append(unaryInterceptors, extraUnaryInterceptors...)

	// Build stream interceptors: otel + cluster injection + extras.
	streamInterceptors := []grpc.StreamClientInterceptor{
		otelgrpc.StreamClientInterceptor(tracer.GetInterceptorOpts()...),
		interceptor.ClusterInjectionStreamClientInterceptor(),
	}
	streamInterceptors = append(streamInterceptors, extraStreamInterceptors...)

	dialOptions := cfg.GetDialOptionsFromConfig()
	dialOptions = append(dialOptions,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(unaryInterceptors...),
		grpc.WithChainStreamInterceptor(streamInterceptors...),
		grpc.WithReturnConnectionError(),
		grpc.WithDefaultServiceConfig(string(defaultServiceConfigJSON)),
	)
	if rb != nil {
		dialOptions = append(dialOptions, grpc.WithResolvers(rb))
	}
	return dialOptions
}
