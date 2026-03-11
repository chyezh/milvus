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

package grpcquerynodeclient

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/grpcutil"
	"github.com/milvus-io/milvus/internal/util/grpcutil/contextutil"
	"github.com/milvus-io/milvus/internal/util/grpcutil/discoverer"
	"github.com/milvus-io/milvus/internal/util/grpcutil/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/grpcutil/resolver"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

var (
	sharedOnce    sync.Once
	sharedService lazygrpc.Service[querypb.QueryNodeClient]
	sharedRB      resolver.Builder
)

func getSharedService() lazygrpc.Service[querypb.QueryNodeClient] {
	sharedOnce.Do(func() {
		etcdCli, _ := kvfactory.GetEtcdAndPath()
		role := sessionutil.GetSessionPrefixByRole(typeutil.QueryNodeRole)
		sharedRB = resolver.NewSessionBuilder(etcdCli,
			discoverer.OptSDPrefix(role),
			discoverer.OptSDVersionRange(">=2.6.0-dev"),
		)
		cfg := &paramtable.Get().QueryNodeGrpcClientCfg
		dialTimeout := cfg.DialTimeout.GetAsDuration(time.Millisecond)
		dialOptions := grpcutil.GetDialOptions(cfg, "milvus.proto.query.QueryNode", sharedRB, nil, nil)
		conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
			ctx, cancel := context.WithTimeout(ctx, dialTimeout)
			defer cancel()
			return grpc.DialContext(ctx,
				resolver.SessionResolverScheme+":///"+typeutil.QueryNodeRole,
				dialOptions...,
			)
		})
		sharedService = lazygrpc.WithServiceCreator(conn, querypb.NewQueryNodeClient)
	})
	return sharedService
}

// Client is the grpc client of QueryNode.
type Client struct {
	service lazygrpc.Service[querypb.QueryNodeClient]
	nodeID  int64
}

// NewClient creates a new QueryNode client.
func NewClient(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
	if addr == "" {
		return nil, errors.New("addr is empty")
	}
	return &Client{
		service: getSharedService(),
		nodeID:  nodeID,
	}, nil
}

// Close close QueryNode's grpc client
func (c *Client) Close() error {
	return nil
}

// CloseSharedConnection closes the shared gRPC connection and resolver.
func CloseSharedConnection() {
	if sharedRB != nil {
		sharedRB.Close()
	}
	if sharedService != nil {
		sharedService.Close()
	}
}

func callService[T any](ctx context.Context, c *Client, fn func(ctx context.Context, client querypb.QueryNodeClient) (T, error)) (T, error) {
	client, err := c.service.GetService(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	ctx = contextutil.WithPickServerID(ctx, c.nodeID)
	return fn(ctx, client)
}

// GetComponentStates gets the component states of QueryNode.
func (c *Client) GetComponentStates(ctx context.Context, _ *milvuspb.GetComponentStatesRequest, _ ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetTimeTickChannel gets the time tick channel of QueryNode.
func (c *Client) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest, _ ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
}

// GetStatisticsChannel gets the statistics channel of QueryNode.
func (c *Client) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, _ ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// WatchDmChannels watches the channels about data manipulation.
func (c *Client) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.WatchDmChannels(ctx, req)
	})
}

// UnsubDmChannel unsubscribes the channels about data manipulation.
func (c *Client) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.UnsubDmChannel(ctx, req)
	})
}

// LoadSegments loads the segments to search.
func (c *Client) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.LoadSegments(ctx, req)
	})
}

// ReleaseCollection releases the data of the specified collection in QueryNode.
func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.ReleaseCollection(ctx, req)
	})
}

// LoadPartitions updates partitions meta info in QueryNode.
func (c *Client) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.LoadPartitions(ctx, req)
	})
}

// ReleasePartitions releases the data of the specified partitions in QueryNode.
func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.ReleasePartitions(ctx, req)
	})
}

// ReleaseSegments releases the data of the specified segments in QueryNode.
func (c *Client) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.ReleaseSegments(ctx, req)
	})
}

// Search performs replica search tasks in QueryNode.
func (c *Client) Search(ctx context.Context, req *querypb.SearchRequest, _ ...grpc.CallOption) (*internalpb.SearchResults, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*internalpb.SearchResults, error) {
		return client.Search(ctx, req)
	})
}

func (c *Client) SearchSegments(ctx context.Context, req *querypb.SearchRequest, _ ...grpc.CallOption) (*internalpb.SearchResults, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*internalpb.SearchResults, error) {
		return client.SearchSegments(ctx, req)
	})
}

// Query performs replica query tasks in QueryNode.
func (c *Client) Query(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*internalpb.RetrieveResults, error) {
		return client.Query(ctx, req)
	})
}

func (c *Client) QueryStream(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (querypb.QueryNode_QueryStreamClient, error) {
		return client.QueryStream(ctx, req)
	})
}

func (c *Client) QuerySegments(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*internalpb.RetrieveResults, error) {
		return client.QuerySegments(ctx, req)
	})
}

func (c *Client) QueryStreamSegments(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (querypb.QueryNode_QueryStreamSegmentsClient, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (querypb.QueryNode_QueryStreamSegmentsClient, error) {
		return client.QueryStreamSegments(ctx, req)
	})
}

// GetSegmentInfo gets the information of the specified segments in QueryNode.
func (c *Client) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest, _ ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*querypb.GetSegmentInfoResponse, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.GetSegmentInfo(ctx, req)
	})
}

// SyncReplicaSegments syncs replica node segments information to shard leaders.
func (c *Client) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.SyncReplicaSegments(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of QueryNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, _ ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*internalpb.ShowConfigurationsResponse, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.ShowConfigurations(ctx, req)
	})
}

// GetMetrics gets the metrics information of QueryNode.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, _ ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*milvuspb.GetMetricsResponse, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.GetMetrics(ctx, req)
	})
}

func (c *Client) GetStatistics(ctx context.Context, request *querypb.GetStatisticsRequest, _ ...grpc.CallOption) (*internalpb.GetStatisticsResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*internalpb.GetStatisticsResponse, error) {
		return client.GetStatistics(ctx, request)
	})
}

func (c *Client) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest, _ ...grpc.CallOption) (*querypb.GetDataDistributionResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*querypb.GetDataDistributionResponse, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.GetDataDistribution(ctx, req)
	})
}

func (c *Client) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID))
		return client.SyncDistribution(ctx, req)
	})
}

// Delete is used to forward delete message between delegator and workers.
func (c *Client) Delete(ctx context.Context, req *querypb.DeleteRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID),
		)
		return client.Delete(ctx, req)
	})
}

// DeleteBatch is the API to apply same delete data into multiple segments.
// it's basically same as `Delete` but cost less memory pressure.
func (c *Client) DeleteBatch(ctx context.Context, req *querypb.DeleteBatchRequest, _ ...grpc.CallOption) (*querypb.DeleteBatchResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*querypb.DeleteBatchResponse, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID),
		)
		return client.DeleteBatch(ctx, req)
	})
}

func (c *Client) UpdateSchema(ctx context.Context, req *querypb.UpdateSchemaRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID),
		)
		return client.UpdateSchema(ctx, req)
	})
}

func (c *Client) RunAnalyzer(ctx context.Context, req *querypb.RunAnalyzerRequest, _ ...grpc.CallOption) (*milvuspb.RunAnalyzerResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*milvuspb.RunAnalyzerResponse, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID),
		)
		return client.RunAnalyzer(ctx, req)
	})
}

func (c *Client) DropIndex(ctx context.Context, req *querypb.DropIndexRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID),
		)
		return client.DropIndex(ctx, req)
	})
}

func (c *Client) UpdateIndex(ctx context.Context, req *querypb.UpdateIndexRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID),
		)
		return client.UpdateIndex(ctx, req)
	})
}

func (c *Client) ValidateAnalyzer(ctx context.Context, req *querypb.ValidateAnalyzerRequest, _ ...grpc.CallOption) (*querypb.ValidateAnalyzerResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*querypb.ValidateAnalyzerResponse, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID),
		)
		return client.ValidateAnalyzer(ctx, req)
	})
}

func (c *Client) GetHighlight(ctx context.Context, req *querypb.GetHighlightRequest, _ ...grpc.CallOption) (*querypb.GetHighlightResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*querypb.GetHighlightResponse, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID),
		)
		return client.GetHighlight(ctx, req)
	})
}

func (c *Client) SyncFileResource(ctx context.Context, req *internalpb.SyncFileResourceRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*commonpb.Status, error) {
		req = typeutil.Clone(req)
		return client.SyncFileResource(ctx, req)
	})
}

func (c *Client) ComputePhraseMatchSlop(ctx context.Context, req *querypb.ComputePhraseMatchSlopRequest, _ ...grpc.CallOption) (*querypb.ComputePhraseMatchSlopResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client querypb.QueryNodeClient) (*querypb.ComputePhraseMatchSlopResponse, error) {
		req = typeutil.Clone(req)
		commonpbutil.UpdateMsgBase(
			req.GetBase(),
			commonpbutil.FillMsgBaseFromClient(c.nodeID),
		)
		return client.ComputePhraseMatchSlop(ctx, req)
	})
}
