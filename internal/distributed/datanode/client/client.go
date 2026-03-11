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

package grpcdatanodeclient

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
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
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

type DataNodeClient struct {
	datapb.DataNodeClient
	workerpb.IndexNodeClient
}

var (
	sharedOnce    sync.Once
	sharedService lazygrpc.Service[DataNodeClient]
	sharedRB      resolver.Builder
)

func getSharedService() lazygrpc.Service[DataNodeClient] {
	sharedOnce.Do(func() {
		etcdCli, _ := kvfactory.GetEtcdAndPath()
		role := sessionutil.GetSessionPrefixByRole(typeutil.DataNodeRole)
		sharedRB = resolver.NewSessionBuilder(
			etcdCli,
			discoverer.OptSDPrefix(role),
			discoverer.OptSDVersionRange(">=2.6.0-dev"),
		)
		cfg := &Params.DataNodeGrpcClientCfg
		dialTimeout := cfg.DialTimeout.GetAsDuration(time.Millisecond)
		dialOptions := grpcutil.GetDialOptions(cfg, "milvus.proto.data.DataNode", sharedRB, nil, nil)
		conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
			ctx, cancel := context.WithTimeout(ctx, dialTimeout)
			defer cancel()
			return grpc.DialContext(
				ctx,
				resolver.SessionResolverScheme+":///"+typeutil.DataNodeRole,
				dialOptions...,
			)
		})
		sharedService = lazygrpc.WithServiceCreator(conn, func(cc grpc.ClientConnInterface) DataNodeClient {
			return DataNodeClient{
				DataNodeClient:  datapb.NewDataNodeClient(cc),
				IndexNodeClient: workerpb.NewIndexNodeClient(cc),
			}
		})
	})
	return sharedService
}

// Client is the grpc client for DataNode
type Client struct {
	service  lazygrpc.Service[DataNodeClient]
	serverID int64
}

// NewClient creates a client for DataNode.
func NewClient(ctx context.Context, addr string, serverID int64, encryption bool) (types.DataNodeClient, error) {
	svc := getSharedService()
	if svc == nil {
		log.Ctx(ctx).Error("failed to get shared DataNode service", zap.Int64("serverID", serverID))
	}
	return &Client{
		service:  svc,
		serverID: serverID,
	}, nil
}

// Close stops the client.
// This is a thin wrapper; the shared connection is managed separately.
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

func callService[T any](ctx context.Context, c *Client, fn func(ctx context.Context, client DataNodeClient) (T, error)) (T, error) {
	client, err := c.service.GetService(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	ctx = contextutil.WithPickServerID(ctx, c.serverID)
	return fn(ctx, client)
}

// GetComponentStates returns ComponentStates
func (c *Client) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetStatisticsChannel return the statistics channel in string
// Statistics channel contains statistics infos of query nodes, such as segment infos, memory infos
func (c *Client) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// Deprecated
// WatchDmChannels create consumers on dmChannels to reveive Incremental data
func (c *Client) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.serverID))
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.WatchDmChannels(ctx, req)
	})
}

// FlushSegments notifies DataNode to flush the segments req provids. The flush tasks are async to this
//
//	rpc, DataNode will flush the segments in the background.
//
// Return UnexpectedError code in status:
//
//	If DataNode isn't in HEALTHY: states not HEALTHY or dynamic checks not HEALTHY
//	If DataNode doesn't find the correspounding segmentID in its memeory replica
//
// Return Success code in status and trigers background flush:
//
//	Log an info log if a segment is under flushing
func (c *Client) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.serverID))
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.FlushSegments(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of DataNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.serverID))
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*internalpb.ShowConfigurationsResponse, error) {
		return client.ShowConfigurations(ctx, req)
	})
}

// GetMetrics returns metrics
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.serverID))
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*milvuspb.GetMetricsResponse, error) {
		return client.DataNodeClient.GetMetrics(ctx, req)
	})
}

// CompactionV2 return compaction by given plan
func (c *Client) CompactionV2(ctx context.Context, req *datapb.CompactionPlan, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.CompactionV2(ctx, req)
	})
}

func (c *Client) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest, opts ...grpc.CallOption) (*datapb.CompactionStateResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.serverID))
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*datapb.CompactionStateResponse, error) {
		return client.GetCompactionState(ctx, req)
	})
}

func (c *Client) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest, opts ...grpc.CallOption) (*datapb.ResendSegmentStatsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.serverID))
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*datapb.ResendSegmentStatsResponse, error) {
		return client.ResendSegmentStats(ctx, req)
	})
}

// SyncSegments is the DataNode client side code for SyncSegments call.
func (c *Client) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.SyncSegments(ctx, req)
	})
}

// FlushChannels notifies DataNode to sync all the segments belongs to the target channels.
func (c *Client) FlushChannels(ctx context.Context, req *datapb.FlushChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.FlushChannels(ctx, req)
	})
}

func (c *Client) NotifyChannelOperation(ctx context.Context, req *datapb.ChannelOperationsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.NotifyChannelOperation(ctx, req)
	})
}

func (c *Client) CheckChannelOperationProgress(ctx context.Context, req *datapb.ChannelWatchInfo, opts ...grpc.CallOption) (*datapb.ChannelOperationProgressResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*datapb.ChannelOperationProgressResponse, error) {
		return client.CheckChannelOperationProgress(ctx, req)
	})
}

func (c *Client) PreImport(ctx context.Context, req *datapb.PreImportRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.PreImport(ctx, req)
	})
}

func (c *Client) ImportV2(ctx context.Context, req *datapb.ImportRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.ImportV2(ctx, req)
	})
}

func (c *Client) QueryPreImport(ctx context.Context, req *datapb.QueryPreImportRequest, opts ...grpc.CallOption) (*datapb.QueryPreImportResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*datapb.QueryPreImportResponse, error) {
		return client.QueryPreImport(ctx, req)
	})
}

func (c *Client) QueryImport(ctx context.Context, req *datapb.QueryImportRequest, opts ...grpc.CallOption) (*datapb.QueryImportResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*datapb.QueryImportResponse, error) {
		return client.QueryImport(ctx, req)
	})
}

func (c *Client) DropImport(ctx context.Context, req *datapb.DropImportRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.DropImport(ctx, req)
	})
}

func (c *Client) QuerySlot(ctx context.Context, req *datapb.QuerySlotRequest, opts ...grpc.CallOption) (*datapb.QuerySlotResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*datapb.QuerySlotResponse, error) {
		return client.QuerySlot(ctx, req)
	})
}

func (c *Client) DropCompactionPlan(ctx context.Context, req *datapb.DropCompactionPlanRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.DropCompactionPlan(ctx, req)
	})
}

// CreateJob sends the build index request to IndexNode.
func (c *Client) CreateJob(ctx context.Context, req *workerpb.CreateJobRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.CreateJob(ctx, req)
	})
}

// QueryJobs query the task info of the index task.
func (c *Client) QueryJobs(ctx context.Context, req *workerpb.QueryJobsRequest, opts ...grpc.CallOption) (*workerpb.QueryJobsResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*workerpb.QueryJobsResponse, error) {
		return client.QueryJobs(ctx, req)
	})
}

// DropJobs query the task info of the index task.
func (c *Client) DropJobs(ctx context.Context, req *workerpb.DropJobsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.DropJobs(ctx, req)
	})
}

// GetJobStats query the task info of the index task.
func (c *Client) GetJobStats(ctx context.Context, req *workerpb.GetJobStatsRequest, opts ...grpc.CallOption) (*workerpb.GetJobStatsResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*workerpb.GetJobStatsResponse, error) {
		return client.GetJobStats(ctx, req)
	})
}

func (c *Client) CreateJobV2(ctx context.Context, req *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.CreateJobV2(ctx, req)
	})
}

func (c *Client) QueryJobsV2(ctx context.Context, req *workerpb.QueryJobsV2Request, opts ...grpc.CallOption) (*workerpb.QueryJobsV2Response, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*workerpb.QueryJobsV2Response, error) {
		return client.QueryJobsV2(ctx, req)
	})
}

func (c *Client) DropJobsV2(ctx context.Context, req *workerpb.DropJobsV2Request, opt ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.DropJobsV2(ctx, req)
	})
}

func (c *Client) CreateTask(ctx context.Context, in *workerpb.CreateTaskRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.CreateTask(ctx, in)
	})
}

func (c *Client) QueryTask(ctx context.Context, in *workerpb.QueryTaskRequest, opts ...grpc.CallOption) (*workerpb.QueryTaskResponse, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*workerpb.QueryTaskResponse, error) {
		return client.QueryTask(ctx, in)
	})
}

func (c *Client) DropTask(ctx context.Context, in *workerpb.DropTaskRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.DropTask(ctx, in)
	})
}

func (c *Client) SyncFileResource(ctx context.Context, req *internalpb.SyncFileResourceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return callService(ctx, c, func(ctx context.Context, client DataNodeClient) (*commonpb.Status, error) {
		return client.SyncFileResource(ctx, req)
	})
}
