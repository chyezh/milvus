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

package types

import (
	"context"
	"io"

	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
)

// Limiter defines the interface to perform request rate limiting.
// If Limit function return true, the request will be rejected.
// Otherwise, the request will pass. Limit also returns limit of limiter.
type Limiter interface {
	Check(dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error
	Alloc(ctx context.Context, dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error
}

// Component is the interface all services implement
type Component interface {
	Init() error
	Start() error
	Stop() error
	Register() error
}

// DataNodeClient is the client interface for datanode server
type DataNodeClient interface {
	io.Closer
	datapb.DataNodeClient
	workerpb.IndexNodeClient
}

// DataNode is the interface `datanode` package implements
type DataNode interface {
	Component
	datapb.DataNodeServer
	workerpb.IndexNodeServer
}

// DataNodeComponent is used by grpc server of DataNode
//
//go:generate mockery --name=DataNodeComponent --structname=MockDataNode --output=../mocks  --filename=mock_datanode.go --with-expecter
type DataNodeComponent interface {
	DataNode

	// UpdateStateCode updates state code for DataNode
	//  `stateCode` is current statement of this data node, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

	// GetStateCode return state code of this data node
	GetStateCode() commonpb.StateCode

	SetAddress(address string)
	GetAddress() string
	GetNodeID() int64

	// SetEtcdClient set etcd client for DataNode
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetMixCoordClient set SetMixCoordClient for DataNode
	// `mixCoord` is a client of root coordinator.
	//
	// Return a generic error in status:
	//     If the mixCoord is nil or the mixCoord has been set before.
	// Return nil in status:
	//     The mixCoord is not nil.
	SetMixCoordClient(mixCoord MixCoordClient) error
}

// DataCoordClient is the client interface for datacoord server
type DataCoordClient interface {
	io.Closer
	datapb.DataCoordClient
	indexpb.IndexCoordClient
}

// DataCoord is the interface `datacoord` package implements
type DataCoord interface {
	Component
	datapb.DataCoordServer
}

// DataCoordComponent defines the interface of DataCoord component.
//
//go:generate mockery --name=DataCoordComponent --structname=MockDataCoord --output=../mocks  --filename=mock_datacoord.go --with-expecter
type DataCoordComponent interface {
	DataCoord

	SetAddress(address string)
	// SetEtcdClient set EtcdClient for DataCoord
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetTiKVClient set TiKV client for QueryNode
	SetTiKVClient(client *txnkv.Client)

	SetMixCoord(mixCoord MixCoord)

	// SetDataNodeCreator set DataNode client creator func for DataCoord
	SetDataNodeCreator(func(context.Context, string, int64) (DataNodeClient, error))
}

// RootCoordClient is the client interface for rootcoord server
type RootCoordClient interface {
	io.Closer
	rootcoordpb.RootCoordClient
}

// RootCoord is the interface `rootcoord` package implements
//
//go:generate mockery --name=RootCoord  --output=../mocks --filename=mock_rootcoord.go --with-expecter
type RootCoord interface {
	Component
	rootcoordpb.RootCoordServer
}

// RootCoordComponent is used by grpc server of RootCoord
type RootCoordComponent interface {
	RootCoord

	SetAddress(address string)
	// SetEtcdClient set EtcdClient for RootCoord
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetTiKVClient set TiKV client for RootCoord
	SetTiKVClient(client *txnkv.Client)

	// UpdateStateCode updates state code for RootCoord
	// State includes: Initializing, Healthy and Abnormal
	UpdateStateCode(commonpb.StateCode)

	// SetMixCoord set SetMixCoord for RootCoord
	// `dataCoord` is a client of data coordinator.
	//
	// Always return nil.
	SetMixCoord(mixCoord MixCoord) error

	// SetProxyCreator set Proxy client creator func for RootCoord
	SetProxyCreator(func(ctx context.Context, addr string, nodeID int64) (ProxyClient, error))

	// GetMetrics notifies RootCoordComponent to collect metrics for specified component
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// ProxyClient is the client interface for proxy server
type ProxyClient interface {
	io.Closer
	proxypb.ProxyClient
}

// Proxy is the interface `proxy` package implements
type Proxy interface {
	Component
	proxypb.ProxyServer
	milvuspb.MilvusServiceServer

	ImportV2(context.Context, *internalpb.ImportRequest) (*internalpb.ImportResponse, error)
	GetImportProgress(context.Context, *internalpb.GetImportProgressRequest) (*internalpb.GetImportProgressResponse, error)
	ListImports(context.Context, *internalpb.ListImportsRequest) (*internalpb.ListImportsResponse, error)
}

// ProxyComponent defines the interface of proxy component.
//
//go:generate mockery --name=ProxyComponent --structname=MockProxy --output=../mocks  --filename=mock_proxy.go --with-expecter
type ProxyComponent interface {
	Proxy

	SetAddress(address string)
	GetAddress() string
	// SetEtcdClient set EtcdClient for Proxy
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetMixCoordClient set MixCoord for Proxy
	// `mixCoord` is a client of mix coordinator.
	SetMixCoordClient(rootCoord MixCoordClient)

	// SetQueryNodeCreator set QueryNode client creator func for Proxy
	SetQueryNodeCreator(func(ctx context.Context, addr string, nodeID int64) (QueryNodeClient, error))

	// GetRateLimiter returns the rateLimiter in Proxy
	GetRateLimiter() (Limiter, error)

	// UpdateStateCode updates state code for Proxy
	//  `stateCode` is current statement of this proxy node, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)
}

type QueryNodeClient interface {
	io.Closer
	querypb.QueryNodeClient
}

// QueryNode is the interface `querynode` package implements
type QueryNode interface {
	Component
	querypb.QueryNodeServer
}

// QueryNodeComponent is used by grpc server of QueryNode
//
//go:generate mockery --name=QueryNodeComponent --structname=MockQueryNode --output=../mocks  --filename=mock_querynode.go --with-expecter
type QueryNodeComponent interface {
	QueryNode

	// UpdateStateCode updates state code for QueryNode
	//  `stateCode` is current statement of this query node, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

	SetAddress(address string)
	GetAddress() string
	GetNodeID() int64

	// SetEtcdClient set etcd client for QueryNode
	SetEtcdClient(etcdClient *clientv3.Client)
}

// QueryCoordClient is the client interface for querycoord server
type QueryCoordClient interface {
	io.Closer
	querypb.QueryCoordClient
}

// QueryCoord is the interface `querycoord` package implements
type QueryCoord interface {
	Component
	querypb.QueryCoordServer
}

// QueryCoordComponent is used by grpc server of QueryCoord
//
//go:generate mockery --name=QueryCoordComponent --structname=MockQueryCoord --output=../mocks  --filename=mock_querycoord.go --with-expecter
type QueryCoordComponent interface {
	QueryCoord

	SetAddress(address string)

	// SetEtcdClient set etcd client for QueryCoord
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetTiKVClient set TiKV client for QueryCoord
	SetTiKVClient(client *txnkv.Client)

	// UpdateStateCode updates state code for QueryCoord
	//  `stateCode` is current statement of this QueryCoord, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

	// SetQueryNodeCreator set QueryNode client creator func for QueryCoord
	SetQueryNodeCreator(func(ctx context.Context, addr string, nodeID int64) (QueryNodeClient, error))

	SetMixCoord(mixCoord MixCoord)
}

// MixCoordClient is the client interface for mixcoord server
type MixCoordClient interface {
	io.Closer
	rootcoordpb.RootCoordClient
	datapb.DataCoordClient
	indexpb.IndexCoordClient

	ShowLoadCollections(ctx context.Context, in *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error)
	ShowLoadPartitions(ctx context.Context, in *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error)
	LoadPartitions(ctx context.Context, in *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	LoadCollection(ctx context.Context, in *querypb.LoadCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	SyncNewCreatedPartition(ctx context.Context, in *querypb.SyncNewCreatedPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	GetPartitionStates(ctx context.Context, in *querypb.GetPartitionStatesRequest, opts ...grpc.CallOption) (*querypb.GetPartitionStatesResponse, error)
	GetLoadSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error)
	LoadBalance(ctx context.Context, in *querypb.LoadBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error)
	// https://wiki.lfaidata.foundation/display/MIL/MEP+8+--+Add+metrics+for+proxy
	GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error)
	// https://wiki.lfaidata.foundation/display/MIL/MEP+23+--+Multiple+memory+replication+design
	GetReplicas(ctx context.Context, in *milvuspb.GetReplicasRequest, opts ...grpc.CallOption) (*milvuspb.GetReplicasResponse, error)
	GetShardLeaders(ctx context.Context, in *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error)
	CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error)
	CreateResourceGroup(ctx context.Context, in *milvuspb.CreateResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	UpdateResourceGroups(ctx context.Context, in *querypb.UpdateResourceGroupsRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	DropResourceGroup(ctx context.Context, in *milvuspb.DropResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	TransferNode(ctx context.Context, in *milvuspb.TransferNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	TransferReplica(ctx context.Context, in *querypb.TransferReplicaRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	ListResourceGroups(ctx context.Context, in *milvuspb.ListResourceGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListResourceGroupsResponse, error)
	DescribeResourceGroup(ctx context.Context, in *querypb.DescribeResourceGroupRequest, opts ...grpc.CallOption) (*querypb.DescribeResourceGroupResponse, error)
	// ops interfaces
	ListCheckers(ctx context.Context, in *querypb.ListCheckersRequest, opts ...grpc.CallOption) (*querypb.ListCheckersResponse, error)
	ActivateChecker(ctx context.Context, in *querypb.ActivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	DeactivateChecker(ctx context.Context, in *querypb.DeactivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	ListQueryNode(ctx context.Context, in *querypb.ListQueryNodeRequest, opts ...grpc.CallOption) (*querypb.ListQueryNodeResponse, error)
	GetQueryNodeDistribution(ctx context.Context, in *querypb.GetQueryNodeDistributionRequest, opts ...grpc.CallOption) (*querypb.GetQueryNodeDistributionResponse, error)
	SuspendBalance(ctx context.Context, in *querypb.SuspendBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	ResumeBalance(ctx context.Context, in *querypb.ResumeBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	CheckBalanceStatus(ctx context.Context, in *querypb.CheckBalanceStatusRequest, opts ...grpc.CallOption) (*querypb.CheckBalanceStatusResponse, error)
	SuspendNode(ctx context.Context, in *querypb.SuspendNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	ResumeNode(ctx context.Context, in *querypb.ResumeNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	TransferSegment(ctx context.Context, in *querypb.TransferSegmentRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	TransferChannel(ctx context.Context, in *querypb.TransferChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	CheckQueryNodeDistribution(ctx context.Context, in *querypb.CheckQueryNodeDistributionRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	UpdateLoadConfig(ctx context.Context, in *querypb.UpdateLoadConfigRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
}

// MixCoord is the interface `MixCoord` package implements
//
//go:generate mockery --name=MixCoord  --output=../mocks --filename=mock_mixcoord.go --with-expecter
type MixCoord interface {
	Component

	rootcoordpb.RootCoordServer
	datapb.DataCoordServer
	indexpb.IndexCoordServer

	ShowLoadCollections(ctx context.Context, in *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error)
	ShowLoadPartitions(ctx context.Context, in *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error)
	LoadPartitions(ctx context.Context, in *querypb.LoadPartitionsRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest) (*commonpb.Status, error)
	LoadCollection(ctx context.Context, in *querypb.LoadCollectionRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	SyncNewCreatedPartition(ctx context.Context, in *querypb.SyncNewCreatedPartitionRequest) (*commonpb.Status, error)
	GetPartitionStates(ctx context.Context, in *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error)
	GetLoadSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)
	LoadBalance(ctx context.Context, in *querypb.LoadBalanceRequest) (*commonpb.Status, error)
	ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)
	// https://wiki.lfaidata.foundation/display/MIL/MEP+8+--+Add+metrics+for+proxy
	GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
	// https://wiki.lfaidata.foundation/display/MIL/MEP+23+--+Multiple+memory+replication+design
	GetReplicas(ctx context.Context, in *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error)
	GetShardLeaders(ctx context.Context, in *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error)
	CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error)
	CreateResourceGroup(ctx context.Context, in *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error)
	UpdateResourceGroups(ctx context.Context, in *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error)
	DropResourceGroup(ctx context.Context, in *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error)
	TransferNode(ctx context.Context, in *milvuspb.TransferNodeRequest) (*commonpb.Status, error)
	TransferReplica(ctx context.Context, in *querypb.TransferReplicaRequest) (*commonpb.Status, error)
	ListResourceGroups(ctx context.Context, in *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error)
	DescribeResourceGroup(ctx context.Context, in *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error)
	// ops interfaces
	ListCheckers(ctx context.Context, in *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error)
	ActivateChecker(ctx context.Context, in *querypb.ActivateCheckerRequest) (*commonpb.Status, error)
	DeactivateChecker(ctx context.Context, in *querypb.DeactivateCheckerRequest) (*commonpb.Status, error)
	ListQueryNode(ctx context.Context, in *querypb.ListQueryNodeRequest) (*querypb.ListQueryNodeResponse, error)
	GetQueryNodeDistribution(ctx context.Context, in *querypb.GetQueryNodeDistributionRequest) (*querypb.GetQueryNodeDistributionResponse, error)
	SuspendBalance(ctx context.Context, in *querypb.SuspendBalanceRequest) (*commonpb.Status, error)
	ResumeBalance(ctx context.Context, in *querypb.ResumeBalanceRequest) (*commonpb.Status, error)
	CheckBalanceStatus(ctx context.Context, in *querypb.CheckBalanceStatusRequest) (*querypb.CheckBalanceStatusResponse, error)
	SuspendNode(ctx context.Context, in *querypb.SuspendNodeRequest) (*commonpb.Status, error)
	ResumeNode(ctx context.Context, in *querypb.ResumeNodeRequest) (*commonpb.Status, error)
	TransferSegment(ctx context.Context, in *querypb.TransferSegmentRequest) (*commonpb.Status, error)
	TransferChannel(ctx context.Context, in *querypb.TransferChannelRequest) (*commonpb.Status, error)
	CheckQueryNodeDistribution(ctx context.Context, in *querypb.CheckQueryNodeDistributionRequest) (*commonpb.Status, error)
	UpdateLoadConfig(ctx context.Context, in *querypb.UpdateLoadConfigRequest) (*commonpb.Status, error)

	// GetMetrics notifies MixCoordComponent to collect metrics for specified component
	GetDcMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)

	// GetMetrics notifies MixCoordComponent to collect metrics for specified component
	GetQcMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
	// GetMetrics notifies MixCoordComponent to collect metrics for specified component
	NotifyDropPartition(ctx context.Context, channel string, partitionIDs []int64) error
}

// MixCoordComponent is used by grpc server of MixCoord
type MixCoordComponent interface {
	MixCoord

	SetAddress(address string)
	// SetEtcdClient set EtcdClient for MixCoord
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// SetTiKVClient set TiKV client for MixCoord
	SetTiKVClient(client *txnkv.Client)

	// UpdateStateCode updates state code for MixCoord
	// State includes: Initializing, Healthy and Abnormal
	UpdateStateCode(commonpb.StateCode)

	// GetMetrics notifies MixCoordComponent to collect metrics for specified component
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)

	RegisterStreamingCoordGRPCService(server *grpc.Server)

	GracefulStop()

	SetMixCoordClient(client MixCoordClient)
}
