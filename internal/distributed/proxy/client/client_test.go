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

package grpcproxyclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// mockServiceImpl implements lazygrpc.Service[proxypb.ProxyClient] for testing.
type mockServiceImpl struct {
	client    proxypb.ProxyClient
	clientErr error
	closed    bool
}

func (m *mockServiceImpl) GetService(ctx context.Context) (proxypb.ProxyClient, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if m.clientErr != nil {
		return nil, m.clientErr
	}
	return m.client, nil
}

func (m *mockServiceImpl) GetConn(ctx context.Context) (*grpc.ClientConn, error) {
	return nil, nil
}

func (m *mockServiceImpl) Close() {
	m.closed = true
}

func newTestClient(mockProxy *mocks.MockProxyClient) *Client {
	return &Client{
		service: &mockServiceImpl{client: mockProxy},
		nodeID:  1,
	}
}

func Test_NewClient(t *testing.T) {
	paramtable.Init()
	// Test with a mock service directly
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	assert.NotNil(t, client)

	// cleanup
	err := client.Close()
	assert.NoError(t, err)
}

func Test_GetComponentStates(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		Status: merr.Success(),
	}, nil)
	_, err := client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetStatisticsChannel(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
		Status: merr.Success(),
	}, nil)
	_, err := client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
		Status: merr.Status(merr.ErrServiceNotReady),
	}, nil)

	_, err = client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_InvalidateCollectionMetaCache(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err := client.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_InvalidateCredentialCache(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().InvalidateCredentialCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err := client.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().InvalidateCredentialCache(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.InvalidateCredentialCache(ctx, &proxypb.InvalidateCredCacheRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_UpdateCredentialCache(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().UpdateCredentialCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err := client.UpdateCredentialCache(ctx, &proxypb.UpdateCredCacheRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().UpdateCredentialCache(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.UpdateCredentialCache(ctx, &proxypb.UpdateCredCacheRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.UpdateCredentialCache(ctx, &proxypb.UpdateCredCacheRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_RefreshPolicyInfoCache(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().RefreshPolicyInfoCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err := client.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().RefreshPolicyInfoCache(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetProxyMetrics(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().GetProxyMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{Status: merr.Success()}, nil)
	_, err := client.GetProxyMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetProxyMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{Status: merr.Status(merr.ErrServiceNotReady)}, nil)

	_, err = client.GetProxyMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetProxyMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_SetRates(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().SetRates(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err := client.SetRates(ctx, &proxypb.SetRatesRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().SetRates(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.SetRates(ctx, &proxypb.SetRatesRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.SetRates(ctx, &proxypb.SetRatesRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_ListClientInfos(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().ListClientInfos(mock.Anything, mock.Anything).Return(&proxypb.ListClientInfosResponse{Status: merr.Success()}, nil)
	_, err := client.ListClientInfos(ctx, &proxypb.ListClientInfosRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().ListClientInfos(mock.Anything, mock.Anything).Return(&proxypb.ListClientInfosResponse{Status: merr.Status(merr.ErrServiceNotReady)}, nil)

	_, err = client.ListClientInfos(ctx, &proxypb.ListClientInfosRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.ListClientInfos(ctx, &proxypb.ListClientInfosRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetDdChannel(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().GetDdChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{Status: merr.Success()}, nil)
	_, err := client.GetDdChannel(ctx, &internalpb.GetDdChannelRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetDdChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{Status: merr.Status(merr.ErrServiceNotReady)}, nil)

	_, err = client.GetDdChannel(ctx, &internalpb.GetDdChannelRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.GetDdChannel(ctx, &internalpb.GetDdChannelRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_ImportV2(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	mockProxy.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(&internalpb.ImportResponse{Status: merr.Success()}, nil)
	_, err := client.ImportV2(ctx, &internalpb.ImportRequest{})
	assert.Nil(t, err)

	mockProxy.EXPECT().GetImportProgress(mock.Anything, mock.Anything).Return(&internalpb.GetImportProgressResponse{Status: merr.Success()}, nil)
	_, err = client.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{})
	assert.Nil(t, err)

	mockProxy.EXPECT().ListImports(mock.Anything, mock.Anything).Return(&internalpb.ListImportsResponse{Status: merr.Success()}, nil)
	_, err = client.ListImports(ctx, &internalpb.ListImportsRequest{})
	assert.Nil(t, err)
}

func Test_InvalidateShardLeaderCache(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	// test success
	mockProxy.EXPECT().InvalidateShardLeaderCache(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	_, err := client.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{})
	assert.Nil(t, err)

	// test return error code
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().InvalidateShardLeaderCache(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrServiceNotReady), nil)

	_, err = client.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{})
	assert.Nil(t, err)

	// test ctx done
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	time.Sleep(20 * time.Millisecond)
	_, err = client.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_GetSegmentsInfo(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	mockProxy.EXPECT().GetSegmentsInfo(mock.Anything, mock.Anything).Return(&internalpb.GetSegmentsInfoResponse{Status: merr.Success()}, nil)
	_, err := client.GetSegmentsInfo(ctx, &internalpb.GetSegmentsInfoRequest{})
	assert.Nil(t, err)
}

func Test_GetQuotaMetrics(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockProxy := mocks.NewMockProxyClient(t)
	client := newTestClient(mockProxy)
	defer client.Close()

	mockProxy.EXPECT().GetQuotaMetrics(mock.Anything, mock.Anything).Return(&internalpb.GetQuotaMetricsResponse{Status: merr.Success()}, nil)
	_, err := client.GetQuotaMetrics(ctx, &internalpb.GetQuotaMetricsRequest{})
	assert.Nil(t, err)
}
