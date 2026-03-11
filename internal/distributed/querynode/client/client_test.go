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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// mockService implements lazygrpc.Service[querypb.QueryNodeClient] for testing.
type mockService struct {
	client querypb.QueryNodeClient
	err    error
}

func (m *mockService) GetService(ctx context.Context) (querypb.QueryNodeClient, error) {
	return m.client, m.err
}

func (m *mockService) GetConn(ctx context.Context) (*grpc.ClientConn, error) {
	return nil, nil
}

func (m *mockService) Close() {}

func Test_NewClient(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	client, err := NewClient(ctx, "", 1)
	assert.Nil(t, client)
	assert.Error(t, err)
}

func Test_RPCMethods_ServiceError(t *testing.T) {
	paramtable.Init()

	c := &Client{
		service: &mockService{err: errors.New("dummy")},
		nodeID:  1,
	}

	ctx := context.Background()

	checkFunc := func(retNotNil bool) {
		retCheck := func(notNil bool, ret any, err error) {
			if notNil {
				assert.NotNil(t, ret)
				assert.NoError(t, err)
			} else {
				assert.Nil(t, ret)
				assert.Error(t, err)
			}
		}

		r1, err := c.GetComponentStates(ctx, nil)
		retCheck(retNotNil, r1, err)

		r2, err := c.GetTimeTickChannel(ctx, nil)
		retCheck(retNotNil, r2, err)

		r3, err := c.GetStatisticsChannel(ctx, nil)
		retCheck(retNotNil, r3, err)

		r6, err := c.WatchDmChannels(ctx, nil)
		retCheck(retNotNil, r6, err)

		r7, err := c.LoadSegments(ctx, nil)
		retCheck(retNotNil, r7, err)

		r8, err := c.ReleaseCollection(ctx, nil)
		retCheck(retNotNil, r8, err)

		r8, err = c.LoadPartitions(ctx, nil)
		retCheck(retNotNil, r8, err)

		r9, err := c.ReleasePartitions(ctx, nil)
		retCheck(retNotNil, r9, err)

		r10, err := c.ReleaseSegments(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := c.GetSegmentInfo(ctx, nil)
		retCheck(retNotNil, r11, err)

		r12, err := c.GetMetrics(ctx, nil)
		retCheck(retNotNil, r12, err)

		r14, err := c.Search(ctx, nil)
		retCheck(retNotNil, r14, err)

		r15, err := c.Query(ctx, nil)
		retCheck(retNotNil, r15, err)

		r16, err := c.SyncReplicaSegments(ctx, nil)
		retCheck(retNotNil, r16, err)

		r17, err := c.GetStatistics(ctx, nil)
		retCheck(retNotNil, r17, err)

		r18, err := c.ShowConfigurations(ctx, nil)
		retCheck(retNotNil, r18, err)

		r19, err := c.QuerySegments(ctx, nil)
		retCheck(retNotNil, r19, err)

		r20, err := c.SearchSegments(ctx, nil)
		retCheck(retNotNil, r20, err)

		r21, err := c.DeleteBatch(ctx, nil)
		retCheck(retNotNil, r21, err)

		r22, err := c.RunAnalyzer(ctx, nil)
		retCheck(retNotNil, r22, err)

		r23, err := c.ValidateAnalyzer(ctx, nil)
		retCheck(retNotNil, r23, err)

		r24, err := c.GetHighlight(ctx, nil)
		retCheck(retNotNil, r24, err)

		// stream rpc
		qs, err := c.QueryStream(ctx, nil)
		retCheck(retNotNil, qs, err)
	}

	// Test error case: service returns error
	checkFunc(false)
}

func Test_RPCMethods_ClientError(t *testing.T) {
	paramtable.Init()

	c := &Client{
		service: &mockService{client: &mock.GrpcQueryNodeClient{Err: errors.New("dummy")}},
		nodeID:  1,
	}

	ctx := context.Background()

	r1, err := c.GetComponentStates(ctx, nil)
	assert.Nil(t, r1)
	assert.Error(t, err)

	r2, err := c.Search(ctx, nil)
	assert.Nil(t, r2)
	assert.Error(t, err)
}

func Test_RPCMethods_Success(t *testing.T) {
	paramtable.Init()

	c := &Client{
		service: &mockService{client: &mock.GrpcQueryNodeClient{Err: nil}},
		nodeID:  1,
	}

	ctx := context.Background()

	r1, err := c.GetComponentStates(ctx, nil)
	assert.NotNil(t, r1)
	assert.NoError(t, err)

	r2, err := c.Search(ctx, nil)
	assert.NotNil(t, r2)
	assert.NoError(t, err)

	r3, err := c.Query(ctx, nil)
	assert.NotNil(t, r3)
	assert.NoError(t, err)

	r4, err := c.WatchDmChannels(ctx, nil)
	assert.NotNil(t, r4)
	assert.NoError(t, err)

	r5, err := c.GetMetrics(ctx, nil)
	assert.NotNil(t, r5)
	assert.NoError(t, err)

	r6, err := c.GetDataDistribution(ctx, nil)
	assert.NotNil(t, r6)
	assert.NoError(t, err)

	r7, err := c.SyncDistribution(ctx, nil)
	assert.NotNil(t, r7)
	assert.NoError(t, err)
}

func Test_Close(t *testing.T) {
	c := &Client{
		service: &mockService{},
		nodeID:  1,
	}
	err := c.Close()
	assert.NoError(t, err)
}
