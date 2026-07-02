package manager

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ ManagerClient = (*managerClientImpl)(nil)

// managerClientImpl implements ManagerClient using etcd session discovery.
type managerClientImpl struct {
	lifetime *typeutil.Lifetime
	stopped  chan struct{}

	rb      resolver.Builder
	service lazygrpc.Service[viewpb.ViewSyncServiceClient]
}

func (c *managerClientImpl) WatchNodeChanged(ctx context.Context) (<-chan struct{}, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("querynode manager client is closing")
	}
	defer c.lifetime.Done()

	resultCh := make(chan struct{}, 1)
	go func() {
		defer close(resultCh)
		c.rb.Resolver().Watch(ctx, func(state resolver.VersionedState) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.stopped:
				return status.NewOnShutdownError("querynode manager client is closing")
			case resultCh <- struct{}{}:
			}
			return nil
		})
	}()
	return resultCh, nil
}

func (c *managerClientImpl) GetAllQueryNodes(ctx context.Context) (map[int64]*NodeInfo, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("querynode manager client is closing")
	}
	defer c.lifetime.Done()

	state, err := c.rb.Resolver().GetLatestState(ctx)
	if err != nil {
		return nil, err
	}

	result := make(map[int64]*NodeInfo, len(state.State.Addresses))
	for serverID, session := range state.Sessions() {
		result[serverID] = &NodeInfo{
			ServerID: serverID,
			Address:  session.Address,
		}
	}
	return result, nil
}

func (c *managerClientImpl) CreateViewSyncClient(ctx context.Context, queryNodeID int64) (viewpb.ViewSyncServiceClient, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("querynode manager client is closing")
	}
	defer c.lifetime.Done()

	client, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}
	return &routedViewSyncServiceClient{
		queryNodeID: queryNodeID,
		client:      client,
	}, nil
}

func (c *managerClientImpl) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	close(c.stopped)
	c.lifetime.Wait()

	c.service.Close()
	c.rb.Close()
}

type routedViewSyncServiceClient struct {
	queryNodeID int64
	client      viewpb.ViewSyncServiceClient
}

func (c *routedViewSyncServiceClient) SyncQueryView(ctx context.Context, opts ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	return c.client.SyncQueryView(contextutil.WithPickServerID(ctx, c.queryNodeID), opts...)
}

func (c *routedViewSyncServiceClient) SyncDataView(ctx context.Context, in *viewpb.SyncDataViewRequest, opts ...grpc.CallOption) (*viewpb.SyncDataViewResponse, error) {
	return c.client.SyncDataView(contextutil.WithPickServerID(ctx, c.queryNodeID), in, opts...)
}
