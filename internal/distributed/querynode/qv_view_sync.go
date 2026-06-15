package grpcquerynode

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	qn "github.com/milvus-io/milvus/internal/querynodev2"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/internal/views/worknode/qnview"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func registerQueryViewSyncServer(grpcServer *grpc.Server, segMgr qnview.SegmentManager) {
	viewpb.RegisterViewSyncServiceServer(grpcServer, &queryNodeViewSyncServer{
		ViewSyncServer: handler.NewViewSyncServer(qnview.NewQNQueryViewHandler(segMgr)),
	})
}

type queryNodeViewSyncServer struct {
	viewpb.UnimplementedViewSyncServiceServer
	*handler.ViewSyncServer
}

func (s *queryNodeViewSyncServer) SyncQueryView(stream viewpb.ViewSyncService_SyncQueryViewServer) error {
	return s.ViewSyncServer.SyncQueryView(stream)
}

func (s *Server) registerQueryViewSyncServer() {
	registerQueryViewSyncServer(s.grpcServer, &lazyQNSegmentManager{
		build: func() qnview.SegmentManager {
			qnImpl, ok := s.querynode.(*qn.QueryNode)
			if !ok {
				return nil
			}
			return qnImpl.NewQueryViewSegmentManager(
				&lazyQueryViewMetadataProvider{mixCoord: s.mixCoord},
				queryViewTransformLogAccesser(),
			)
		},
	})
}

type lazyQNSegmentManager struct {
	build func() qnview.SegmentManager

	mu      sync.Mutex
	manager qnview.SegmentManager
}

func (m *lazyQNSegmentManager) Acquire(req qnview.AcquireSegments) {
	manager := m.get()
	if manager == nil {
		go func() {
			if req.OnUnrecoverable != nil {
				req.OnUnrecoverable()
			}
		}()
		return
	}
	manager.Acquire(req)
}

func (m *lazyQNSegmentManager) Release(req qnview.ReleaseSegments) {
	manager := m.get()
	if manager == nil {
		go func() {
			if req.OnDropped != nil {
				req.OnDropped()
			}
		}()
		return
	}
	manager.Release(req)
}

func (m *lazyQNSegmentManager) get() qnview.SegmentManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.manager == nil && m.build != nil {
		m.manager = m.build()
	}
	return m.manager
}

func queryViewTransformLogAccesser() wal.TransformLogAccesser {
	wal := streaming.WAL()
	if wal == nil {
		return nil
	}
	return wal.TransformLog()
}

type lazyQueryViewMetadataProvider struct {
	mixCoord *syncutil.Future[types.MixCoordClient]
}

func (p *lazyQueryViewMetadataProvider) client(ctx context.Context) (types.MixCoordClient, error) {
	if p.mixCoord == nil {
		return nil, merr.WrapErrServiceUnavailable("mixcoord client is not initialized")
	}
	return p.mixCoord.GetWithContext(ctx)
}

func (p *lazyQueryViewMetadataProvider) DescribeCollection(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	client, err := p.client(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection)),
		// Collection name alone is ambiguous after database support.
		CollectionID: collectionID,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("failed to describe collection for query view", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (p *lazyQueryViewMetadataProvider) GetSegmentInfo(ctx context.Context, segmentIDs ...int64) ([]*datapb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	client, err := p.client(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs:       segmentIDs,
		IncludeUnHealthy: true,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("failed to get segment info for query view", zap.Int64s("segmentIDs", segmentIDs), zap.Error(err))
		return nil, err
	}
	if len(resp.GetInfos()) == 0 && len(segmentIDs) > 0 {
		return nil, merr.WrapErrSegmentNotFound(segmentIDs[0], "no such segment in DataCoord")
	}
	if len(segmentIDs) > 0 {
		returned := make(map[int64]struct{}, len(resp.GetInfos()))
		for _, info := range resp.GetInfos() {
			returned[info.GetID()] = struct{}{}
		}
		for _, segmentID := range segmentIDs {
			if _, ok := returned[segmentID]; !ok {
				return nil, merr.WrapErrSegmentNotFound(segmentID, "no such segment in DataCoord")
			}
		}
	}
	if err := binlog.DecompressMultiBinLogs(resp.GetInfos()); err != nil {
		return nil, err
	}
	return resp.GetInfos(), nil
}

func (p *lazyQueryViewMetadataProvider) ListIndexes(ctx context.Context, collectionID int64) ([]*indexpb.IndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	client, err := p.client(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.ListIndexes(ctx, &indexpb.ListIndexesRequest{CollectionID: collectionID})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("failed to list indexes for query view", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}
	return resp.GetIndexInfos(), nil
}

func (p *lazyQueryViewMetadataProvider) GetIndexInfo(ctx context.Context, collectionID int64, segmentIDs ...int64) (map[int64][]*querypb.FieldIndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	client, err := p.client(ctx)
	if err != nil {
		return nil, err
	}
	var resp *indexpb.GetIndexInfoResponse
	retry.Do(ctx, func() error {
		resp, err = client.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
			CollectionID: collectionID,
			SegmentIDs:   segmentIDs,
		})
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("failed to get index info for query view", zap.Int64("collectionID", collectionID), zap.Int64s("segmentIDs", segmentIDs), zap.Error(err))
		return nil, err
	}
	if len(resp.GetSegmentInfo()) == 0 {
		return nil, merr.WrapErrIndexNotFoundForSegments(segmentIDs)
	}
	indexes := make(map[int64][]*querypb.FieldIndexInfo, len(resp.GetSegmentInfo()))
	for _, segmentID := range segmentIDs {
		segmentInfo, ok := resp.GetSegmentInfo()[segmentID]
		if !ok || len(segmentInfo.GetIndexInfos()) == 0 {
			return nil, merr.WrapErrIndexNotFoundForSegments(segmentIDs)
		}
	}
	for segmentID, segmentInfo := range resp.GetSegmentInfo() {
		for _, info := range segmentInfo.GetIndexInfos() {
			indexes[segmentID] = append(indexes[segmentID], &querypb.FieldIndexInfo{
				FieldID:                   info.GetFieldID(),
				EnableIndex:               true,
				IndexName:                 info.GetIndexName(),
				IndexID:                   info.GetIndexID(),
				BuildID:                   info.GetBuildID(),
				IndexParams:               info.GetIndexParams(),
				IndexFilePaths:            info.GetIndexFilePaths(),
				IndexSize:                 int64(info.GetMemSize()),
				IndexVersion:              info.GetIndexVersion(),
				NumRows:                   info.GetNumRows(),
				CurrentIndexVersion:       info.GetCurrentIndexVersion(),
				CurrentScalarIndexVersion: info.GetCurrentScalarIndexVersion(),
				IndexStorePathVersion:     info.GetIndexStorePathVersion(),
			})
		}
	}
	return indexes, nil
}
