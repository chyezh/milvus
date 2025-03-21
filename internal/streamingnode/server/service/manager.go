package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/service/manager"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ ManagerService = (*managerServiceImpl)(nil)

// NewManagerService create a streamingnode manager service.
func NewManagerService(m walmanager.Manager) ManagerService {
	return &managerServiceImpl{
		lifetime:   typeutil.NewLifetime(),
		walManager: m,
	}
}

type ManagerService interface {
	streamingpb.StreamingNodeManagerServiceServer

	Close()
}

// managerServiceImpl implements ManagerService.
// managerServiceImpl is just a rpc level to handle incoming grpc.
// all manager logic should be done in wal.Manager.
type managerServiceImpl struct {
	ctx    context.Context
	cancel context.CancelFunc

	lifetime   *typeutil.Lifetime
	walManager walmanager.Manager
}

// Sync syncs the assignment.
func (m *managerServiceImpl) Sync(svr streamingpb.StreamingNodeManagerService_SyncServer) error {
	if !m.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("manager grpc service is closed")
	}
	defer m.lifetime.Done()

	s := manager.NewAssignmentSyncServer(m.ctx, svr, m.walManager)
	return s.Execute()
}

// Close closes the manager service.
func (m *managerServiceImpl) Close() {
	m.lifetime.SetState(typeutil.LifetimeStateStopped)
	m.cancel()
	m.lifetime.Wait()
}
