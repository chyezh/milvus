package server

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/lognode/server/service"
	"github.com/milvus-io/milvus/internal/lognode/server/timetick"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// Server is the lognode server.
type Server struct {
	// session of current server.
	session *sessionutil.Session

	// basic component variables managed by external.
	rc         types.RootCoordClient
	etcdClient *clientv3.Client
	grpcServer *grpc.Server

	// service level instances.
	handlerService service.HandlerService
	managerService service.ManagerService

	// basic component instances.
	walManager            walmanager.Manager
	componentStateService *componentutil.ComponentStateService // state.
}

// Init initializes the lognode server.
func (s *Server) Init(ctx context.Context) (err error) {
	log.Info("init lognode server...")
	s.componentStateService.OnInitializing()
	// init all basic components.
	s.initBasicComponent(ctx)

	// init all service.
	s.initService(ctx)
	log.Info("lognode server initialized")
	s.componentStateService.OnInitialized(s.session.ServerID)
	return nil
}

// Start starts the lognode server.
func (s *Server) Start() {
	log.Info("start lognode server")

	log.Info("lognode service started")
}

// Stop stops the lognode server.
func (s *Server) Stop() {
	log.Info("stopping lognode server...")
	s.componentStateService.OnStopping()
	log.Info("close wal manager...")
	s.walManager.Close()
	log.Info("lognode server stopped")
}

// Health returns the health status of the lognode server.
func (s *Server) Health(ctx context.Context) commonpb.StateCode {
	resp, _ := s.componentStateService.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	return resp.State.StateCode
}

// initBasicComponent initialize all underlying dependency for lognode.
func (s *Server) initBasicComponent(ctx context.Context) {
	var err error
	s.walManager, err = walmanager.OpenManager(
		&walmanager.OpenOption{
			InterceptorBuilders: []walimpls.InterceptorBuilder{
				timetick.NewInterceptorBuilder(s.rc),
			},
		},
	)
	if err != nil {
		panic("open wal manager failed")
	}
}

// initService initializes the grpc service.
func (s *Server) initService(ctx context.Context) {
	s.handlerService = service.NewHandlerService(s.walManager)
	s.managerService = service.NewManagerService(s.walManager)
	s.registerGRPCService(s.grpcServer)
}

// registerGRPCService register all grpc service to grpc server.
func (s *Server) registerGRPCService(grpcServer *grpc.Server) {
	logpb.RegisterLogNodeHandlerServiceServer(grpcServer, s.handlerService)
	logpb.RegisterLogNodeManagerServiceServer(grpcServer, s.managerService)
	logpb.RegisterLogNodeStateServiceServer(grpcServer, s.componentStateService)
}
