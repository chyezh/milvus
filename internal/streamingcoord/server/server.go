package server

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/collector"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/layout"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server is the streamingcoord server.
type Server struct {
	// session of current server.
	session sessionutil.SessionInterface

	// basic component variables managed by external.
	etcdClient *clientv3.Client
	catalog    metastore.StreamingCoordCataLog

	// service level variables.
	channelService    service.ChannelService
	assignmentService service.AssignmentService

	// basic component variables can be used at service level.
	channelMeta            channel.Meta
	balancer               balancer.Balancer
	logNodeManager         manager.ManagerClient                // manage the streamingnode client.
	logNodeStatusCollector *collector.Collector                 // collect streamingnode status.
	componentStateService  *componentutil.ComponentStateService // state.
}

// NewStreamingCoord creates a new streamingcoord server.
func NewStreamingCoord() *Server {
	return &Server{
		componentStateService: componentutil.NewComponentStateService(typeutil.StreamingCoordRole),
	}
}

// Init initializes the streamingcoord server.
func (s *Server) Init(ctx context.Context) (err error) {
	log.Info("init streamingcoord server...")
	s.componentStateService.OnInitializing()

	// Init all underlying component of streamingcoord server.
	if err := s.initBasicComponent(ctx); err != nil {
		log.Error("init basic component of streamingcoord server failed", zap.Error(err))
		return err
	}
	// Init all grpc service of streamingcoord server.
	s.initService(ctx)
	s.componentStateService.OnInitialized(s.session.GetServerID())
	log.Info("streamingcoord server initialized")
	return nil
}

// initBasicComponent initialize all underlying dependency for streamingcoord.
func (s *Server) initBasicComponent(ctx context.Context) error {
	// Create layout.
	// 1. Get PChannel Meta from catalog.
	log.Info("init channel meta...")
	channelDatas, err := s.catalog.ListPChannel(ctx)
	if err != nil {
		return status.NewInner("fail to list all PChannel info, %s", err.Error())
	}
	channels := make(map[string]channel.PhysicalChannel, len(channelDatas))
	for name, data := range channelDatas {
		channels[name] = channel.NewPhysicalChannel(s.catalog, data)
	}
	s.channelMeta = channel.NewMeta(s.catalog, channels)
	log.Info("init channel meta done")

	// 2. Connect to log node manager to manage and query status of underlying log nodes.
	log.Info("dial to log node manager...")
	s.logNodeManager = manager.DialContext(ctx, s.etcdClient)
	log.Info("dial to log node manager done")

	// 3. Create layout of cluster.
	// Fetch copy of pChannels from meta storage.
	pChannels := s.channelMeta.GetPChannels(ctx)
	// Fetch node status from log nodes.
	log.Info("fetch log node status...")
	nodeStatus, err := s.logNodeManager.CollectAllStatus(ctx)
	if err != nil {
		return status.NewInner("collect all streamingnode status failed, %s", err.Error())
	}
	layout := layout.NewLayout(pChannels, nodeStatus)
	log.Info("fetch log node status done")

	// 4. Create Balancer.
	s.balancer = balancer.NewBalancer(balancer.NewChannelCountFairPolicy(), layout, s.logNodeManager)

	// 5. Create a node status collector.
	s.logNodeStatusCollector = collector.NewCollector(s.logNodeManager, s.balancer)
	return nil
}

// initService initializes the grpc service.
func (s *Server) initService(ctx context.Context) {
	s.channelService = service.NewChannelService(s.channelMeta, s.balancer)
	s.assignmentService = service.NewAssignmentService(s.balancer, s.logNodeStatusCollector)
}

// registerGRPCService register all grpc service to grpc server.
func (s *Server) RegisterGRPCService(grpcServer *grpc.Server) {
	streamingpb.RegisterStreamingCoordAssignmentServiceServer(grpcServer, s.assignmentService)
	streamingpb.RegisterStreamingCoordChannelServiceServer(grpcServer, s.channelService)
	streamingpb.RegisterStreamingCoordStateServiceServer(grpcServer, s.componentStateService)
}

// Start starts the streamingcoord server.
func (s *Server) Start() {
	log.Info("start streamingcoord server")
	s.logNodeStatusCollector.Start()
	log.Info("streamingcoord service started")
}

// Stop stops the streamingcoord server.
func (s *Server) Stop() {
	s.logNodeStatusCollector.Stop()
	log.Info("stopping streamingcoord server...")
	s.componentStateService.OnStopping()
	log.Info("close balancer...")
	s.balancer.Close()
	log.Info("close log node manager client...")
	s.logNodeManager.Close()
	log.Info("close log node grpc conn...")
	log.Info("streamingcoord server stopped")
}
