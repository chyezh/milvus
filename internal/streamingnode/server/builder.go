package server

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ServerBuilder is used to build a server.
// All component should be initialized before server initialization should be added here.
type ServerBuilder struct {
	etcdClient *clientv3.Client
	grpcServer *grpc.Server
	rc         types.RootCoordClient
	session    *sessionutil.Session
}

// NewServerBuilder creates a new server builder.
func NewServerBuilder() *ServerBuilder {
	return &ServerBuilder{}
}

// WithETCD sets etcd client to the server builder.
func (b *ServerBuilder) WithETCD(e *clientv3.Client) *ServerBuilder {
	b.etcdClient = e
	return b
}

// WithGRPCServer sets grpc server to the server builder.
func (b *ServerBuilder) WithGRPCServer(svr *grpc.Server) *ServerBuilder {
	b.grpcServer = svr
	return b
}

// WithRootCoordClient sets root coord client to the server builder.
func (b *ServerBuilder) WithRootCoordClient(rc types.RootCoordClient) *ServerBuilder {
	b.rc = rc
	return b
}

// WithSession sets session to the server builder.
func (b *ServerBuilder) WithSession(session *sessionutil.Session) *ServerBuilder {
	b.session = session
	return b
}

// Build builds a streaming node server.
func (s *ServerBuilder) Build() *Server {
	resource.Init(
		resource.OptETCD(s.etcdClient),
		resource.OptRootCoordClient(s.rc),
	)
	return &Server{
		session:               s.session,
		grpcServer:            s.grpcServer,
		componentStateService: componentutil.NewComponentStateService(typeutil.StreamingNodeRole),
	}
}
