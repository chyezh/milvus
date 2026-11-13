package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestServerRegistersReplicaTransport(t *testing.T) {
	paramtable.Init()
	server := grpc.NewServer()
	defer server.Stop()
	s := &Server{handlerService: service.NewHandlerService(nil), managerService: service.NewManagerService(nil)}
	s.registerGRPCService(server)
	info := server.GetServiceInfo()["milvus.proto.streaming.StreamingNodeReplicaService"]
	require.Len(t, info.Methods, 1)
	require.Equal(t, "SubscribeWALEvents", info.Methods[0].Name)
	require.True(t, info.Methods[0].IsServerStream)
	require.False(t, info.Methods[0].IsClientStream)
}
