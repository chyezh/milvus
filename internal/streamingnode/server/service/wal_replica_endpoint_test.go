package service

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type endpointManager struct{ walmanager.Manager }

func (*endpointManager) QueryReplicaEndpoint(*streamingpb.WALReplicaEndpointRequest) (*viewpb.StreamingWorkNode, error) {
	panic("mockey")
}

func TestWALReplicaEndpointRPC(t *testing.T) {
	mockey.PatchConvey("service reports the local installed query runtime", t, func() {
		target := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 42, WalReplicaId: 2, RuntimeId: 5, WalTerm: 7, ReadOnly: true}
		var endpointErr error
		mockey.Mock((*endpointManager).QueryReplicaEndpoint).To(func(*streamingpb.WALReplicaEndpointRequest) (*viewpb.StreamingWorkNode, error) {
			return target, endpointErr
		}).Build()
		service := NewReplicaService(&endpointManager{})
		req := &streamingpb.WALReplicaEndpointRequest{Pchannel: "p", NodeId: 42, ReplicaId: 2}
		actual, err := service.GetWALReplicaEndpoint(t.Context(), req)
		require.NoError(t, err)
		require.Same(t, target, actual)
		endpointErr = status.NewChannelFenced("p")
		_, err = service.GetWALReplicaEndpoint(t.Context(), req)
		require.Error(t, err)
		_, err = NewReplicaService(&replicaManager{}).GetWALReplicaEndpoint(t.Context(), req)
		require.Error(t, err)
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = service.GetWALReplicaEndpoint(canceled, req)
		require.ErrorIs(t, err, context.Canceled)
	})
}
