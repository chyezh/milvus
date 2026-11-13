package service

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	replicahandler "github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/replicadiscovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type runtimeWatchManager struct {
	walmanager.Manager
	wal.QueryReplicaManager
}

func (*runtimeWatchManager) GetAvailableQueryReplica(*viewpb.StreamingWorkNode) (wal.WAL, error) {
	panic("mockey")
}

type runtimeWatchRPC struct {
	streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer
}

func (*runtimeWatchRPC) Context() context.Context { panic("mockey") }

func TestReplicaWatchBindsLocalReadOnlyRuntime(t *testing.T) {
	for _, failure := range []string{"identity", "channel", "term", "mode", "unsupported", "stale", "success"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("directory lookup never falls back to writer assignment", t, func() {
				target := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 42, WalReplicaId: 3, WalReplicaRevision: 4, RuntimeId: 5, WalTerm: 7, ReadOnly: true}
				channel := &streamingpb.PChannelInfo{Name: "p", Term: 7, AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY}
				var lookupErr error
				var manager walmanager.Manager = &runtimeWatchManager{}
				switch failure {
				case "identity":
					target.RuntimeId = 0
				case "channel":
					channel.Name = "other"
				case "term":
					channel.Term = 8
				case "mode":
					channel.AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE
				case "unsupported":
					manager = &replicaManager{}
				case "stale":
					lookupErr = status.NewChannelFenced("p")
				}
				outgoing, err := worknodehandler.EncodeQueryReplicaToOutgoingContext(t.Context(), target)
				require.NoError(t, err)
				outgoing = contextutil.WithCreateReplicaWatchStream(outgoing, &streamingpb.CreateReplicaWatchStreamRequest{Pchannel: channel})
				md, _ := metadata.FromOutgoingContext(outgoing)
				incoming := metadata.NewIncomingContext(t.Context(), md)
				mockey.Mock((*runtimeWatchRPC).Context).Return(incoming).Build()
				lookup := mockey.Mock((*runtimeWatchManager).GetAvailableQueryReplica).To(func(actual *viewpb.StreamingWorkNode) (wal.WAL, error) {
					require.True(t, proto.Equal(target, actual))
					return &replicaWAL{}, lookupErr
				}).Build()
				directory := snview.NewReplicaDirectoryForTest()
				defer directory.Close()
				stream := directory.AcquireStream(t.Context(), "p", nil)
				defer stream.Close()
				mockey.Mock((*replicaWAL).AcquireReplicaWatchStream).Return(stream, nil).Build()
				serve := mockey.Mock(replicahandler.ServeStream).Return(nil).Build()
				err = acquireAndServeReplicaWatch(manager, &runtimeWatchRPC{})
				if failure == "success" {
					require.NoError(t, err)
					require.Equal(t, 1, serve.Times())
				} else {
					require.Error(t, err)
					require.Zero(t, serve.Times())
				}
				if failure == "success" || failure == "stale" {
					require.Equal(t, 1, lookup.Times())
				} else {
					require.Zero(t, lookup.Times())
				}
			})
		})
	}
}
