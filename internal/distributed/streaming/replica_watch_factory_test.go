package streaming

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type endpointWatchHandler struct{ handler.HandlerClient }

func (*endpointWatchHandler) GetWALReplicaEndpoint(context.Context, string, *streamingpb.WALReplicaAssignment) (*viewpb.StreamingWorkNode, error) {
	panic("mockey")
}

func (*endpointWatchHandler) AcquireReplicaWatchStream(context.Context, string) (wal.ReplicaWatchStream, error) {
	panic("mockey")
}

func TestReplicaWatchFactoryBindsEachAttempt(t *testing.T) {
	mockey.PatchConvey("reconnect resolves the member again before opening its local directory", t, func() {
		identity := qviews.StreamingNode{PChannel: "p", NodeID: 2, WALReplicaID: 3, WALReplicaRevision: 4, RuntimeID: 5, WALTerm: 7, ReadOnly: true}
		var resolveErr error
		resolve := mockey.Mock((*endpointWatchHandler).GetWALReplicaEndpoint).To(func(context.Context, string, *streamingpb.WALReplicaAssignment) (*viewpb.StreamingWorkNode, error) {
			return identity.IntoProto(), resolveErr
		}).Build()
		directory := snview.NewReplicaDirectoryForTest()
		defer directory.Close()
		mockey.Mock((*endpointWatchHandler).AcquireReplicaWatchStream).To(func(ctx context.Context, pc string) (wal.ReplicaWatchStream, error) {
			actual, err := worknodehandler.DecodeQueryReplicaFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, identity, qviews.StreamingNodeFromProto(actual))
			return directory.AcquireStream(ctx, pc, nil), nil
		}).Build()
		factory := replicaWatchFactory(&endpointWatchHandler{})
		for range 2 {
			stream, actual, err := factory(t.Context(), "p", &streamingpb.WALReplicaAssignment{ReplicaId: 3})
			require.NoError(t, err)
			require.Equal(t, identity, actual)
			require.NoError(t, stream.Close())
			identity.RuntimeID++
		}
		resolveErr = merr.WrapErrServiceUnavailable("member preparing")
		stream, _, err := factory(t.Context(), "p", &streamingpb.WALReplicaAssignment{ReplicaId: 3})
		require.Error(t, err)
		require.Nil(t, stream)
		require.Equal(t, 3, resolve.Times())
	})
}
