package streaming

import (
	"context"

	resumablereplicas "github.com/milvus-io/milvus/internal/distributed/streaming/internal/replicadiscovery"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The production HandlerClient is constructed with session discovery. Resolve
// the member on every reconnect, then keep that runtime identity for the entire
// physical View-directory attempt.
func replicaWatchFactory(client handler.HandlerClient) resumablereplicas.ReplicaStreamFactory {
	endpoints := client.(handler.WALReplicaDiscoveryClient)
	return func(ctx context.Context, pc string, member *streamingpb.WALReplicaAssignment) (wal.ReplicaWatchStream, qviews.StreamingNode, error) {
		endpoint, err := endpoints.GetWALReplicaEndpoint(ctx, pc, member)
		if err != nil {
			return nil, qviews.StreamingNode{}, err
		}
		ctx, err = worknodehandler.EncodeQueryReplicaToOutgoingContext(ctx, endpoint)
		if err != nil {
			return nil, qviews.StreamingNode{}, err
		}
		stream, err := client.AcquireReplicaWatchStream(ctx, pc)
		return stream, qviews.StreamingNodeFromProto(endpoint), err
	}
}

// QueryViewClient combines owner-routed query RPCs and resumable discovery.
// HandlerClient itself provides only physical, non-resuming operations.
type QueryViewClient interface {
	handler.QueryViewClient
	SubscribeReplicas(string, wal.ReplicaWatchHandler) (wal.ReplicaWatchSubscription, error)
}

type queryViewClient struct {
	handler.QueryViewClient
	owner *walAccesserImpl
}

func (w *walAccesserImpl) QueryViewClient() QueryViewClient {
	return queryViewClient{QueryViewClient: w.handlerClient.QueryViewClient(), owner: w}
}

func (c queryViewClient) SubscribeReplicas(vc string, h wal.ReplicaWatchHandler) (wal.ReplicaWatchSubscription, error) {
	if !c.owner.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer c.owner.lifetime.Done()
	return c.owner.replicaClient.Subscribe(vc, h)
}
