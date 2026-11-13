package recovery

import (
	"context"

	"google.golang.org/grpc"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// SubscribeAndApply runs one transport attempt against a primary. The caller
// owns retry and source selection. Failure never closes the local query runtime
// or advances its cursor to the last received-but-unapplied event.
func (r *ReadOnlyRecoveryStorage) SubscribeAndApply(ctx context.Context, client streamingpb.StreamingNodeReplicaServiceClient, primary types.PChannelInfo, opts ...grpc.CallOption) error {
	ctx, cancel := contextutil.MergeContext(ctx, r.ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	r.sourceMu.Lock()
	if r.sourceCancel != nil {
		r.sourceCancel()
	}
	r.sourceAttempt++
	attempt := r.sourceAttempt
	r.sourceCancel = cancel
	r.sourceMu.Unlock()
	after := r.Applied()
	// Logical transactions can exceed gRPC's 4 MB default. Retain Milvus's
	// bounded transport limit; an explicit caller limit takes precedence.
	opts = append([]grpc.CallOption{grpc.MaxCallRecvMsgSize(paramtable.DefaultClientMaxRecvSize)}, opts...)
	stream, err := client.SubscribeWALEvents(ctx, &streamingpb.WALReplicaSubscribeRequest{Pchannel: types.NewProtoFromPChannelInfo(primary), After: after.IntoProto()}, opts...)
	if err != nil {
		return status.ConvertStreamingError("SubscribeWALEvents", err)
	}
	for {
		pb, err := stream.Recv()
		if err != nil {
			for _, detail := range grpcstatus.Convert(err).Details() {
				if current, ok := detail.(*streamingpb.WALReplicaCursor); ok {
					return walevent.NewResumeRequired(r.Applied(), walevent.CursorFromProto(current))
				}
			}
			return status.ConvertStreamingError("SubscribeWALEvents", err)
		}
		event, err := walevent.EventFromProto(pb)
		if err != nil {
			return merr.Wrap(err, "decode remote WAL event")
		}
		if err := r.applySubscribedEvent(ctx, attempt, event); err != nil {
			return err
		}
	}
}

func (r *ReadOnlyRecoveryStorage) applySubscribedEvent(ctx context.Context, attempt uint64, event walevent.Event) error {
	r.sourceMu.Lock()
	defer r.sourceMu.Unlock()
	if attempt != r.sourceAttempt {
		return context.Canceled
	}
	return r.Apply(ctx, event)
}
