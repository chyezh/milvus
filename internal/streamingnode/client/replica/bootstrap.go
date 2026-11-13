package replica

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Bootstrap owns one finite transfer. The caller builds a private runtime with
// Snapshot and Replay, checks Complete, and only then publishes that runtime.
// Close is required on every path, including failed recovery.
type Bootstrap struct {
	Snapshot *streamingpb.WALReplicaSnapshot
	stream   streamingpb.StreamingNodeReplicaService_BootstrapWALReplicaClient
	cancel   context.CancelFunc
}

func (s *Source) Bootstrap(ctx context.Context, checkpoint *streamingpb.WALCheckpoint, opts ...grpc.CallOption) (*Bootstrap, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	opts = append([]grpc.CallOption{grpc.MaxCallRecvMsgSize(paramtable.DefaultClientMaxRecvSize)}, opts...)
	stream, err := s.events.BootstrapWALReplica(ctx, &streamingpb.WALReplicaBootstrapRequest{Pchannel: types.NewProtoFromPChannelInfo(s.primary.Channel), Checkpoint: checkpoint}, opts...)
	if err != nil {
		cancel()
		return nil, status.ConvertStreamingError("BootstrapWALReplica", err)
	}
	b := &Bootstrap{stream: stream, cancel: cancel}
	b.Snapshot, err = walevent.ReadSnapshot(ctx, func() (*streamingpb.WALReplicaSnapshotChunk, error) {
		frame, err := b.receive()
		if err != nil {
			return nil, err
		}
		if frame.GetSnapshot() == nil {
			return nil, merr.WrapErrDataIntegrityMsg("WAL replica bootstrap omitted metadata before replay")
		}
		return frame.GetSnapshot(), nil
	})
	if err == nil {
		err = walevent.ValidateSnapshot(s.primary.Channel.Term, b.Snapshot)
	}
	if err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

func (b *Bootstrap) receive() (*streamingpb.WALReplicaBootstrapFrame, error) {
	frame, err := b.stream.Recv()
	if err != nil {
		return nil, status.ConvertStreamingError("BootstrapWALReplica", err)
	}
	return frame, nil
}

// Replay never accepts metadata, completion or transport EOF as a WAL message.
// Recovery verifies the ordered logical prefix and exact physical upper bound.
func (b *Bootstrap) Replay(ctx context.Context) (message.ImmutableMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	frame, err := b.receive()
	if err != nil {
		return nil, err
	}
	if frame.GetReplay() == nil {
		return nil, merr.WrapErrDataIntegrityMsg("WAL replica bootstrap ended before replay reached its cut")
	}
	return walevent.DecodeEntry(frame.GetReplay())
}

// Complete distinguishes a fully transferred cut from a truncated response.
// A transport failure here discards the private candidate, even if replay had
// already reached its upper TimeTick.
func (b *Bootstrap) Complete(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	frame, err := b.receive()
	if err != nil {
		return err
	}
	if frame.GetComplete() == nil || !proto.Equal(frame.GetComplete(), b.Snapshot.GetCursor()) {
		return merr.WrapErrDataIntegrityMsg("WAL replica bootstrap completion does not match its cut")
	}
	_, err = b.stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return status.ConvertStreamingError("BootstrapWALReplica", err)
	}
	return merr.WrapErrDataIntegrityMsg("WAL replica bootstrap continued after completion")
}

func (b *Bootstrap) Close() { b.cancel() }
