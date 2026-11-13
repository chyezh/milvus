package service

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Bootstrap has a finite lifetime, including flow-controlled sends. Its slot
// and protection remain owned by the worker until it actually exits.
func (hs *replicaServiceImpl) BootstrapWALReplica(req *streamingpb.WALReplicaBootstrapRequest, rpc streamingpb.StreamingNodeReplicaService_BootstrapWALReplicaServer) error {
	ctx, cancel := context.WithTimeout(rpc.Context(), 2*time.Minute)
	defer cancel()
	select {
	case hs.bootstrapSlots <- struct{}{}:
	default:
		return status.NewGRPCStatusFromStreamingError(status.NewOnShutdownError("WAL replica bootstrap admission exhausted")).Err()
	}
	finished := make(chan error, 1)
	go func() {
		defer func() { <-hs.bootstrapSlots }()
		finished <- hs.serveReplicaBootstrap(ctx, req, rpc.Send)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-finished:
		if err == nil {
			return nil
		}
		return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
	}
}

func (hs *replicaServiceImpl) serveReplicaBootstrap(ctx context.Context, req *streamingpb.WALReplicaBootstrapRequest, send func(*streamingpb.WALReplicaBootstrapFrame) error) error {
	channel := req.GetPchannel()
	if channel.GetName() == "" || channel.GetTerm() <= 0 || channel.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
		return status.NewInvalidArgument("WAL replica bootstrap requires a primary channel")
	}
	raw, err := hs.walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(channel))
	if err != nil {
		return err
	}
	provider, ok := wal.Unwrap(raw).(wal.ReplicaBootstrapProvider)
	if !ok {
		return status.NewOnShutdownError("protected WAL replica bootstrap is unavailable")
	}
	bootstrap, err := provider.OpenReplicaBootstrap(ctx, req.GetCheckpoint())
	if err != nil {
		return err
	}
	defer bootstrap.Close()
	snapshot := bootstrap.Snapshot()
	if err := walevent.ValidateSnapshot(channel.GetTerm(), snapshot); err != nil {
		return err
	}
	if err := walevent.WriteSnapshot(ctx, snapshot, func(chunk *streamingpb.WALReplicaSnapshotChunk) error {
		return send(&streamingpb.WALReplicaBootstrapFrame{Payload: &streamingpb.WALReplicaBootstrapFrame_Snapshot{Snapshot: chunk}})
	}); err != nil {
		return err
	}
	through, upper := snapshot.GetCheckpoint().GetTimeTick(), snapshot.GetObserved().GetTimeTick()
	for through < upper {
		if err := ctx.Err(); err != nil {
			return err
		}
		msg, err := bootstrap.Next(ctx)
		if err != nil {
			return err
		}
		if msg == nil || !msg.IsPersisted() || msg.TimeTick() <= through || msg.TimeTick() > upper {
			return merr.WrapErrDataIntegrityMsg("protected WAL replay does not continue its snapshot prefix")
		}
		if msg.TimeTick() == upper && !proto.Equal(message.MustMarshalMessageID(msg.LastConfirmedMessageID()), snapshot.Observed.MessageId) {
			return merr.WrapErrDataIntegrityMsg("protected WAL replay reached a different physical frontier")
		}
		if err := send(&streamingpb.WALReplicaBootstrapFrame{Payload: &streamingpb.WALReplicaBootstrapFrame_Replay{Replay: walevent.EncodeEntry(msg)}}); err != nil {
			return err
		}
		through = msg.TimeTick()
	}
	return send(&streamingpb.WALReplicaBootstrapFrame{Payload: &streamingpb.WALReplicaBootstrapFrame_Complete{Complete: snapshot.Cursor}})
}
