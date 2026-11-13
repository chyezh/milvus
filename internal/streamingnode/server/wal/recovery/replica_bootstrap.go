package recovery

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReplicaReplay reads complete logical messages from the snapshot's physical
// restart position. Ordering and transaction assembly precede TimeTick filtering.
// The bootstrap transport owns the scanner and its bounded retention lease.
type ReplicaReplay func(context.Context) (message.ImmutableMessage, error)

// BootstrapReadOnlyRecoveryStorage creates a private runtime, replays the
// metadata image to its logical cut, and applies the remaining completion facts.
// It returns no partially initialized runtime on failure. Its owner registers
// QueryView intent only after this returns, then consumes the cut's S+1 suffix.
func BootstrapReadOnlyRecoveryStorage(ctx context.Context, cfg ReadOnlyConfig, snapshot *streamingpb.WALReplicaSnapshot, replay ReplicaReplay, opts ...RecoveryStorageOption) (_ *ReadOnlyRecoveryStorage, err error) {
	if err := validateReplicaSnapshot(cfg.Channel.Term, snapshot); err != nil {
		return nil, err
	}
	cfg.Applied = walevent.CursorFromProto(snapshot.Cursor)
	cfg.Checkpoint = utility.NewWALCheckpointFromProto(snapshot.Checkpoint)
	cfg.Manifest = snapshot.GetStorage().GetSummary()
	if cfg.Modules.VChannelMetas, err = vchannelMetaMap(snapshot.Vchannels); err != nil {
		return nil, err
	}
	if cfg.Modules.Segments, err = segmentAssignmentMetaMap(snapshot.Segments); err != nil {
		return nil, err
	}
	r, err := NewReadOnlyRecoveryStorage(cfg, opts...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			r.Close()
		}
	}()
	if err = r.replayReplicaSnapshot(ctx, snapshot, replay); err != nil {
		return nil, err
	}
	storage := snapshot.GetStorage()
	if err = r.applyEvent(ctx, walevent.Event{Payload: walevent.StoragePublished{
		VChannels: storage.GetVchannels(), Segments: storage.GetSegments(),
	}}); err != nil {
		return nil, err
	}
	// Scoped MVCC barriers may be volatile, so retained WAL replay cannot
	// reconstruct them. Install them only after replay and storage coverage.
	for name, tick := range snapshot.QueryBarriers {
		if module := r.vchannelManager.Module(name); module != nil {
			r.summaryManager.ObserveQueryBarrier(name, tick)
			module.ObserveQueryBarrier(ctx, tick)
		}
	}
	return r, nil
}

func validateReplicaSnapshot(term int64, snapshot *streamingpb.WALReplicaSnapshot) error {
	if snapshot.GetCursor().GetTerm() != term || term <= 0 {
		return merr.WrapErrServiceUnavailable("WAL replica snapshot belongs to another primary term")
	}
	cp, observed := snapshot.GetCheckpoint(), snapshot.GetObserved()
	if cp.GetLogGeneration() != observed.GetLogGeneration() {
		return merr.WrapErrDataIntegrityMsg("WAL replica snapshot crosses log generations")
	}
	if cp.GetMessageId() == nil || observed.GetMessageId() == nil || cp.GetTimeTick() > observed.GetTimeTick() {
		return merr.WrapErrDataIntegrityMsg("WAL replica snapshot has an invalid replay interval")
	}
	start, err := message.UnmarshalMessageID(cp.MessageId)
	if err != nil {
		return merr.Wrap(err, "decode replica checkpoint")
	}
	end, err := message.UnmarshalMessageID(observed.MessageId)
	if err != nil {
		return merr.Wrap(err, "decode replica replay frontier")
	}
	if start.WALName() != end.WALName() || end.LT(start) || (cp.TimeTick == observed.TimeTick && !start.EQ(end)) {
		return merr.WrapErrDataIntegrityMsg("WAL replica snapshot replay frontiers disagree")
	}
	return nil
}

func (r *ReadOnlyRecoveryStorage) replayReplicaSnapshot(ctx context.Context, snapshot *streamingpb.WALReplicaSnapshot, replay ReplicaReplay) error {
	through := snapshot.Checkpoint.TimeTick
	upper := snapshot.Observed.TimeTick
	for through < upper {
		if err := ctx.Err(); err != nil {
			return err
		}
		msg, err := replay(ctx)
		if err != nil {
			return merr.Wrap(err, "read retained WAL for replica bootstrap")
		}
		if msg == nil {
			return merr.WrapErrServiceUnavailable("retained WAL ended before the replica snapshot boundary")
		}
		if !msg.IsPersisted() || msg.TimeTick() <= snapshot.Checkpoint.TimeTick {
			continue
		}
		if msg.TimeTick() <= through || msg.TimeTick() > upper {
			return merr.WrapErrDataIntegrityMsg("WAL replica replay is not the ordered snapshot prefix")
		}
		if msg.TimeTick() == upper && !proto.Equal(message.MustMarshalMessageID(msg.LastConfirmedMessageID()), snapshot.Observed.MessageId) {
			return merr.WrapErrDataIntegrityMsg("WAL replica replay reached a different log at the snapshot boundary")
		}
		r.observeMessage(ctx, msg)
		through = msg.TimeTick()
	}
	return ctx.Err()
}
