package streamingnode

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ metastore.WALReplayRetentionCatalog = (*catalog)(nil)

const maxReplayPins = 32

func replayStateKey(channel string) string { return retentionPrefix(channel) + "replay-state" }

func (c *catalog) loadReplayState(ctx context.Context, channel string) (*streamingpb.WALReplayRetentionState, error) {
	encoded, err := c.loadRetentionValue(ctx, replayStateKey(channel))
	if err != nil {
		return nil, err
	}
	state := &streamingpb.WALReplayRetentionState{}
	if err := proto.Unmarshal([]byte(encoded), state); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode WAL replay retention")
	}
	if len(state.Pins) > maxReplayPins {
		return nil, merr.WrapErrDataIntegrityMsg("WAL replay pin budget exceeded")
	}
	if state.Pins == nil {
		state.Pins = make(map[string]*streamingpb.WALReplayPin)
	}
	return state, nil
}

func replayMessageID(checkpoint *streamingpb.WALCheckpoint) (message.MessageID, error) {
	if checkpoint.GetMessageId() == nil {
		return nil, merr.WrapErrDataIntegrityMsg("WAL replay checkpoint has no physical position")
	}
	id, err := message.UnmarshalMessageID(checkpoint.MessageId)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode WAL replay position")
	}
	return id, nil
}

func sameReplayLog(a, b *streamingpb.WALCheckpoint) bool {
	return a.GetLogGeneration() == b.GetLogGeneration() && a.GetMessageId().GetWALName() == b.GetMessageId().GetWALName()
}

// Retention stores only the paired restart position, not unrelated recovery
// control/schema data carried by a full WALCheckpoint.
func replayCheckpoint(checkpoint *streamingpb.WALCheckpoint) *streamingpb.WALCheckpoint {
	return &streamingpb.WALCheckpoint{
		MessageId: proto.CloneOf(checkpoint.MessageId), TimeTick: checkpoint.TimeTick,
		LogGeneration: checkpoint.LogGeneration,
	}
}

func (c *catalog) replayOwner(ctx context.Context, channel string, term int64, checkpoint *streamingpb.WALCheckpoint) (string, *streamingpb.WALCheckpoint, error) {
	encoded, err := c.metaKV.Load(ctx, buildConsumeCheckpointKey(channel))
	if err != nil {
		return "", nil, err
	}
	current := &streamingpb.WALCheckpoint{}
	if err := proto.Unmarshal([]byte(encoded), current); err != nil {
		return "", nil, merr.WrapErrDataIntegrity(err, "decode WAL replay owner")
	}
	if term <= 0 || current.Term != term || !sameReplayLog(current, checkpoint) {
		return "", nil, merr.WrapErrServiceUnavailable("WAL replay source term or log generation changed")
	}
	return encoded, current, nil
}

func (c *catalog) PinWALReplay(ctx context.Context, channel string, pin *streamingpb.WALReplayPin) error {
	if pin.GetPinId() == "" || len(pin.GetPinId()) > 256 {
		return merr.WrapErrServiceInternalMsg("invalid WAL replay pin identity")
	}
	id, err := replayMessageID(pin.Checkpoint)
	if err != nil {
		return err
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return err
	}
	owner, _, err := c.replayOwner(ctx, channel, pin.SourceTerm, pin.Checkpoint)
	if err != nil {
		return err
	}
	released, err := c.loadRetentionValue(ctx, retentionKey(channel, "replay-released", pin.PinId))
	if err != nil {
		return err
	}
	if released != "" {
		return merr.WrapErrServiceUnavailable("WAL replay pin was already released")
	}
	state, err := c.loadReplayState(ctx, channel)
	if err != nil {
		return err
	}
	if watermark := state.Truncated; watermark != nil {
		if watermark.LogGeneration > pin.Checkpoint.LogGeneration ||
			(watermark.LogGeneration == pin.Checkpoint.LogGeneration && !sameReplayLog(watermark, pin.Checkpoint)) {
			return merr.WrapErrDataIntegrityMsg("WAL replay retention log generation disagrees with its owner")
		}
		if sameReplayLog(watermark, pin.Checkpoint) {
			truncated, err := replayMessageID(watermark)
			if err != nil {
				return err
			}
			if id.LT(truncated) {
				return merr.WrapErrServiceUnavailable("WAL replay starts behind an authorized truncation; recapture the baseline")
			}
		}
	}
	normalized := &streamingpb.WALReplayPin{PinId: pin.PinId, SourceTerm: pin.SourceTerm, Checkpoint: replayCheckpoint(pin.Checkpoint)}
	if existing := state.Pins[pin.PinId]; existing != nil {
		if !proto.Equal(existing, normalized) {
			return merr.WrapErrDataIntegrityMsg("WAL replay pin identity was reused")
		}
	} else if len(state.Pins) == maxReplayPins {
		return merr.WrapErrServiceUnavailable("WAL replay pin budget exhausted")
	}
	state.Pins[pin.PinId] = normalized
	encoded, err := proto.Marshal(state)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "encode WAL replay pin")
	}
	return c.commitRetention(ctx, channel, epoch, map[string]string{replayStateKey(channel): string(encoded)}, nil, predicates.ValueEqual(buildConsumeCheckpointKey(channel), owner))
}

func (c *catalog) ReleaseWALReplay(ctx context.Context, channel, pinID string) error {
	if pinID == "" || len(pinID) > 256 {
		return merr.WrapErrServiceInternalMsg("invalid WAL replay pin identity")
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return err
	}
	state, err := c.loadReplayState(ctx, channel)
	if err != nil {
		return err
	}
	delete(state.Pins, pinID)
	encoded, err := proto.Marshal(state)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "encode WAL replay release")
	}
	return c.commitRetention(ctx, channel, epoch, map[string]string{
		replayStateKey(channel): string(encoded), retentionKey(channel, "replay-released", pinID): pinID,
	}, nil)
}

func (c *catalog) AuthorizeWALTruncation(ctx context.Context, channel string, term int64, requested *streamingpb.WALCheckpoint) (*streamingpb.WALCheckpoint, error) {
	id, err := replayMessageID(requested)
	if err != nil {
		return nil, err
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return nil, err
	}
	owner, current, err := c.replayOwner(ctx, channel, term, requested)
	if err != nil {
		return nil, err
	}
	published, err := replayMessageID(current)
	if err != nil {
		return nil, err
	}
	if published.LT(id) || requested.TimeTick > current.TimeTick {
		return nil, merr.WrapErrServiceInternalMsg("WAL truncation exceeds its published checkpoint")
	}
	state, err := c.loadReplayState(ctx, channel)
	if err != nil {
		return nil, err
	}
	safe := replayCheckpoint(requested)
	var floor message.MessageID
	if watermark := state.Truncated; watermark != nil {
		if watermark.LogGeneration > current.LogGeneration ||
			(watermark.LogGeneration == current.LogGeneration && !sameReplayLog(watermark, current)) {
			return nil, merr.WrapErrDataIntegrityMsg("WAL truncation log generation regressed")
		}
		if sameReplayLog(watermark, current) {
			previous, err := replayMessageID(watermark)
			if err != nil {
				return nil, err
			}
			if published.LT(previous) {
				return nil, merr.WrapErrDataIntegrityMsg("published WAL checkpoint regressed behind truncation")
			}
			floor = previous
			if id.LT(previous) {
				safe, id = replayCheckpoint(watermark), previous
			}
		}
	}
	for _, pin := range state.Pins {
		if pin.GetPinId() == "" || pin.GetSourceTerm() <= 0 || pin.GetCheckpoint() == nil {
			return nil, merr.WrapErrDataIntegrityMsg("invalid retained WAL replay pin")
		}
		if !sameReplayLog(pin.Checkpoint, current) {
			continue
		}
		start, err := replayMessageID(pin.Checkpoint)
		if err != nil {
			return nil, err
		}
		if floor != nil && start.LT(floor) {
			return nil, merr.WrapErrDataIntegrityMsg("WAL replay pin is behind an already authorized truncation")
		}
		if start.LT(id) {
			safe, id = replayCheckpoint(pin.Checkpoint), start
		}
	}
	// Pins, including old-term pins, never disappear merely because the writer
	// changed. Their owner must explicitly finish or fence the bootstrap first.
	state.Truncated = safe
	encoded, err := proto.Marshal(state)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "encode WAL truncation authorization")
	}
	if err := c.commitRetention(ctx, channel, epoch, map[string]string{replayStateKey(channel): string(encoded)}, nil, predicates.ValueEqual(buildConsumeCheckpointKey(channel), owner)); err != nil {
		return nil, err
	}
	return safe, nil
}
