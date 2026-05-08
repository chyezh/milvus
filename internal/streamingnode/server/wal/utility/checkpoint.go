package utility

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const (
	RecoveryMagicStreamingInitialized int64 = 1 // the vchannel info is set into the catalog.
	// the checkpoint is set into the catalog.
)

// NewWALCheckpointFromProto creates a new WALCheckpoint from a protobuf message.
func NewWALCheckpointFromProto(cp *streamingpb.WALCheckpoint) *WALCheckpoint {
	if cp == nil {
		return nil
	}
	return &WALCheckpoint{
		MetaCheckpoint:      NewCheckpoint(message.MustUnmarshalMessageID(cp.MetaMessageId), cp.MetaTimeTick),
		DataCheckpoint:      NewCheckpointFromWALDataCheckpointProto(cp.DataCheckpoint),
		Magic:               cp.RecoveryMagic,
		ReplicateConfig:     cp.ReplicateConfig,
		ReplicateCheckpoint: NewReplicateCheckpointFromProto(cp.ReplicateCheckpoint),
		AlterWalState:       cp.AlterWalState,
	}
}

// WALCheckpoint represents the recovery checkpoints of a pchannel in the
// Write-Ahead Log (WAL).
type WALCheckpoint struct {
	MetaCheckpoint      *Checkpoint // should always be not nil.
	DataCheckpoint      *Checkpoint
	Magic               int64
	ReplicateCheckpoint *ReplicateCheckpoint
	ReplicateConfig     *commonpb.ReplicateConfiguration
	AlterWalState       *streamingpb.AlterWALState
}

// Checkpoint represents a WAL position and its timetick.
type Checkpoint struct {
	MessageID message.MessageID
	TimeTick  uint64
}

// IntoProto converts the WALCheckpoint to a protobuf message.
func (c *WALCheckpoint) IntoProto() *streamingpb.WALCheckpoint {
	if c == nil {
		return nil
	}
	return &streamingpb.WALCheckpoint{
		MetaMessageId:       message.MustMarshalMessageID(c.MetaCheckpoint.MessageID),
		MetaTimeTick:        c.MetaCheckpoint.TimeTick,
		RecoveryMagic:       c.Magic,
		ReplicateConfig:     c.ReplicateConfig,
		ReplicateCheckpoint: c.ReplicateCheckpoint.IntoProto(),
		AlterWalState:       c.AlterWalState,
		DataCheckpoint:      c.DataCheckpoint.IntoWALDataCheckpointProto(),
	}
}

// Clone creates a new WALCheckpoint with the same values as the original.
func (c *WALCheckpoint) Clone() *WALCheckpoint {
	return &WALCheckpoint{
		MetaCheckpoint:      c.MetaCheckpoint.Clone(),
		DataCheckpoint:      c.DataCheckpoint.Clone(),
		Magic:               c.Magic,
		ReplicateConfig:     c.ReplicateConfig,
		ReplicateCheckpoint: c.ReplicateCheckpoint.Clone(),
		AlterWalState:       c.AlterWalState,
	}
}

// NewCheckpoint creates a new checkpoint from a message id and timetick.
func NewCheckpoint(messageID message.MessageID, timeTick uint64) *Checkpoint {
	return &Checkpoint{
		MessageID: messageID,
		TimeTick:  timeTick,
	}
}

// NewCheckpointFromWALDataCheckpointProto creates a new Checkpoint from a data checkpoint protobuf message.
func NewCheckpointFromWALDataCheckpointProto(cp *streamingpb.WALDataCheckpoint) *Checkpoint {
	if cp == nil {
		return nil
	}
	return NewCheckpoint(message.MustUnmarshalMessageID(cp.MessageId), cp.TimeTick)
}

// IntoWALDataCheckpointProto converts the checkpoint to a data checkpoint protobuf message.
func (c *Checkpoint) IntoWALDataCheckpointProto() *streamingpb.WALDataCheckpoint {
	if c == nil {
		return nil
	}
	return &streamingpb.WALDataCheckpoint{
		MessageId: message.MustMarshalMessageID(c.MessageID),
		TimeTick:  c.TimeTick,
	}
}

// Clone creates a new Checkpoint with the same values as the original.
func (c *Checkpoint) Clone() *Checkpoint {
	if c == nil {
		return nil
	}
	return NewCheckpoint(c.MessageID, c.TimeTick)
}

// NewReplicateCheckpointFromProto creates a new ReplicateCheckpoint from a protobuf message.
func NewReplicateCheckpointFromProto(cp *commonpb.ReplicateCheckpoint) *ReplicateCheckpoint {
	if cp == nil {
		return nil
	}
	return &ReplicateCheckpoint{
		MessageID: message.MustUnmarshalMessageID(cp.MessageId),
		ClusterID: cp.ClusterId,
		PChannel:  cp.Pchannel,
		TimeTick:  cp.TimeTick,
	}
}

// ReplicateCheckpoint represents a source milvus cluster checkpoint.
// It's used to recover the replication state for remote source cluster.
type ReplicateCheckpoint struct {
	ClusterID string            // the cluster id of the source cluster.
	PChannel  string            // the pchannel of the source cluster.
	MessageID message.MessageID // the last confirmed message id of the last replicated message, may be nil when initializing.
	TimeTick  uint64            // the time tick of the last replicated message.
}

// IntoProto converts the ReplicateCheckpoint to a protobuf message.
func (c *ReplicateCheckpoint) IntoProto() *commonpb.ReplicateCheckpoint {
	if c == nil {
		return nil
	}
	return &commonpb.ReplicateCheckpoint{
		ClusterId: c.ClusterID,
		Pchannel:  c.PChannel,
		MessageId: message.MustMarshalMessageID(c.MessageID),
		TimeTick:  c.TimeTick,
	}
}

// Clone creates a new ReplicateCheckpoint with the same values as the original.
func (c *ReplicateCheckpoint) Clone() *ReplicateCheckpoint {
	if c == nil {
		return nil
	}
	return &ReplicateCheckpoint{
		ClusterID: c.ClusterID,
		PChannel:  c.PChannel,
		MessageID: c.MessageID,
		TimeTick:  c.TimeTick,
	}
}
