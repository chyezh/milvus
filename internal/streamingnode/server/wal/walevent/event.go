// Package walevent provides the ordered input shared by WAL replica runtimes.
package walevent

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Cursor identifies an applied event prefix. Seq zero denotes the recovered
// baseline of a term. A publisher that loses its sequence must acquire a new
// WAL term; reconnecting a transport does not create a new term or sequence.
type Cursor struct {
	Term int64
	Seq  uint64
}

// Event and its payload are immutable after publication. Apply implementations
// must copy any protobuf that they need to modify. In particular, an event
// contains coordinator intent, never a primary's local QueryView readiness.
type Event struct {
	Cursor  Cursor
	Payload Payload
}

// Payload is closed to the four inputs of the replica state machine. Transport
// reset, retention, and readiness are deliberately not business events.
type Payload interface {
	freeze() (Payload, int)
}

// WALEntry carries a complete, ordered logical message, including assembled
// transactions and volatile query barriers. Append completion is not sufficient
// to produce this event: it must come from the ordered recovery stream.
type WALEntry struct {
	Message message.ImmutableMessage
}

func (e WALEntry) freeze() (Payload, int) {
	// ImmutableMessage owns immutable Go data. Keeping it in the journal does
	// not retain a persistence ACK reference or delay the recovery checkpoint.
	return e, e.Message.EstimateSize()
}

// QueryViewIntentBatch carries the coordinator's desired views. Report callbacks
// belong to each replica's control connection and never enter the event stream.
type QueryViewIntentBatch struct {
	Views []*viewpb.QueryViewOfShard
}

func (e QueryViewIntentBatch) freeze() (Payload, int) {
	result := QueryViewIntentBatch{Views: make([]*viewpb.QueryViewOfShard, len(e.Views))}
	size := 0
	for i, view := range e.Views {
		result.Views[i] = proto.Clone(view).(*viewpb.QueryViewOfShard)
		size += proto.Size(result.Views[i])
	}
	return result, size
}

// StoragePublished contains durable storage descriptors whose completion cannot
// be inferred from a WAL message. Descriptors reference immutable objects; they
// must not contain an unpersisted message tail or local worker callbacks.
type StoragePublished struct {
	VChannels []*streamingpb.VChannelMeta
	Segments  []*streamingpb.SegmentAssignmentMeta
	Summary   *streamingpb.PChannelSummaryManifest
}

func (e StoragePublished) freeze() (Payload, int) {
	result := StoragePublished{
		VChannels: make([]*streamingpb.VChannelMeta, len(e.VChannels)),
		Segments:  make([]*streamingpb.SegmentAssignmentMeta, len(e.Segments)),
		Summary:   proto.Clone(e.Summary).(*streamingpb.PChannelSummaryManifest),
	}
	size := proto.Size(result.Summary)
	for i, vchannel := range e.VChannels {
		result.VChannels[i] = proto.Clone(vchannel).(*streamingpb.VChannelMeta)
		size += proto.Size(result.VChannels[i])
	}
	for i, segment := range e.Segments {
		result.Segments[i] = proto.Clone(segment).(*streamingpb.SegmentAssignmentMeta)
		size += proto.Size(result.Segments[i])
	}
	return result, size
}

// CheckpointPublished is emitted only after the corresponding storage and
// catalog transaction is durable. It does not authorize RO persistence or GC.
type CheckpointPublished struct {
	Checkpoint *streamingpb.WALCheckpoint
}

func (e CheckpointPublished) freeze() (Payload, int) {
	cp := proto.Clone(e.Checkpoint).(*streamingpb.WALCheckpoint)
	return CheckpointPublished{Checkpoint: cp}, proto.Size(cp)
}
