package segment

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type writeOnlyInsertBuffer struct {
	entries      []message.RetainedImmutableMessage
	fromTimeTick uint64
	toTimeTick   uint64
	rows         uint64
	binarySize   uint64
}

func (b *writeOnlyInsertBuffer) append(msg message.RetainedImmutableMessage, assignment *messagespb.PartitionSegmentAssignment) {
	b.appendMessage(msg, assignment.GetRows(), assignment.GetBinarySize())
}

func (b *writeOnlyInsertBuffer) appendMessage(msg message.RetainedImmutableMessage, rows uint64, binarySize uint64) {
	timetick := msg.TimeTick()
	if len(b.entries) == 0 {
		b.fromTimeTick = timetick
	}
	b.toTimeTick = timetick
	b.rows += rows
	b.binarySize += binarySize
	b.entries = append(b.entries, msg)
}

func (b writeOnlyInsertBuffer) DataTimeTick() uint64 {
	return b.toTimeTick
}

func (b writeOnlyInsertBuffer) Messages() []message.ImmutableMessage {
	if len(b.entries) == 0 {
		return nil
	}
	messages := make([]message.ImmutableMessage, len(b.entries))
	for idx, entry := range b.entries {
		messages[idx] = message.CloneImmutableMessage(entry)
	}
	return messages
}

func (b writeOnlyInsertBuffer) borrowedMessages() []message.ImmutableMessage {
	if len(b.entries) == 0 {
		return nil
	}
	messages := make([]message.ImmutableMessage, len(b.entries))
	for idx, entry := range b.entries {
		messages[idx] = entry
	}
	return messages
}

func (b *writeOnlyInsertBuffer) flushPack(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema) *flushPack {
	return &flushPack{
		Meta:         proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta),
		CollectionID: meta.GetCollectionId(),
		PartitionID:  meta.GetPartitionId(),
		SegmentID:    meta.GetSegmentId(),
		VChannel:     meta.GetVchannel(),
		FromTimeTick: b.fromTimeTick,
		ToTimeTick:   b.toTimeTick,
		Schema:       schema,
		Rows:         b.rows,
		BinarySize:   b.binarySize,
		Inserts:      b.borrowedMessages(),
	}
}

func (b *writeOnlyInsertBuffer) reset() {
	*b = writeOnlyInsertBuffer{}
}

func (b *writeOnlyInsertBuffer) takeAll() writeOnlyInsertBuffer {
	chunk := *b
	b.reset()
	return chunk
}
