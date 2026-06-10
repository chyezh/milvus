package transformlog

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const defaultBufferMaxRows = 1024

type buffer struct {
	entries             []deleteEntry
	fromTimeTick        uint64
	toTimeTick          uint64
	rows                uint64
	maxRows             uint64
	flushing            bool
	flushTargetTimeTick uint64
}

func newBuffer(maxRows uint64) buffer {
	if maxRows == 0 {
		maxRows = defaultBufferMaxRows
	}
	return buffer{maxRows: maxRows}
}

func (b *buffer) AppendDelete(msg message.ImmutableDeleteMessageV1) {
	timetick := msg.TimeTick()
	if len(b.entries) == 0 {
		b.fromTimeTick = timetick
	}
	body := msg.MustBody()
	rows := deleteEntryRows(body)
	b.toTimeTick = timetick
	b.rows += rows
	b.entries = append(b.entries, deleteEntry{
		timeTick: timetick,
		rows:     rows,
		request:  cloneDeleteRequest(body),
	})
}

func (b *buffer) ShouldFlush() bool {
	return len(b.entries) > 0 && b.rows >= b.maxRows
}

func (b *buffer) StartFlush(timetick uint64) bool {
	if timetick == 0 {
		timetick = b.toTimeTick
	}
	if timetick == 0 {
		return false
	}
	if timetick > b.flushTargetTimeTick {
		b.flushTargetTimeTick = timetick
	}
	if b.flushing {
		return false
	}
	b.flushing = true
	return true
}

func (b *buffer) FinishFlush() {
	b.flushing = false
	b.flushTargetTimeTick = 0
}

func (b *buffer) IsFlushing() bool {
	return b.flushing
}

func (b *buffer) DataTimeTick() uint64 {
	return b.toTimeTick
}

func (b *buffer) FlushTargetTimeTick() uint64 {
	return b.flushTargetTimeTick
}

func (b *buffer) IsEmpty() bool {
	return len(b.entries) == 0
}

func (b *buffer) FlushChunk(chunkID uint64, timetick uint64) *streamingpb.TransformLogChunk {
	entries := b.flushEntriesThrough(timetick)
	if len(entries) == 0 {
		return nil
	}
	chunkEntries := make([]*streamingpb.TransformLogEntry, 0, len(entries))
	for _, entry := range entries {
		chunkEntries = append(chunkEntries, transformLogEntryFromDeleteEntry(entry))
	}
	return &streamingpb.TransformLogChunk{
		ChunkId: chunkID,
		Entries: chunkEntries,
	}
}

func (b *buffer) HasFlushWorkThrough(timetick uint64) bool {
	return len(b.entriesThrough(timetick)) > 0
}

func (b *buffer) flushEntriesThrough(timetick uint64) []deleteEntry {
	entries := b.entriesThrough(timetick)
	if len(entries) == 0 {
		return nil
	}
	var rows uint64
	for idx, entry := range entries {
		if idx > 0 && rows+entry.rows > b.maxRows {
			return entries[:idx]
		}
		rows += entry.rows
		if rows >= b.maxRows {
			return entries[:idx+1]
		}
	}
	return entries
}

func (b *buffer) entriesThrough(timetick uint64) []deleteEntry {
	for idx, entry := range b.entries {
		if entry.timeTick > timetick {
			return b.entries[:idx]
		}
	}
	return b.entries
}

func (b *buffer) DiscardThrough(timetick uint64) {
	kept := b.entries[:0]
	for _, entry := range b.entries {
		if entry.timeTick <= timetick {
			continue
		}
		kept = append(kept, entry)
	}
	b.entries = kept
	b.rebuildStats()
}

func (b *buffer) rebuildStats() {
	b.fromTimeTick = 0
	b.toTimeTick = 0
	b.rows = 0
	if len(b.entries) == 0 {
		return
	}
	b.fromTimeTick = b.entries[0].timeTick
	for _, entry := range b.entries {
		b.toTimeTick = entry.timeTick
		b.rows += entry.rows
	}
}

type deleteEntry struct {
	timeTick uint64
	rows     uint64
	request  *msgpb.DeleteRequest
}

func deleteEntryRows(request *msgpb.DeleteRequest) uint64 {
	if rows := len(request.GetTimestamps()); rows > 0 {
		return uint64(rows)
	}
	return 1
}

func transformLogEntryFromDeleteEntry(entry deleteEntry) *streamingpb.TransformLogEntry {
	request := cloneDeleteRequest(entry.request)
	return &streamingpb.TransformLogEntry{
		TimeTick: entry.timeTick,
		Entry: &streamingpb.TransformLogEntry_Delete{
			Delete: &streamingpb.TransformDeleteEntry{
				Blocks: []*streamingpb.TransformDeleteBlock{
					{
						PartitionId: request.GetPartitionID(),
						PrimaryKeys: request.GetPrimaryKeys(),
					},
				},
			},
		},
	}
}

func cloneDeleteRequest(value *msgpb.DeleteRequest) *msgpb.DeleteRequest {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*msgpb.DeleteRequest)
}
