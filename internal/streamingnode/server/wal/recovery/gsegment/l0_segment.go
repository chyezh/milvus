package gsegment

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type L0Segment struct {
	mu sync.Mutex

	meta         *streamingpb.SegmentAssignmentMeta
	sealedChunks []*DeleteChunk // sealed chunks for delete messages
	growingChunk *DeleteChunk   // growing chunk for delete messages
	sealed       bool
	dirty        bool
}

// ObserveDelete observes a delete message.
func (s *L0Segment) ObserveDelete(msg message.ImmutableDeleteMessageV1) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timetick := msg.TimeTick()
	if timetick < s.meta.CheckpointTimeTick {
		return
	}

	rows := msg.Header().TotolRows()
	binarySize := uint64(msg.EstimateSize())

	s.pushMessageIntoChunk(msg)
	s.meta.Stat.ModifiedRows += rows
	s.meta.Stat.ModifiedBinarySize += binarySize
	s.meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(timetick).Unix()
	if s.meta.Stat.BeginTimeTick == 0 {
		s.meta.Stat.BeginTimeTick = timetick
		s.meta.Stat.EndTimeTick = timetick
	}
	s.meta.Stat.EndTimeTick = timetick
	s.meta.CheckpointTimeTick = timetick
	s.dirty = true
}

// pushMessageIntoChunk pushes the message into the chunk.
func (s *L0Segment) pushMessageIntoChunk(msg message.ImmutableDeleteMessageV1) {
	if s.growingChunk == nil {
		s.growingChunk = &DeleteChunk{}
	}
	s.growingChunk.Push(msg)
	// For L0 segments, we seal chunks more aggressively since they're smaller
	if s.growingChunk.size > 1024*1024 { // 1MB threshold for L0 segments
		s.sealGrowingChunk()
	}
}

// sealGrowingChunk seals the growing chunk and adds it to the sealed chunks.
func (s *L0Segment) sealGrowingChunk() {
	if s.growingChunk == nil || s.growingChunk.IsEmpty() {
		return
	}
	s.sealedChunks = append(s.sealedChunks, s.growingChunk)
	s.growingChunk = nil
}

// Flush flushes the segment from growing state into flushed state.
func (s *L0Segment) Flush(timetick uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if timetick < s.meta.CheckpointTimeTick {
		return
	}

	if s.sealed {
		return
	}
	s.sealed = true
	s.sealGrowingChunk()
	s.meta.CheckpointTimeTick = timetick
	s.meta.Stat.FlushSegmentTimeTick = timetick
	s.dirty = true
}

// BeginSaveChunk begins to save the chunk.
func (s *L0Segment) BeginSaveChunk() (chunk *DeleteChunk, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.sealedChunks) == 0 {
		return nil, false
	}
	return s.sealedChunks[0], true
}

type SaveDeleteChunkDoneRequest struct {
	DeltaPath string
	Binlog    *datapb.FieldBinlog
}

// SaveChunkDone saves the chunk and pops it from the sealed chunks.
func (s *L0Segment) SaveChunkDone(req *SaveDeleteChunkDoneRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.meta.Stat.BinlogCounter++
	l0Storage := s.meta.PersistedStorage.(*streamingpb.SegmentAssignmentMeta_L0).L0
	l0Storage.DeltaBinlog = append(l0Storage.DeltaBinlog, req.Binlog)
	s.sealedChunks = s.sealedChunks[1:]
	if len(s.sealedChunks) == 0 && s.sealed {
		// if the segment is already flushed, the L0 segment is not needed to be saved anymore.
		s.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	}
	s.dirty = true
}

// IsDirty returns whether the segment has dirty data.
func (s *L0Segment) IsDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty
}

// ConsumeSnapshot consumes the dirty data and returns a snapshot.
// Returns nil if the segment is not dirty.
func (s *L0Segment) ConsumeSnapshot() *streamingpb.SegmentAssignmentMeta {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.dirty {
		return nil
	}

	s.dirty = false
	// Return a deep copy of the meta using proto.Clone
	return proto.Clone(s.meta).(*streamingpb.SegmentAssignmentMeta)
}

// GetMeta returns the segment metadata.
func (s *L0Segment) GetMeta() *streamingpb.SegmentAssignmentMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta
}

// IsSealed returns whether the segment is sealed.
func (s *L0Segment) IsSealed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealed
}

// GetSealedChunks returns the sealed chunks.
func (s *L0Segment) GetSealedChunks() []*DeleteChunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealedChunks
}
