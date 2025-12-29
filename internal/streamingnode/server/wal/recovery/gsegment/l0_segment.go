package gsegment

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type L0Segment struct {
	mu sync.Mutex

	meta              *streamingpb.SegmentAssignmentMeta
	msgs              []message.ImmutableDeleteMessageV1
	startFromTimeTick uint64
	endToTimeTick     uint64
	sealed            bool
	dirty             bool
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

	s.msgs = append(s.msgs, msg)
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
	s.meta.CheckpointTimeTick = timetick
	s.meta.Stat.FlushSegmentTimeTick = timetick
	s.dirty = true
}

func (s *L0Segment) BeginSaveChunk() (chunk *DeleteChunk, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.msgs) == 0 {
		return nil, false
	}
	return s.msgs[0], true
}
