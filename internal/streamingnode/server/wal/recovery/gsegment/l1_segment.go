// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gsegment

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

// L1Segment is the segment recovery info for L1 segment.
type L1Segment struct {
	mu sync.Mutex

	meta         *streamingpb.SegmentAssignmentMeta
	sealed       bool                       // whether the segment is sealed.
	sealedChunks []*InsertChunk             // sealed chunks for insert messages, the data in the chunk will never be changed.
	growingChunk *InsertChunk               // growing chunk for insert messages, the data in the chunk can be changed.
	dirty        bool                       // whether the segment recovery info is dirty.
	schema       *schemapb.CollectionSchema // schema for current segment.
}

// ObserveInsert observes an insert message.
func (s *L1Segment) ObserveInsert(msg message.ImmutableInsertMessageV1) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timetick := msg.TimeTick()
	if timetick < s.meta.CheckpointTimeTick {
		return
	}
	// current we only support one partition per insert message.
	// TODO: after we support multiple partitions per insert message, we need to update this codes.
	rows := msg.Header().TotolRows()
	binarySize := uint64(msg.EstimateSize())

	s.pushMessageIntoChunk(msg, binarySize)
	s.meta.Stat.ModifiedBinarySize += binarySize
	s.meta.Stat.ModifiedRows += rows
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
func (s *L1Segment) Flush(timetick uint64) {
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
func (s *L1Segment) BeginSaveChunk() (chunk *InsertChunk, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.sealedChunks) == 0 {
		return nil, false
	}
	return s.sealedChunks[0], true
}

type SaveChunkDoneRequest struct {
	ManifestPath string
	Binlog       *streamingpb.L1SegmentBinLogs
}

// SaveChunkDone saves the chunk and pops it from the sealed chunks.
func (s *L1Segment) SaveChunkDone(req *SaveChunkDoneRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.meta.Stat.BinlogCounter++
	l1Storage := s.meta.PersistedStorage.(*streamingpb.SegmentAssignmentMeta_L1).L1
	l1Storage.ManifestPath = req.ManifestPath
	l1Storage.Binlogs = append(l1Storage.Binlogs, req.Binlog)
	s.sealedChunks = s.sealedChunks[1:]
	if len(s.sealedChunks) == 0 && s.sealed {
		// if the segment is already flushed, the L1 segment is not needed to be saved anymore.
		s.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	}
	s.dirty = true
}

// pushMessageIntoChunk pushes the message into the chunk.
func (s *L1Segment) pushMessageIntoChunk(msg message.ImmutableInsertMessageV1, size uint64) {
	if s.growingChunk != nil && s.growingChunk.AvailableSize() < int64(size) {
		// if the growing chunk is not enough to hold the message, seal it right away.
		s.sealGrowingChunk()
	}
	if s.growingChunk == nil {
		expectedChunkSize := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
		s.growingChunk = newInsertChunk(expectedChunkSize)
	}
	s.growingChunk.Push(msg)
	if s.growingChunk.AvailableSize() <= 0 {
		// may be a single message is greater than the expected chunk size, so we need to seal the growing chunk right now.
		s.sealGrowingChunk()
	}
}

// sealGrowingChunk seals the growing chunk and adds it to the sealed chunks.
func (s *L1Segment) sealGrowingChunk() {
	if s.growingChunk == nil {
		return
	}
	s.sealedChunks = append(s.sealedChunks, s.growingChunk)
	s.growingChunk = nil
}

// IsDirty returns whether the segment has dirty data.
func (s *L1Segment) IsDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty
}

// ConsumeSnapshot consumes the dirty data and returns a snapshot.
// Returns nil if the segment is not dirty.
func (s *L1Segment) ConsumeSnapshot() *streamingpb.SegmentAssignmentMeta {
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
func (s *L1Segment) GetMeta() *streamingpb.SegmentAssignmentMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta
}

// IsSealed returns whether the segment is sealed.
func (s *L1Segment) IsSealed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealed
}

// GetSealedChunks returns the sealed chunks.
func (s *L1Segment) GetSealedChunks() []*InsertChunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealedChunks
}

// GetSchema returns the collection schema.
func (s *L1Segment) GetSchema() *schemapb.CollectionSchema {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.schema
}
