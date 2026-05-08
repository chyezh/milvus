// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package gsegment

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// l0ChunkSizeThreshold is the byte budget at which a growing delete chunk is
// sealed. L0 segments hold far less data than L1 so a smaller value (1 MiB)
// is adequate; promote to a paramtable field if tuning becomes necessary.
const l0ChunkSizeThreshold int64 = 1 << 20

// L0Segment buffers delete messages for a single L0 segment and schedules
// their persistence. Shape matches L1Segment; see that type for field notes.
type L0Segment struct {
	mu sync.Mutex

	meta         *streamingpb.SegmentAssignmentMeta
	schema       *schemapb.CollectionSchema
	sealedChunks []*DeleteChunk
	growingChunk *DeleteChunk
	sealed       bool
	dirty        bool
	inFlight     bool
}

func newL0Segment(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema) *L0Segment {
	if meta.PersistedStorage == nil {
		meta.PersistedStorage = &streamingpb.SegmentAssignmentMeta_L0{
			L0: &streamingpb.L0SegmentPersistedStorage{},
		}
	}
	if meta.Stat == nil {
		meta.Stat = &streamingpb.SegmentAssignmentStat{}
	}
	return &L0Segment{meta: meta, schema: schema, dirty: true}
}

// ObserveDelete appends a delete message to the growing chunk. Sealing is
// triggered by the chunk-size threshold or by Flush.
func (s *L0Segment) ObserveDelete(msg message.ImmutableDeleteMessageV1, schema *schemapb.CollectionSchema) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if schema != nil {
		s.schema = schema
	}
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
	}
	s.meta.Stat.EndTimeTick = timetick
	s.meta.CheckpointTimeTick = timetick
	s.dirty = true
}

func (s *L0Segment) pushMessageIntoChunk(msg message.ImmutableDeleteMessageV1) {
	if s.growingChunk == nil {
		s.growingChunk = &DeleteChunk{}
	}
	s.growingChunk.Push(msg)
	if s.growingChunk.size > l0ChunkSizeThreshold {
		s.sealGrowingChunk()
	}
}

func (s *L0Segment) sealGrowingChunk() {
	if s.growingChunk == nil || s.growingChunk.IsEmpty() {
		return
	}
	s.sealedChunks = append(s.sealedChunks, s.growingChunk)
	s.growingChunk = nil
}

// Flush seals the segment and its growing chunk so they can be persisted.
func (s *L0Segment) Flush(timetick uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if timetick < s.meta.CheckpointTimeTick || s.sealed {
		return
	}
	s.sealed = true
	s.sealGrowingChunk()
	if len(s.sealedChunks) == 0 {
		s.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	}
	s.meta.CheckpointTimeTick = timetick
	s.meta.Stat.FlushSegmentTimeTick = timetick
	s.dirty = true
}

// SealStaleGrowingChunk seals the current growing delete chunk when it has
// stayed open longer than the configured sync period.
func (s *L0Segment) SealStaleGrowingChunk(timetick uint64, staleDuration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.growingChunk == nil || s.growingChunk.IsEmpty() ||
		!isStaleTimeTick(s.growingChunk.startFromTimeTick, timetick, staleDuration) {
		return false
	}
	s.sealGrowingChunk()
	return true
}

// BeginSaveChunk claims the oldest sealed chunk and marks the segment as
// having an in-flight sync. Returns (nil, false) if nothing to do or already
// in flight.
func (s *L0Segment) BeginSaveChunk() (*DeleteChunk, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inFlight || len(s.sealedChunks) == 0 {
		return nil, false
	}
	s.inFlight = true
	return s.sealedChunks[0], true
}

// SaveDeleteChunkDoneRequest carries the result of a completed delete task.
type SaveDeleteChunkDoneRequest struct {
	Binlog *datapb.FieldBinlog
}

// SaveChunkDone records the binlog, pops the chunk, clears inFlight, and
// promotes state to FLUSHED once the segment is sealed with no pending work.
func (s *L0Segment) SaveChunkDone(req *SaveDeleteChunkDoneRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.meta.Stat.BinlogCounter++
	l0 := s.ensureL0StorageLocked()
	if req.Binlog != nil {
		l0.DeltaBinlog = append(l0.DeltaBinlog, req.Binlog)
	}
	if len(s.sealedChunks) > 0 {
		s.sealedChunks = s.sealedChunks[1:]
	}
	s.inFlight = false
	if len(s.sealedChunks) == 0 && s.sealed {
		s.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	}
	s.dirty = true
}

// SyncSafeTimeTick returns the largest timetick whose delete side effects are
// durable and reflected in segment meta.
func (s *L0Segment) SyncSafeTimeTick() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.growingChunk == nil && len(s.sealedChunks) == 0 && !s.inFlight {
		return s.meta.CheckpointTimeTick
	}
	return s.lastSyncedTimeTickLocked()
}

func (s *L0Segment) lastSyncedTimeTickLocked() uint64 {
	var timetick uint64
	if l0 := s.ensureL0StorageLocked(); l0 != nil {
		for _, fieldBinlog := range l0.GetDeltaBinlog() {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				if binlog.GetTimestampTo() > timetick {
					timetick = binlog.GetTimestampTo()
				}
			}
		}
	}
	if timetick != 0 {
		return timetick
	}
	if s.meta.GetStat().GetCreateSegmentTimeTick() != 0 {
		return s.meta.GetStat().GetCreateSegmentTimeTick()
	}
	return s.meta.GetStat().GetBeginTimeTick()
}

// AbortSync releases the in-flight slot without advancing persisted state.
func (s *L0Segment) AbortSync() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight = false
}

func (s *L0Segment) ensureL0StorageLocked() *streamingpb.L0SegmentPersistedStorage {
	switch stg := s.meta.PersistedStorage.(type) {
	case *streamingpb.SegmentAssignmentMeta_L0:
		if stg.L0 == nil {
			stg.L0 = &streamingpb.L0SegmentPersistedStorage{}
		}
		return stg.L0
	default:
		l0 := &streamingpb.L0SegmentPersistedStorage{}
		s.meta.PersistedStorage = &streamingpb.SegmentAssignmentMeta_L0{L0: l0}
		return l0
	}
}

// IsDirty reports whether new state awaits persisting.
func (s *L0Segment) IsDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty
}

// ConsumeSnapshot returns a deep-copied meta snapshot and resets dirty.
func (s *L0Segment) ConsumeSnapshot() *streamingpb.SegmentAssignmentMeta {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.dirty {
		return nil
	}
	s.dirty = false
	return proto.Clone(s.meta).(*streamingpb.SegmentAssignmentMeta)
}

// Snapshot returns a deep-copied view of the segment meta without consuming
// dirty state.
func (s *L0Segment) Snapshot() *streamingpb.SegmentAssignmentMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(s.meta).(*streamingpb.SegmentAssignmentMeta)
}

// GetMeta returns the raw meta pointer; callers must not mutate it.
func (s *L0Segment) GetMeta() *streamingpb.SegmentAssignmentMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta
}

// GetSchema returns the collection schema bound by recovery storage.
func (s *L0Segment) GetSchema() *schemapb.CollectionSchema {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.schema
}

// IsSealed reports whether Flush has been called.
func (s *L0Segment) IsSealed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealed
}

// GetSealedChunks exposes the current sealed chunk list (primarily for tests).
func (s *L0Segment) GetSealedChunks() []*DeleteChunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealedChunks
}
