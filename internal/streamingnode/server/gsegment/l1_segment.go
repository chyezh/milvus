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
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// L1Segment is the in-memory buffer + assignment metadata for a single L1
// (insert) segment. Message streams push into the growing chunk until it
// reaches the configured size, at which point it is sealed and handed to the
// SyncScheduler. See goals.md for the overall flow.
type L1Segment struct {
	mu sync.Mutex

	meta         *streamingpb.SegmentAssignmentMeta
	schema       *schemapb.CollectionSchema
	sealed       bool
	sealedChunks []*InsertChunk
	growingChunk *InsertChunk
	dirty        bool
	// inFlight is set while a sealed chunk is being serialized/uploaded. Guards
	// L1 per-segment serialization so CheckpointTimeTick stays monotonic.
	inFlight bool
}

// newL1Segment constructs an L1Segment from create-segment metadata. The
// persisted-storage oneof is initialized here if missing so later chunk
// writes don't have to nil-check.
func newL1Segment(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema) *L1Segment {
	if meta.PersistedStorage == nil {
		meta.PersistedStorage = &streamingpb.SegmentAssignmentMeta_L1{
			L1: &streamingpb.L1SegmentPersistedStorage{},
		}
	}
	if meta.Stat == nil {
		meta.Stat = &streamingpb.SegmentAssignmentStat{}
	}
	return &L1Segment{
		meta:   meta,
		schema: schema,
		dirty:  true,
	}
}

// ObserveInsert accumulates an insert message into the growing chunk, sealing
// it once size reaches the configured target. TimeTicks behind the checkpoint
// are dropped (txn messages may share tick, they're already applied once).
func (s *L1Segment) ObserveInsert(msg message.ImmutableInsertMessageV1) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timetick := msg.TimeTick()
	if timetick < s.meta.CheckpointTimeTick {
		return
	}
	rows := msg.Header().TotolRows()
	binarySize := uint64(msg.EstimateSize())

	s.pushMessageIntoChunk(msg, binarySize)
	s.meta.Stat.ModifiedBinarySize += binarySize
	s.meta.Stat.ModifiedRows += rows
	s.meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(timetick).Unix()
	if s.meta.Stat.BeginTimeTick == 0 {
		s.meta.Stat.BeginTimeTick = timetick
	}
	s.meta.Stat.EndTimeTick = timetick
	s.meta.CheckpointTimeTick = timetick
	s.dirty = true
}

// Flush transitions the segment to sealed state and seals the growing chunk
// so it can be persisted. Idempotent.
func (s *L1Segment) Flush(timetick uint64) {
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

// SealStaleGrowingChunk seals the current growing chunk when it has stayed
// open longer than the configured sync period.
func (s *L1Segment) SealStaleGrowingChunk(timetick uint64, staleDuration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.growingChunk == nil || s.growingChunk.IsEmpty() ||
		!isStaleTimeTick(s.growingChunk.startFromTimeTick, timetick, staleDuration) {
		return false
	}
	s.sealGrowingChunk()
	return true
}

// BeginSaveChunk claims the oldest sealed chunk for persistence and marks the
// segment as having an in-flight sync. Returns (nil, false) if there is no
// work or a sync is already in flight; callers must invoke SaveChunkDone or
// AbortSync to release the slot.
func (s *L1Segment) BeginSaveChunk() (*InsertChunk, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inFlight || len(s.sealedChunks) == 0 {
		return nil, false
	}
	s.inFlight = true
	return s.sealedChunks[0], true
}

// SaveChunkDoneRequest carries the binlog output of a completed insert chunk.
type SaveChunkDoneRequest struct {
	ManifestPath string
	Binlog       *streamingpb.L1SegmentBinLogs
	MergedStats  *datapb.FieldBinlog
}

type L1ChunkSyncContext struct {
	Flush          bool
	SegmentRows    int64
	ManifestPath   string
	ColumnGroups   []storagecommon.ColumnGroup
	PreviousBinlog []*streamingpb.L1SegmentBinLogs
}

func (s *L1Segment) ChunkSyncContext(chunk *InsertChunk) L1ChunkSyncContext {
	s.mu.Lock()
	defer s.mu.Unlock()

	l1 := s.ensureL1StorageLocked()
	previous := make([]*streamingpb.L1SegmentBinLogs, 0, len(l1.GetBinlogs()))
	for _, binlog := range l1.GetBinlogs() {
		previous = append(previous, proto.Clone(binlog).(*streamingpb.L1SegmentBinLogs))
	}
	return L1ChunkSyncContext{
		Flush:          s.sealed && len(s.sealedChunks) == 1 && s.sealedChunks[0] == chunk,
		SegmentRows:    int64(s.meta.GetStat().GetModifiedRows()),
		ManifestPath:   l1.GetManifestPath(),
		ColumnGroups:   inferColumnGroups(s.schema, previous),
		PreviousBinlog: previous,
	}
}

// SaveChunkDone records the persisted binlog, pops the chunk, and clears the
// in-flight flag. If the segment has been Flush()ed and no chunks remain, it
// transitions to FLUSHED state.
func (s *L1Segment) SaveChunkDone(req *SaveChunkDoneRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.meta.Stat.BinlogCounter++
	l1 := s.ensureL1StorageLocked()
	if req.ManifestPath != "" {
		l1.ManifestPath = req.ManifestPath
	}
	if req.MergedStats != nil {
		l1.MergedStatsBinlog = proto.Clone(req.MergedStats).(*datapb.FieldBinlog)
	}
	if req.Binlog != nil {
		l1.Binlogs = append(l1.Binlogs, req.Binlog)
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

// SyncSafeTimeTick returns the largest timetick whose segment-data side
// effects are durable in object storage and reflected in segment meta.
func (s *L1Segment) SyncSafeTimeTick() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.growingChunk == nil && len(s.sealedChunks) == 0 && !s.inFlight {
		return s.meta.CheckpointTimeTick
	}
	return s.lastSyncedTimeTickLocked()
}

func (s *L1Segment) lastSyncedTimeTickLocked() uint64 {
	var timetick uint64
	if l1 := s.ensureL1StorageLocked(); l1 != nil {
		for _, binlog := range l1.GetBinlogs() {
			if binlog.GetToTimeTick() > timetick {
				timetick = binlog.GetToTimeTick()
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

// AbortSync clears the in-flight flag without advancing state, so the owning
// SegmentManager can re-attempt BeginSaveChunk. Used when the scheduler fails
// a task with a terminal (non-retryable) error, e.g., on Close.
func (s *L1Segment) AbortSync() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight = false
}

func (s *L1Segment) ensureL1StorageLocked() *streamingpb.L1SegmentPersistedStorage {
	switch stg := s.meta.PersistedStorage.(type) {
	case *streamingpb.SegmentAssignmentMeta_L1:
		if stg.L1 == nil {
			stg.L1 = &streamingpb.L1SegmentPersistedStorage{}
		}
		return stg.L1
	default:
		l1 := &streamingpb.L1SegmentPersistedStorage{}
		s.meta.PersistedStorage = &streamingpb.SegmentAssignmentMeta_L1{L1: l1}
		return l1
	}
}

func inferColumnGroups(
	schema *schemapb.CollectionSchema,
	binlogs []*streamingpb.L1SegmentBinLogs,
) []storagecommon.ColumnGroup {
	if schema == nil {
		return nil
	}
	var latest []*datapb.FieldBinlog
	for i := len(binlogs) - 1; i >= 0; i-- {
		if len(binlogs[i].GetFieldBinlog()) > 0 {
			latest = binlogs[i].GetFieldBinlog()
			break
		}
	}
	if len(latest) == 0 {
		return nil
	}
	allFields := typeutil.GetAllFieldSchemas(schema)
	field2idx := make(map[int64]int, len(allFields))
	for idx, field := range allFields {
		field2idx[field.GetFieldID()] = idx
	}
	groups := make([]storagecommon.ColumnGroup, 0, len(latest))
	for _, binlog := range latest {
		if len(binlog.GetChildFields()) == 0 {
			return storagecommon.SplitColumns(
				allFields,
				map[int64]storagecommon.ColumnStats{},
				storagecommon.NewSelectedDataTypePolicy(),
				storagecommon.NewRemanentShortPolicy(-1),
			)
		}
		group := storagecommon.ColumnGroup{
			GroupID: binlog.GetFieldID(),
			Fields:  append([]int64{}, binlog.GetChildFields()...),
		}
		for _, fieldID := range group.Fields {
			group.Columns = append(group.Columns, field2idx[fieldID])
		}
		groups = append(groups, group)
	}
	return groups
}

// pushMessageIntoChunk routes a message to the growing chunk, sealing if full.
func (s *L1Segment) pushMessageIntoChunk(msg message.ImmutableInsertMessageV1, size uint64) {
	if s.growingChunk != nil && s.growingChunk.AvailableSize() < int64(size) {
		s.sealGrowingChunk()
	}
	if s.growingChunk == nil {
		expectedChunkSize := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
		s.growingChunk = newInsertChunk(expectedChunkSize)
	}
	s.growingChunk.Push(msg)
	if s.growingChunk.AvailableSize() <= 0 {
		// A single message larger than the expected chunk size still seals the chunk
		// so we don't postpone work indefinitely.
		s.sealGrowingChunk()
	}
}

func (s *L1Segment) sealGrowingChunk() {
	if s.growingChunk == nil {
		return
	}
	s.sealedChunks = append(s.sealedChunks, s.growingChunk)
	s.growingChunk = nil
}

// IsDirty reports whether there is state that hasn't been snapshotted yet.
func (s *L1Segment) IsDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty
}

// ConsumeSnapshot returns a deep-copied snapshot of meta and clears the dirty
// flag. Returns nil if there's nothing new to persist.
func (s *L1Segment) ConsumeSnapshot() *streamingpb.SegmentAssignmentMeta {
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
func (s *L1Segment) Snapshot() *streamingpb.SegmentAssignmentMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(s.meta).(*streamingpb.SegmentAssignmentMeta)
}

// GetMeta returns the raw meta pointer. Callers must not mutate it.
func (s *L1Segment) GetMeta() *streamingpb.SegmentAssignmentMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta
}

// IsSealed reports whether Flush has been called.
func (s *L1Segment) IsSealed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealed
}

// GetSealedChunks exposes the current sealed chunk list (primarily for tests).
func (s *L1Segment) GetSealedChunks() []*InsertChunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealedChunks
}

// GetSchema returns the collection schema bound at CreateL1Segment.
func (s *L1Segment) GetSchema() *schemapb.CollectionSchema {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.schema
}
