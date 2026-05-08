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
	"context"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type l0SegmentKey struct {
	vchannel     string
	collectionID int64
	partitionID  int64
}

// SegmentManager owns the in-memory L0/L1 segment buffers and the
// SyncScheduler that drives chunk persistence. It is the sole writer of
// SegmentAssignmentMeta while tasks are in flight; recovery_storage reads
// snapshots via GetDirtySnapshots.
//
// Per-segment serialization: each L1/L0Segment tracks an inFlight flag, so at
// most one sync task runs per segment at a time. This keeps
// CheckpointTimeTick monotonic within a segment's binlog sequence.
type SegmentManager struct {
	log.Binder

	mu         sync.RWMutex
	l0Segments map[int64]*L0Segment
	l1Segments map[int64]*L1Segment
	l0Index    map[l0SegmentKey]int64

	scheduler     *SyncScheduler
	chunkManager  storage.ChunkManager
	allocator     allocator.Interface
	storageConfig *indexpb.StorageConfig
}

// SchemaProvider resolves collection schema for recovered L1 segments.
type SchemaProvider interface {
	GetSchema(ctx context.Context, vchannel string, timetick uint64) (*schemapb.CollectionSchema, error)
}

type SegmentManagerOption func(*SegmentManager)

func WithLogIDAllocator(alloc allocator.Interface) SegmentManagerOption {
	return func(m *SegmentManager) {
		if alloc != nil {
			m.allocator = alloc
		}
	}
}

type ContextIDAllocator interface {
	Allocate(ctx context.Context) (uint64, error)
}

func WithContextIDAllocator(ctx context.Context, alloc ContextIDAllocator) SegmentManagerOption {
	if ctx == nil {
		ctx = context.Background()
	}
	if alloc == nil {
		return func(*SegmentManager) {}
	}
	return WithLogIDAllocator(&contextIDAllocator{ctx: ctx, allocator: alloc})
}

func WithStorageConfig(storageConfig *indexpb.StorageConfig) SegmentManagerOption {
	return func(m *SegmentManager) {
		if storageConfig != nil {
			m.storageConfig = storageConfig
		}
	}
}

type contextIDAllocator struct {
	ctx       context.Context
	allocator ContextIDAllocator
}

func (a *contextIDAllocator) Alloc(count uint32) (allocator.UniqueID, allocator.UniqueID, error) {
	var start, end allocator.UniqueID
	for i := uint32(0); i < count; i++ {
		id, err := a.AllocOne()
		if err != nil {
			return 0, 0, err
		}
		if i == 0 {
			start = id
		}
		end = id
	}
	return start, end, nil
}

func (a *contextIDAllocator) AllocOne() (allocator.UniqueID, error) {
	id, err := a.allocator.Allocate(a.ctx)
	return allocator.UniqueID(id), err
}

// NewSegmentManager constructs a segment manager with a fresh SyncScheduler.
// The 4/8 defaults for CPU/IO workers are a conservative starting point;
// expose via paramtable when we wire that in.
func NewSegmentManager(cm storage.ChunkManager, opts ...SegmentManagerOption) *SegmentManager {
	manager := &SegmentManager{
		l0Segments:    make(map[int64]*L0Segment),
		l1Segments:    make(map[int64]*L1Segment),
		l0Index:       make(map[l0SegmentKey]int64),
		scheduler:     NewSyncScheduler(4, 8),
		chunkManager:  cm,
		allocator:     allocator.NewLocalAllocator(1, math.MaxInt64),
		storageConfig: packed.CreateStorageConfig(),
	}
	for _, opt := range opts {
		opt(manager)
	}
	return manager
}

// CreateL1Segment registers a new L1 segment under the given meta + schema.
// Duplicate creates are logged and dropped.
func (m *SegmentManager) CreateL1Segment(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.l1Segments[meta.SegmentId]; exists {
		m.Logger().Warn("L1 segment already exists", zap.Int64("segmentID", meta.SegmentId))
		return
	}
	m.l1Segments[meta.SegmentId] = newL1Segment(meta, schema)
	m.Logger().Info("Created L1 segment", zap.Int64("segmentID", meta.SegmentId))
}

// CreateL0Segment registers a new L0 segment. Duplicate creates are dropped.
func (m *SegmentManager) CreateL0Segment(meta *streamingpb.SegmentAssignmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.l0Segments[meta.SegmentId]; exists {
		m.Logger().Warn("L0 segment already exists", zap.Int64("segmentID", meta.SegmentId))
		return
	}
	m.l0Segments[meta.SegmentId] = newL0Segment(meta, nil)
	m.l0Index[l0KeyFromMeta(meta)] = meta.SegmentId
	m.Logger().Info("Created L0 segment", zap.Int64("segmentID", meta.SegmentId))
}

// ObserveInsert fans an insert message out to the L1 segments named in its
// partition assignments. Missing segments are logged once and skipped.
func (m *SegmentManager) ObserveInsert(msg message.ImmutableInsertMessageV1) {
	header := msg.Header()
	for _, partition := range header.GetPartitions() {
		segmentID := partition.GetSegmentAssignment().GetSegmentId()

		m.mu.RLock()
		segment, exists := m.l1Segments[segmentID]
		m.mu.RUnlock()

		if !exists {
			m.Logger().Warn("L1 segment not found for insert", zap.Int64("segmentID", segmentID))
			continue
		}
		segment.ObserveInsert(msg)
		m.tryScheduleL1(segment)
	}
}

// ObserveDelete lazily routes a delete message to the current L0 segment for
// its vchannel/collection/partition. DeleteMessageHeader intentionally carries
// only aggregate collection-level stats; the partition route comes from the
// DeleteRequest body, matching flushcommon L0 write-buffer semantics.
func (m *SegmentManager) ObserveDelete(msg message.ImmutableDeleteMessageV1, schema *schemapb.CollectionSchema) {
	body, err := msg.Body()
	if err != nil {
		m.Logger().Warn("failed to read delete message body", zap.Error(err))
		return
	}
	if body == nil || body.GetPrimaryKeys() == nil {
		return
	}

	key := l0SegmentKey{
		vchannel:     msg.VChannel(),
		collectionID: msg.Header().GetCollectionId(),
		partitionID:  body.GetPartitionID(),
	}

	m.mu.Lock()
	segment := m.getOrCreateL0SegmentLocked(key, msg.TimeTick(), schema)
	m.mu.Unlock()

	segment.ObserveDelete(msg, schema)
	m.tryScheduleL0(segment)
}

func (m *SegmentManager) getOrCreateL0SegmentLocked(key l0SegmentKey, timetick uint64, schema *schemapb.CollectionSchema) *L0Segment {
	if segmentID, ok := m.l0Index[key]; ok {
		if segment := m.l0Segments[segmentID]; segment != nil && !segment.IsSealed() {
			return segment
		}
	}
	segmentID, err := m.allocator.AllocOne()
	if err != nil {
		panic(err)
	}
	segment := newL0Segment(&streamingpb.SegmentAssignmentMeta{
		CollectionId:       key.collectionID,
		PartitionId:        key.partitionID,
		SegmentId:          segmentID,
		Vchannel:           key.vchannel,
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		CheckpointTimeTick: previousTimeTick(timetick),
		Stat: &streamingpb.SegmentAssignmentStat{
			MaxRows:               math.MaxUint64,
			MaxBinarySize:         math.MaxUint64,
			CreateTimestamp:       tsoutil.PhysicalTime(timetick).Unix(),
			LastModifiedTimestamp: tsoutil.PhysicalTime(timetick).Unix(),
			CreateSegmentTimeTick: previousTimeTick(timetick),
			Level:                 datapb.SegmentLevel_L0,
		},
		PersistedStorage: &streamingpb.SegmentAssignmentMeta_L0{
			L0: &streamingpb.L0SegmentPersistedStorage{},
		},
	}, schema)
	m.l0Segments[segmentID] = segment
	m.l0Index[key] = segmentID
	m.Logger().Info("Created lazy L0 segment",
		zap.Int64("segmentID", segmentID),
		zap.String("vchannel", key.vchannel),
		zap.Int64("collectionID", key.collectionID),
		zap.Int64("partitionID", key.partitionID))
	return segment
}

func l0KeyFromMeta(meta *streamingpb.SegmentAssignmentMeta) l0SegmentKey {
	return l0SegmentKey{
		vchannel:     meta.GetVchannel(),
		collectionID: meta.GetCollectionId(),
		partitionID:  meta.GetPartitionId(),
	}
}

func previousTimeTick(timetick uint64) uint64 {
	if timetick == 0 {
		return 0
	}
	return timetick - 1
}

// FlushL1Segment seals the given segment and schedules any remaining chunks.
func (m *SegmentManager) FlushL1Segment(segmentID int64, timetick uint64) {
	m.mu.RLock()
	segment, exists := m.l1Segments[segmentID]
	m.mu.RUnlock()
	if !exists {
		m.Logger().Warn("L1 segment not found for flush", zap.Int64("segmentID", segmentID))
		return
	}
	segment.Flush(timetick)
	m.tryScheduleL1(segment)
}

// FlushL0Segment seals the given L0 segment and schedules any remaining chunks.
func (m *SegmentManager) FlushL0Segment(segmentID int64, timetick uint64) {
	m.mu.RLock()
	segment, exists := m.l0Segments[segmentID]
	m.mu.RUnlock()
	if !exists {
		m.Logger().Warn("L0 segment not found for flush", zap.Int64("segmentID", segmentID))
		return
	}
	segment.Flush(timetick)
	m.tryScheduleL0(segment)
}

// FlushSegment seals a segment when the caller does not know whether it is L0
// or L1. Missing segments are debug-only because flush semantics can be
// replayed after a terminal snapshot has already removed the manager entry.
func (m *SegmentManager) FlushSegment(segmentID int64, timetick uint64) (*streamingpb.SegmentAssignmentMeta, bool) {
	m.mu.RLock()
	l1Segment, l1Exists := m.l1Segments[segmentID]
	l0Segment, l0Exists := m.l0Segments[segmentID]
	m.mu.RUnlock()

	if l1Exists {
		l1Segment.Flush(timetick)
		m.tryScheduleL1(l1Segment)
		return l1Segment.Snapshot(), true
	}
	if l0Exists {
		l0Segment.Flush(timetick)
		m.tryScheduleL0(l0Segment)
		return l0Segment.Snapshot(), true
	}
	m.Logger().Debug("segment not found for flush", zap.Int64("segmentID", segmentID))
	return nil, false
}

// FlushSegments seals the provided segment IDs and schedules their remaining
// chunks for sync. The returned map contains snapshots for segments found.
func (m *SegmentManager) FlushSegments(segmentIDs map[int64]struct{}, timetick uint64) map[int64]*streamingpb.SegmentAssignmentMeta {
	snapshots := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(segmentIDs))
	for segmentID := range segmentIDs {
		if snapshot, ok := m.FlushSegment(segmentID, timetick); ok {
			snapshots[segmentID] = snapshot
		}
	}
	return snapshots
}

// FlushAllSegments seals every managed segment.
func (m *SegmentManager) FlushAllSegments(timetick uint64) map[int64]*streamingpb.SegmentAssignmentMeta {
	return m.flushSegmentsBy(timetick, func(*streamingpb.SegmentAssignmentMeta) bool {
		return true
	})
}

// FlushSegmentsByCollection seals every managed segment of a collection.
func (m *SegmentManager) FlushSegmentsByCollection(collectionID int64, timetick uint64) map[int64]*streamingpb.SegmentAssignmentMeta {
	return m.flushSegmentsBy(timetick, func(meta *streamingpb.SegmentAssignmentMeta) bool {
		return meta.GetCollectionId() == collectionID
	})
}

// FlushSegmentsByPartition seals every managed segment of a partition.
func (m *SegmentManager) FlushSegmentsByPartition(partitionID int64, timetick uint64) map[int64]*streamingpb.SegmentAssignmentMeta {
	return m.flushSegmentsBy(timetick, func(meta *streamingpb.SegmentAssignmentMeta) bool {
		return meta.GetPartitionId() == partitionID
	})
}

func (m *SegmentManager) flushSegmentsBy(
	timetick uint64,
	predicate func(*streamingpb.SegmentAssignmentMeta) bool,
) map[int64]*streamingpb.SegmentAssignmentMeta {
	segmentIDs := make(map[int64]struct{})
	for segmentID, snapshot := range m.GetSnapshots() {
		if predicate(snapshot) {
			segmentIDs[segmentID] = struct{}{}
		}
	}
	return m.FlushSegments(segmentIDs, timetick)
}

// tryScheduleL1 claims the oldest sealed chunk (if any, and if no sync is
// already in flight) and submits it to the scheduler. The returned task's
// OnComplete closure applies the binlog back to the segment and retries the
// next chunk if one became available.
func (m *SegmentManager) tryScheduleL1(segment *L1Segment) {
	chunk, ok := segment.BeginSaveChunk()
	if !ok {
		return
	}
	meta := segment.GetMeta()
	schema := segment.GetSchema()
	if schema == nil {
		// Recovery path yields segments without schema until SchemaProvider
		// lands. Release the slot so we don't block on the next create.
		m.Logger().Warn("L1 segment has nil schema; cannot sync",
			zap.Int64("segmentID", meta.SegmentId))
		segment.AbortSync()
		return
	}
	syncContext := segment.ChunkSyncContext(chunk)
	task := NewInsertChunkTask(
		chunk,
		schema,
		meta.CollectionId,
		meta.PartitionId,
		meta.SegmentId,
		meta.GetStorageVersion(),
		syncContext.ManifestPath,
		syncContext.Flush,
		syncContext.SegmentRows,
		syncContext.ColumnGroups,
		syncContext.PreviousBinlog,
		m.chunkManager,
		m.allocator,
		m.storageConfig,
		func(result *InsertChunkTaskResult, err error) {
			m.onInsertTaskDone(segment, result, err)
		},
	)
	m.scheduler.AddTask(task)
}

// tryScheduleL0 mirrors tryScheduleL1 for delete chunks.
func (m *SegmentManager) tryScheduleL0(segment *L0Segment) {
	chunk, ok := segment.BeginSaveChunk()
	if !ok {
		return
	}
	meta := segment.GetMeta()
	task := NewDeleteChunkTask(
		chunk,
		meta.CollectionId,
		meta.PartitionId,
		meta.SegmentId,
		segment.GetSchema(),
		m.chunkManager,
		m.allocator,
		m.storageConfig,
		func(result *DeleteChunkTaskResult, err error) {
			m.onDeleteTaskDone(segment, result, err)
		},
	)
	m.scheduler.AddTask(task)
}

// onInsertTaskDone is invoked exactly once per L1 task by the scheduler.
func (m *SegmentManager) onInsertTaskDone(segment *L1Segment, result *InsertChunkTaskResult, err error) {
	if err != nil {
		// Terminal failure (non-retryable or ctx canceled): release the slot
		// so the segment can retry on the next observation or flush call.
		m.Logger().Warn("L1 sync task terminal failure",
			zap.Int64("segmentID", segment.GetMeta().SegmentId),
			zap.Error(err))
		segment.AbortSync()
		return
	}
	var binlog *streamingpb.L1SegmentBinLogs
	var manifestPath string
	var mergedStats *datapb.FieldBinlog
	if result != nil {
		binlog = result.Binlog
		manifestPath = result.ManifestPath
		mergedStats = result.MergedStatsBinlog
	}
	segment.SaveChunkDone(&SaveChunkDoneRequest{Binlog: binlog, ManifestPath: manifestPath, MergedStats: mergedStats})
	// A chunk just finished — check if another sealed chunk is ready.
	m.tryScheduleL1(segment)
}

// onDeleteTaskDone is invoked exactly once per L0 task by the scheduler.
func (m *SegmentManager) onDeleteTaskDone(segment *L0Segment, result *DeleteChunkTaskResult, err error) {
	if err != nil {
		m.Logger().Warn("L0 sync task terminal failure",
			zap.Int64("segmentID", segment.GetMeta().SegmentId),
			zap.Error(err))
		segment.AbortSync()
		return
	}
	var binlog any
	if result != nil {
		binlog = result.Binlog
	}
	_ = binlog
	req := &SaveDeleteChunkDoneRequest{}
	if result != nil {
		req.Binlog = result.Binlog
	}
	segment.SaveChunkDone(req)
	m.tryScheduleL0(segment)
}

// GetDirtySnapshots collects dirty meta snapshots from all L0/L1 segments.
// Returns a map of segmentID → snapshot. Callers are expected to persist the
// returned snapshots atomically (recovery_storage background task handles that).
func (m *SegmentManager) GetDirtySnapshots() map[int64]*streamingpb.SegmentAssignmentMeta {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshots := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	for segmentID, segment := range m.l1Segments {
		if snap := segment.ConsumeSnapshot(); snap != nil {
			snapshots[segmentID] = snap
			if snap.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
				delete(m.l1Segments, segmentID)
			}
		}
	}
	for segmentID, segment := range m.l0Segments {
		if snap := segment.ConsumeSnapshot(); snap != nil {
			snapshots[segmentID] = snap
			if snap.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
				m.deleteL0IndexLocked(segmentID, snap)
				delete(m.l0Segments, segmentID)
			}
		}
	}
	return snapshots
}

// GetSnapshots returns deep-copied snapshots of all managed segments without
// consuming dirty state.
func (m *SegmentManager) GetSnapshots() map[int64]*streamingpb.SegmentAssignmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshots := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(m.l1Segments)+len(m.l0Segments))
	for segmentID, segment := range m.l1Segments {
		snapshots[segmentID] = segment.Snapshot()
	}
	for segmentID, segment := range m.l0Segments {
		snapshots[segmentID] = segment.Snapshot()
	}
	return snapshots
}

// HasDirtySnapshot reports whether any segment has unconsumed dirty metadata.
func (m *SegmentManager) HasDirtySnapshot() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, segment := range m.l1Segments {
		if segment.IsDirty() {
			return true
		}
	}
	for _, segment := range m.l0Segments {
		if segment.IsDirty() {
			return true
		}
	}
	return false
}

// SyncSafeTimeTick returns the largest pchannel timetick that can be used as a
// WAL consume checkpoint without getting ahead of durable segment data.
func (m *SegmentManager) SyncSafeTimeTick(observedTimeTick uint64) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	safeTimeTick := uint64(math.MaxUint64)
	for _, segment := range m.l1Segments {
		if segmentSafe := segment.SyncSafeTimeTick(); segmentSafe < safeTimeTick {
			safeTimeTick = segmentSafe
		}
	}
	for _, segment := range m.l0Segments {
		if segmentSafe := segment.SyncSafeTimeTick(); segmentSafe < safeTimeTick {
			safeTimeTick = segmentSafe
		}
	}
	if safeTimeTick == uint64(math.MaxUint64) || observedTimeTick < safeTimeTick {
		return observedTimeTick
	}
	return safeTimeTick
}

// SealStaleChunks seals non-empty growing chunks that are older than
// staleDuration and schedules them for sync.
func (m *SegmentManager) SealStaleChunks(timetick uint64, staleDuration time.Duration) {
	l1Segments := make([]*L1Segment, 0)
	l0Segments := make([]*L0Segment, 0)

	m.mu.RLock()
	for _, segment := range m.l1Segments {
		l1Segments = append(l1Segments, segment)
	}
	for _, segment := range m.l0Segments {
		l0Segments = append(l0Segments, segment)
	}
	m.mu.RUnlock()

	for _, segment := range l1Segments {
		if segment.SealStaleGrowingChunk(timetick, staleDuration) {
			m.tryScheduleL1(segment)
		}
	}
	for _, segment := range l0Segments {
		if segment.SealStaleGrowingChunk(timetick, staleDuration) {
			m.tryScheduleL0(segment)
		}
	}
}

// RecoverFromSnapshot rebuilds segment state from previously persisted
// assignments without schema lookup. Kept for tests and callers that do not
// have vchannel schema history available.
func (m *SegmentManager) RecoverFromSnapshot(segmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta) {
	m.RecoverFromSnapshotWithSchema(context.Background(), segmentAssignments, nil)
}

// RecoverFromSnapshotWithSchema rebuilds segment state from previously
// persisted assignments and resolves L1 schemas through the provided vchannel
// schema history when available.
func (m *SegmentManager) RecoverFromSnapshotWithSchema(
	ctx context.Context,
	segmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta,
	schemaProvider SchemaProvider,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for segmentID, meta := range segmentAssignments {
		cloned := proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
		if cloned.Stat == nil {
			cloned.Stat = &streamingpb.SegmentAssignmentStat{}
		}
		switch cloned.PersistedStorage.(type) {
		case *streamingpb.SegmentAssignmentMeta_L0:
			m.l0Segments[segmentID] = &L0Segment{
				meta:   cloned,
				schema: m.recoverSchema(ctx, cloned, schemaProvider),
				dirty:  false,
			}
			m.l0Index[l0KeyFromMeta(cloned)] = segmentID
		case *streamingpb.SegmentAssignmentMeta_L1:
			m.l1Segments[segmentID] = &L1Segment{
				meta:   cloned,
				schema: m.recoverSchema(ctx, cloned, schemaProvider),
				dirty:  false,
			}
		default:
			// Meta without a persisted_storage oneof: classify by stat.Level if set,
			// otherwise treat as L1 (legacy behavior).
			m.l1Segments[segmentID] = &L1Segment{
				meta:   cloned,
				schema: m.recoverSchema(ctx, cloned, schemaProvider),
				dirty:  false,
			}
		}
	}
	m.Logger().Info("Recovered segments from snapshot",
		zap.Int("l0Count", len(m.l0Segments)),
		zap.Int("l1Count", len(m.l1Segments)))
}

func (m *SegmentManager) recoverSchema(
	ctx context.Context,
	meta *streamingpb.SegmentAssignmentMeta,
	schemaProvider SchemaProvider,
) *schemapb.CollectionSchema {
	if schemaProvider == nil {
		return nil
	}
	schema, err := schemaProvider.GetSchema(ctx, meta.GetVchannel(), recoveredSchemaTimeTick(meta))
	if err != nil {
		m.Logger().Warn("failed to recover segment schema",
			zap.Int64("segmentID", meta.GetSegmentId()),
			zap.String("vchannel", meta.GetVchannel()),
			zap.Uint64("timetick", recoveredSchemaTimeTick(meta)),
			zap.Error(err))
		return nil
	}
	return schema
}

func recoveredSchemaTimeTick(meta *streamingpb.SegmentAssignmentMeta) uint64 {
	if meta.GetCheckpointTimeTick() != 0 {
		return meta.GetCheckpointTimeTick()
	}
	if meta.GetStat().GetBeginTimeTick() != 0 {
		return meta.GetStat().GetBeginTimeTick()
	}
	return meta.GetStat().GetCreateSegmentTimeTick()
}

// RemoveSegment drops the segment from management.
func (m *SegmentManager) RemoveSegment(segmentID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if segment, ok := m.l0Segments[segmentID]; ok {
		m.deleteL0IndexLocked(segmentID, segment.GetMeta())
	}
	delete(m.l0Segments, segmentID)
	delete(m.l1Segments, segmentID)
	m.Logger().Info("Removed segment", zap.Int64("segmentID", segmentID))
}

func (m *SegmentManager) deleteL0IndexLocked(segmentID int64, meta *streamingpb.SegmentAssignmentMeta) {
	key := l0KeyFromMeta(meta)
	if indexedSegmentID, ok := m.l0Index[key]; ok && indexedSegmentID == segmentID {
		delete(m.l0Index, key)
	}
}

// Drain waits until all submitted sync tasks and their completion callbacks are
// finished. It does not close the scheduler, so callers can still consume the
// final dirty snapshots after Drain returns.
func (m *SegmentManager) Drain(ctx context.Context) error {
	return m.scheduler.Drain(ctx)
}

// Close stops the scheduler. In-flight tasks complete their current Poll then
// fail with context.Canceled, releasing segments via AbortSync.
func (m *SegmentManager) Close() {
	m.scheduler.Close()
	m.Logger().Info("Segment manager closed")
}
