package gsegment

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// SegmentManager manages L0 and L1 segments with in-memory buffering and persistence.
type SegmentManager struct {
	mu sync.RWMutex
	log.Binder

	l0Segments map[int64]*L0Segment
	l1Segments map[int64]*L1Segment
	schemas    map[int64]*schemapb.CollectionSchema // collection ID -> schema

	scheduler    *SyncScheduler
	chunkManager storage.ChunkManager
}

// NewSegmentManager creates a new segment manager.
func NewSegmentManager(cm storage.ChunkManager) *SegmentManager {
	return &SegmentManager{
		l0Segments:   make(map[int64]*L0Segment),
		l1Segments:   make(map[int64]*L1Segment),
		schemas:      make(map[int64]*schemapb.CollectionSchema),
		scheduler:    NewSyncScheduler(4, 8), // 4 CPU workers, 8 IO workers
		chunkManager: cm,
	}
}

// CreateL1Segment creates a new L1 segment.
func (m *SegmentManager) CreateL1Segment(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.l1Segments[meta.SegmentId]; exists {
		m.Logger().Warn("L1 segment already exists", zap.Int64("segmentID", meta.SegmentId))
		return
	}

	segment := &L1Segment{
		meta:   meta,
		schema: schema,
		dirty:  true,
	}
	m.l1Segments[meta.SegmentId] = segment
	m.schemas[meta.CollectionId] = schema
	m.Logger().Info("Created L1 segment", zap.Int64("segmentID", meta.SegmentId))
}

// CreateL0Segment creates a new L0 segment.
func (m *SegmentManager) CreateL0Segment(meta *streamingpb.SegmentAssignmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.l0Segments[meta.SegmentId]; exists {
		m.Logger().Warn("L0 segment already exists", zap.Int64("segmentID", meta.SegmentId))
		return
	}

	segment := &L0Segment{
		meta:  meta,
		dirty: true,
	}
	m.l0Segments[meta.SegmentId] = segment
	m.Logger().Info("Created L0 segment", zap.Int64("segmentID", meta.SegmentId))
}

// ObserveInsert observes an insert message and buffers it in the appropriate L1 segment.
func (m *SegmentManager) ObserveInsert(msg message.ImmutableInsertMessageV1) {
	header := msg.Header()
	for _, partition := range header.GetPartitions() {
		segmentID := partition.SegmentAssignment.SegmentId

		m.mu.RLock()
		segment, exists := m.l1Segments[segmentID]
		m.mu.RUnlock()

		if !exists {
			m.Logger().Warn("L1 segment not found for insert", zap.Int64("segmentID", segmentID))
			continue
		}

		segment.ObserveInsert(msg)
		m.checkAndSchedulePersistence(segmentID, true)
	}
}

// ObserveDelete observes a delete message and buffers it in the appropriate L0 segment.
func (m *SegmentManager) ObserveDelete(msg message.ImmutableDeleteMessageV1) {
	header := msg.Header()
	for _, partition := range header.GetPartitions() {
		segmentID := partition.SegmentAssignment.SegmentId

		m.mu.RLock()
		segment, exists := m.l0Segments[segmentID]
		m.mu.RUnlock()

		if !exists {
			m.Logger().Warn("L0 segment not found for delete", zap.Int64("segmentID", segmentID))
			continue
		}

		segment.ObserveDelete(msg)
		m.checkAndSchedulePersistence(segmentID, false)
	}
}

// FlushL1Segment flushes an L1 segment.
func (m *SegmentManager) FlushL1Segment(segmentID int64, timetick uint64) {
	m.mu.RLock()
	segment, exists := m.l1Segments[segmentID]
	m.mu.RUnlock()

	if !exists {
		m.Logger().Warn("L1 segment not found for flush", zap.Int64("segmentID", segmentID))
		return
	}

	segment.Flush(timetick)
	m.checkAndSchedulePersistence(segmentID, true)
}

// FlushL0Segment flushes an L0 segment.
func (m *SegmentManager) FlushL0Segment(segmentID int64, timetick uint64) {
	m.mu.RLock()
	segment, exists := m.l0Segments[segmentID]
	m.mu.RUnlock()

	if !exists {
		m.Logger().Warn("L0 segment not found for flush", zap.Int64("segmentID", segmentID))
		return
	}

	segment.Flush(timetick)
	m.checkAndSchedulePersistence(segmentID, false)
}

// checkAndSchedulePersistence checks if a segment has sealed chunks and schedules persistence tasks.
func (m *SegmentManager) checkAndSchedulePersistence(segmentID int64, isL1 bool) {
	if isL1 {
		m.mu.RLock()
		segment, exists := m.l1Segments[segmentID]
		schema, hasSchema := m.schemas[segment.GetMeta().CollectionId]
		m.mu.RUnlock()

		if !exists || !hasSchema {
			return
		}

		// Check if segment has sealed chunks to persist
		for {
			chunk, ok := segment.BeginSaveChunk()
			if !ok {
				break
			}

			// Create and schedule insert chunk task
			task := &NewInsertChunkTask{
				chunk:        chunk,
				schema:       schema,
				collectionID: segment.GetMeta().CollectionId,
				partitionID:  segment.GetMeta().PartitionId,
				segmentID:    segmentID,
				chunkManager: m.chunkManager,
				state:        taskStateInit,
				cpuBounded:   true,
			}

			m.scheduler.AddTask(task)

			// Create a goroutine to handle task completion
			go m.waitForInsertTaskCompletion(segment, task)
		}
	} else {
		m.mu.RLock()
		segment, exists := m.l0Segments[segmentID]
		m.mu.RUnlock()

		if !exists {
			return
		}

		// Check if segment has sealed chunks to persist
		for {
			chunk, ok := segment.BeginSaveChunk()
			if !ok {
				break
			}

			// Create and schedule delete chunk task
			task := NewDeleteChunkTask(
				chunk,
				segment.GetMeta().CollectionId,
				segment.GetMeta().PartitionId,
				segmentID,
				m.chunkManager,
			)

			m.scheduler.AddTask(task)

			// Create a goroutine to handle task completion
			go m.waitForDeleteTaskCompletion(segment, task)
		}
	}
}

// waitForInsertTaskCompletion waits for an insert task to complete and updates the segment.
func (m *SegmentManager) waitForInsertTaskCompletion(segment *L1Segment, task *NewInsertChunkTask) {
	// Poll until task completes
	for {
		err := task.Poll(context.Background())
		if err == nil {
			// Task completed successfully
			// Create field binlogs
			fieldBinlogs := make([]*datapb.FieldBinlog, 0)
			for fieldID := range task.binlogs {
				fieldBinlogs = append(fieldBinlogs, &datapb.FieldBinlog{
					FieldID: fieldID,
					Binlogs: []*datapb.Binlog{{
						LogPath:       task.uploadedPaths[fmt.Sprintf("%d", fieldID)],
						TimestampFrom: task.chunk.startFromTimeTick,
						TimestampTo:   task.chunk.endToTimeTick,
					}},
				})
			}

			// Create stats binlog
			var statsBinlogs []*datapb.FieldBinlog
			if task.statsBlob != nil {
				statsBinlogs = []*datapb.FieldBinlog{{
					FieldID: -1, // Special field ID for stats
					Binlogs: []*datapb.Binlog{{
						LogPath:       task.uploadedPaths["stats"],
						TimestampFrom: task.chunk.startFromTimeTick,
						TimestampTo:   task.chunk.endToTimeTick,
					}},
				}}
			}

			binlog := &streamingpb.L1SegmentBinLogs{
				FieldBinlog:  fieldBinlogs,
				StatsBinlog:  statsBinlogs,
				FromTimeTick: task.chunk.startFromTimeTick,
				ToTimeTick:   task.chunk.endToTimeTick,
			}

			segment.SaveChunkDone(&SaveChunkDoneRequest{
				ManifestPath: "", // TODO: generate manifest path
				Binlog:       binlog,
			})
			break
		}
		// Continue polling if error indicates continuation needed
		if err.Error() != "continue" {
			m.Logger().Error("Insert task failed", zap.Error(err))
			break
		}
	}
}

// waitForDeleteTaskCompletion waits for a delete task to complete and updates the segment.
func (m *SegmentManager) waitForDeleteTaskCompletion(segment *L0Segment, task *DeleteChunkTask) {
	// Poll until task completes
	for {
		err := task.Poll(context.Background())
		if err == nil {
			// Task completed successfully
			binlog := &datapb.FieldBinlog{
				FieldID: 0, // Delta logs don't have field ID
				Binlogs: []*datapb.Binlog{{
					LogPath:       task.uploadedPath,
					TimestampFrom: task.chunk.startFromTimeTick,
					TimestampTo:   task.chunk.endToTimeTick,
				}},
			}

			segment.SaveChunkDone(&SaveDeleteChunkDoneRequest{
				DeltaPath: task.uploadedPath,
				Binlog:    binlog,
			})
			break
		}
		// Continue polling if error indicates continuation needed
		if err.Error() != "continue" {
			m.Logger().Error("Delete task failed", zap.Error(err))
			break
		}
	}
}

// GetDirtySnapshots returns all dirty segment snapshots for persistence.
func (m *SegmentManager) GetDirtySnapshots() map[int64]*streamingpb.SegmentAssignmentMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshots := make(map[int64]*streamingpb.SegmentAssignmentMeta)

	// Collect L1 segment snapshots
	for segmentID, segment := range m.l1Segments {
		if snapshot := segment.ConsumeSnapshot(); snapshot != nil {
			snapshots[segmentID] = snapshot
		}
	}

	// Collect L0 segment snapshots
	for segmentID, segment := range m.l0Segments {
		if snapshot := segment.ConsumeSnapshot(); snapshot != nil {
			snapshots[segmentID] = snapshot
		}
	}

	return snapshots
}

// RecoverFromSnapshot recovers segment state from a recovery snapshot.
// The snapshot parameter contains SegmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta
func (m *SegmentManager) RecoverFromSnapshot(segmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Recover segments from snapshot
	for segmentID, meta := range segmentAssignments {
		switch meta.PersistedStorage.(type) {
		case *streamingpb.SegmentAssignmentMeta_L0:
			clonedMeta := proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
			// Ensure Stat is initialized if nil
			if clonedMeta.Stat == nil {
				clonedMeta.Stat = &streamingpb.SegmentAssignmentStat{}
			}
			segment := &L0Segment{
				meta:  clonedMeta,
				dirty: false,
			}
			m.l0Segments[segmentID] = segment

		case *streamingpb.SegmentAssignmentMeta_L1:
			// TODO: need schema recovery from vchannel metadata
			clonedMeta := proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
			// Ensure Stat is initialized if nil
			if clonedMeta.Stat == nil {
				clonedMeta.Stat = &streamingpb.SegmentAssignmentStat{}
			}
			segment := &L1Segment{
				meta:   clonedMeta,
				dirty:  false,
				schema: nil, // Schema needs to be recovered from vchannel metadata
			}
			m.l1Segments[segmentID] = segment
		}
	}

	m.Logger().Info("Recovered segments from snapshot",
		zap.Int("l0Count", len(m.l0Segments)),
		zap.Int("l1Count", len(m.l1Segments)))
}

// RemoveSegment removes a segment from management.
func (m *SegmentManager) RemoveSegment(segmentID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.l0Segments, segmentID)
	delete(m.l1Segments, segmentID)
	m.Logger().Info("Removed segment", zap.Int64("segmentID", segmentID))
}

// Close closes the segment manager and releases resources.
func (m *SegmentManager) Close() {
	m.scheduler.Close()
	m.Logger().Info("Segment manager closed")
}