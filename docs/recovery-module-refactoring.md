# Recovery Module Refactoring with Flusher Integration

## Overview

This document summarizes the experience and best practices from refactoring the Milvus recovery module to integrate flusher functionality with in-memory buffering and persistence scheduling.

## Architecture Design

### Core Components

1. **SegmentManager** (`gsegment/segment_manager.go`)
   - Manages both L0 (delete) and L1 (insert) segments
   - Thread-safe operations with RWMutex
   - Handles segment lifecycle: Create → Observe → Flush → Persist

2. **Chunk-based Buffering**
   - L0 segments: 1MB chunks for delete operations
   - L1 segments: 16MB chunks for insert operations
   - Automatic chunk sealing when threshold reached

3. **Task Scheduling** (`gsegment/sync_scheduler.go`)
   - CPU-bound tasks for data serialization
   - IO-bound tasks for storage upload
   - State machine pattern for task execution

### Key Design Decisions

#### 1. Memory Management Strategy
```go
// L0 Segment - smaller chunks for deletes
const L0ChunkThreshold = 1 * 1024 * 1024  // 1MB

// L1 Segment - larger chunks for inserts
const L1ChunkThreshold = 16 * 1024 * 1024 // 16MB
```

**Rationale**: Delete operations are typically smaller and more frequent, while inserts are bulk operations that benefit from larger buffers.

#### 2. Proto Message Copying
```go
// Always use proto.Clone for deep copying proto messages
clonedMeta := proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
```

**Lesson Learned**: Direct struct copying of proto messages fails because they contain unexported fields (like mutex). Always use `proto.Clone()`.

#### 3. Segment State Tracking
```go
type SegmentState int
const (
    SegmentStateGrowing SegmentState = iota
    SegmentStateSealed
    SegmentStateFlushed
)
```

**Design**: Clear state transitions prevent race conditions and ensure data consistency during recovery.

## Implementation Challenges & Solutions

### 1. Import Cycle Prevention

**Problem**: Circular dependency between recovery and gsegment packages.

**Solution**:
- Move shared types to gsegment package
- Use interface abstraction where needed
- Avoid importing recovery package from gsegment

### 2. Thread Safety in Concurrent Operations

**Problem**: Multiple goroutines accessing segment state during flush and persist operations.

**Solution**:
```go
// Use fine-grained locking
func (m *SegmentManager) ObserveInsert(msg message.ImmutableInsertMessageV1) {
    m.mu.RLock()
    segment, exists := m.l1Segments[segmentID]
    m.mu.RUnlock()

    if exists {
        segment.ObserveInsert(msg) // segment has its own mutex
    }
}
```

### 3. Truncate Collection Handling

**Problem**: Truncate operations were not properly flushing segments through SegmentManager.

**Solution**:
```go
func (r *recoveryStorageImpl) handleTruncateCollection(msg message.ImmutableTruncateCollectionMessageV2) {
    // Flush through both legacy and new systems
    r.flushSegments(msg, segments)

    if r.segmentManager != nil {
        for segmentID := range segments {
            r.segmentManager.FlushL0Segment(segmentID, msg.TimeTick())
            r.segmentManager.FlushL1Segment(segmentID, msg.TimeTick())
        }
    }
}
```

### 4. Mock Message Interface Complexity

**Problem**: Message interfaces require many methods, making mocking complex.

**Solution**: Create comprehensive mock implementations with all required methods:
```go
type mockInsertMessage struct {
    header   *messagespb.InsertMessageHeader
    vchannel string
    timetick uint64
}

// Implement all required methods
func (m *mockInsertMessage) Header() *messagespb.InsertMessageHeader { return m.header }
func (m *mockInsertMessage) VChannel() string { return m.vchannel }
func (m *mockInsertMessage) TimeTick() uint64 { return m.timetick }
// ... additional methods
```

## Testing Strategy

### 1. Unit Testing
- Mock ChunkManager for storage operations
- Create mock message implementations
- Test all state transitions
- Verify thread safety with concurrent operations

### 2. Integration Testing
```python
# E2E test phases
1. Create collection and insert data (L1 segments)
2. Delete operations (L0 segments)
3. Mixed operations
4. Index creation and queries
5. Data integrity verification
6. Recovery simulation
```

### 3. Cluster Testing
```bash
# Use milvus-cluster-manage skill for full E2E testing
milvus_control -s start_milvus_full  # Start with streaming
milvus_test python_client             # Run Python tests
```

## Performance Optimizations

### 1. Batch Processing
- Group multiple messages before triggering persistence
- Reduce storage API calls through batching

### 2. Parallel Task Execution
```go
scheduler := NewSyncScheduler(
    cpuWorkers: 4,  // For serialization
    ioWorkers: 2,   // For upload
)
```

### 3. Memory Buffer Reuse
- Reuse chunk buffers where possible
- Clear rather than allocate new buffers

## Best Practices

### 1. Error Handling
```go
// Always handle persistence errors gracefully
if err := task.Poll(ctx); err != nil {
    if err.Error() == "continue" {
        // Task needs more polling
        continue
    }
    // Log and handle actual error
    log.Warn("task failed", zap.Error(err))
    return err
}
```

### 2. Logging and Observability
```go
// Add structured logging at key points
m.Logger().Info("Segment flushed",
    zap.Int64("segmentID", segmentID),
    zap.Uint64("rows", segment.Rows()),
    zap.Uint64("size", segment.BinarySize()))
```

### 3. Resource Management
```go
// Always clean up resources
defer func() {
    scheduler.Close()
    segmentManager.Close()
}()
```

### 4. Backward Compatibility
- Maintain existing interfaces while adding new functionality
- Use feature flags for gradual rollout
- Test with both old and new code paths

## Debugging Tips

### 1. Check Segment States
```bash
# Look for segment state transitions in logs
grep "segment state" milvus-logs/streamingnode.stdout.log
```

### 2. Monitor Chunk Accumulation
```go
// Add metrics for chunk counts
log.Info("Chunk status",
    zap.Int("sealedChunks", len(segment.sealedChunks)),
    zap.Bool("hasGrowing", segment.growingChunk != nil))
```

### 3. Verify Persistence
```bash
# Check if binlogs are created in storage
ls -la ${MILVUS_VOLUME_DIRECTORY}/*/docker-volumes/minio/a-bucket/
```

## Common Pitfalls to Avoid

1. **Don't copy proto messages directly** - Use proto.Clone()
2. **Don't access fields without locks** - Even reads need RLock
3. **Don't ignore checkpoint updates** - They're critical for recovery
4. **Don't skip nil checks** - Proto fields may be nil after recovery
5. **Don't mix old and new flush paths** - Ensure both are called during transition

## Migration Strategy

### Phase 1: Parallel Operation
- Run new SegmentManager alongside existing recovery storage
- Both systems process messages independently
- Verify consistency between systems

### Phase 2: Gradual Cutover
- Start using SegmentManager for new segments
- Migrate existing segments during recovery
- Monitor for any issues

### Phase 3: Complete Migration
- Remove old flusher code
- Rely entirely on SegmentManager
- Clean up legacy interfaces

## Monitoring and Metrics

Key metrics to track:
- Segment creation rate
- Chunk accumulation size
- Flush latency
- Persistence success rate
- Recovery time
- Memory usage per segment

## Conclusion

The recovery module refactoring successfully:
1. Unified L0/L1 segment management
2. Implemented efficient in-memory buffering
3. Added robust persistence scheduling
4. Improved crash recovery reliability
5. Maintained backward compatibility

The modular design allows for future enhancements while maintaining system stability.