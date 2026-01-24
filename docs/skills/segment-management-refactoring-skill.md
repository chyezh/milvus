# Skill: Milvus Segment Management Refactoring

## Purpose
This skill guides the refactoring of segment management systems in Milvus, particularly for integrating buffering, persistence, and recovery mechanisms.

## Prerequisites
- Understanding of Milvus architecture (coordinators, nodes, segments)
- Familiarity with Go concurrency patterns
- Knowledge of protobuf and CGO bindings
- Access to Milvus development environment

## Workflow

### Step 1: Analysis Phase
```bash
# Analyze existing code structure
rg -t go "type.*Segment" internal/
rg -t go "flush|persist|buffer" internal/streamingnode/
```

**Key areas to examine:**
- Current segment interfaces
- Existing flush mechanisms
- Recovery checkpoints
- Message processing flow

### Step 2: Design Phase

Create the following core components:

#### Component Structure
```
gsegment/
├── segment_manager.go      # Main coordinator
├── l0_segment.go           # Delete operations
├── l1_segment.go           # Insert operations
├── insert_chunk.go         # Insert buffering
├── delete_chunk.go         # Delete buffering
├── task.go                 # Persistence tasks
└── sync_scheduler.go       # Task scheduling
```

#### Key Interfaces
```go
type SegmentManager interface {
    CreateL0Segment(meta *streamingpb.SegmentAssignmentMeta)
    CreateL1Segment(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema)
    ObserveInsert(msg message.ImmutableInsertMessageV1)
    ObserveDelete(msg message.ImmutableDeleteMessageV1)
    FlushL0Segment(segmentID int64, timetick uint64)
    FlushL1Segment(segmentID int64, timetick uint64)
    GetDirtySnapshots() map[int64]*streamingpb.SegmentAssignmentMeta
    RecoverFromSnapshot(segments map[int64]*streamingpb.SegmentAssignmentMeta)
}
```

### Step 3: Implementation Phase

#### 3.1 Thread Safety Pattern
```go
type SegmentManager struct {
    mu         sync.RWMutex
    l0Segments map[int64]*L0Segment
    l1Segments map[int64]*L1Segment
}

func (m *SegmentManager) ObserveInsert(msg message.ImmutableInsertMessageV1) {
    m.mu.RLock()
    segment, exists := m.l1Segments[segmentID]
    m.mu.RUnlock()

    if exists {
        segment.ObserveInsert(msg) // segment has own lock
    }
}
```

#### 3.2 Chunk Management Pattern
```go
const (
    L0ChunkSize = 1 * 1024 * 1024   // 1MB for deletes
    L1ChunkSize = 16 * 1024 * 1024  // 16MB for inserts
)

func (s *L1Segment) pushMessageIntoChunk(msg message.ImmutableInsertMessageV1) {
    if s.growingChunk == nil {
        s.growingChunk = NewInsertChunk(L1ChunkSize)
    }

    if !s.growingChunk.HasSpace(msg.EstimateSize()) {
        s.sealGrowingChunk()
        s.growingChunk = NewInsertChunk(L1ChunkSize)
    }

    s.growingChunk.Push(msg)
}
```

#### 3.3 Proto Message Handling
```go
// ALWAYS use proto.Clone for proto messages
snapshot := proto.Clone(segment.meta).(*streamingpb.SegmentAssignmentMeta)

// Initialize nil fields after recovery
if meta.Stat == nil {
    meta.Stat = &streamingpb.SegmentAssignmentStat{}
}
```

### Step 4: Integration Phase

#### 4.1 Hook into Recovery Storage
```go
type recoveryStorageImpl struct {
    // ... existing fields
    segmentManager *gsegment.SegmentManager
}

func (r *recoveryStorageImpl) handleInsert(msg message.ImmutableInsertMessageV1) {
    // Update legacy system
    for _, partition := range msg.Header().GetPartitions() {
        if segment, ok := r.segments[partition.SegmentAssignment.SegmentId]; ok {
            segment.ObserveModified(msg.TimeTick(), partition.Rows, partition.BinarySize)
        }
    }

    // Also send to new system
    r.segmentManager.ObserveInsert(msg)
}
```

#### 4.2 Handle Special Operations
```go
func (r *recoveryStorageImpl) handleTruncateCollection(msg message.ImmutableTruncateCollectionMessageV2) {
    segments := extractSegmentIDs(msg)

    // Flush through legacy system
    r.flushSegments(msg, segments)

    // Also flush through new system
    if r.segmentManager != nil {
        for segmentID := range segments {
            r.segmentManager.FlushL0Segment(segmentID, msg.TimeTick())
            r.segmentManager.FlushL1Segment(segmentID, msg.TimeTick())
        }
    }
}
```

### Step 5: Testing Phase

#### 5.1 Unit Tests
```go
func TestSegmentManager(t *testing.T) {
    mockCM := mocks.NewChunkManager(t)
    sm := NewSegmentManager(mockCM)

    // Test segment creation
    meta := createTestSegmentMeta()
    sm.CreateL1Segment(meta, schema)

    // Test message observation
    msg := createMockInsertMessage()
    sm.ObserveInsert(msg)

    // Test flush
    sm.FlushL1Segment(meta.SegmentId, 1000)

    // Verify snapshots
    snapshots := sm.GetDirtySnapshots()
    assert.NotNil(t, snapshots[meta.SegmentId])
}
```

#### 5.2 E2E Testing Script
```python
def test_recovery_module():
    # Phase 1: Insert operations (L1 segments)
    collection.insert(data)
    collection.flush()

    # Phase 2: Delete operations (L0 segments)
    collection.delete(expr)
    collection.flush()

    # Phase 3: Recovery simulation
    collection.release()
    collection.load()

    # Phase 4: Verify data integrity
    results = collection.query(expr)
    assert len(results) == expected_count
```

#### 5.3 Cluster Testing
```bash
# Start Milvus with streaming
/skill/scripts/milvus_control -s start_milvus_full

# Run full test suite
/skill/scripts/milvus_test python_client

# Check logs for issues
tail -f ${VOLUME_DIR}/*/milvus-logs/*.log
```

### Step 6: Debugging Checklist

- [ ] Check proto message cloning (use proto.Clone)
- [ ] Verify mutex usage (RLock for reads)
- [ ] Confirm checkpoint updates
- [ ] Check nil field initialization
- [ ] Verify both flush paths called
- [ ] Monitor chunk accumulation
- [ ] Check persistence success
- [ ] Verify recovery restoration

### Step 7: Performance Tuning

1. **Chunk Size Optimization**
   - Monitor average message sizes
   - Adjust L0/L1 chunk thresholds
   - Balance memory vs IO frequency

2. **Worker Pool Sizing**
   ```go
   scheduler := NewSyncScheduler(
       runtime.NumCPU(),     // CPU workers
       runtime.NumCPU()/2,   // IO workers
   )
   ```

3. **Batch Operations**
   - Group multiple segments for flush
   - Batch storage API calls
   - Use bulk serialization

### Step 8: Rollout Strategy

1. **Feature Flag Control**
   ```go
   if paramtable.Get().StreamingNodeCfg.EnableNewSegmentManager.GetAsBool() {
       r.segmentManager = gsegment.NewSegmentManager(cm)
   }
   ```

2. **Parallel Validation**
   - Run both systems simultaneously
   - Compare outputs for consistency
   - Log any discrepancies

3. **Gradual Migration**
   - Start with non-critical collections
   - Monitor metrics closely
   - Roll back if issues detected

## Common Commands

```bash
# Build with changes
make build-go

# Run specific tests
go test -v ./internal/streamingnode/server/wal/recovery/gsegment/...

# Check for race conditions
go test -race ./internal/streamingnode/server/wal/recovery/...

# Generate mocks
make generate-mockery-streamingnode

# Format and lint
make fmt
make static-check
```

## Troubleshooting

### Issue: Import cycles
**Solution**: Move shared types to lower-level package

### Issue: Proto message copy fails
**Solution**: Use proto.Clone() instead of direct copy

### Issue: Segments not flushing
**Solution**: Check both legacy and new flush paths

### Issue: Recovery fails
**Solution**: Initialize nil proto fields after recovery

### Issue: Memory growth
**Solution**: Check chunk sealing thresholds

## Success Criteria

- [ ] All existing tests pass
- [ ] New unit tests >90% coverage
- [ ] E2E tests complete successfully
- [ ] No memory leaks detected
- [ ] Recovery time improved
- [ ] Backward compatibility maintained

## References

- [Milvus Architecture](https://milvus.io/docs/architecture_overview.md)
- [Go Concurrency Patterns](https://go.dev/blog/pipelines)
- [Protocol Buffers Best Practices](https://protobuf.dev/programming-guides/dos-donts/)
- [Milvus Development Guide](DEVELOPMENT.md)