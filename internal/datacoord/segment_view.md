# SegmentView Module Design

Date: 2026-03-25

## Overview

Introduce a `segmentViewMeta` module with two aspects:

1. **DataView**: Versioned queryable segment views that govern the segment lifecycle across flush, compaction, and GC.
2. **Metadata consolidation**: Merge segment metadata management and DataView into a single struct with per-collection RWMutex locking and unified write interfaces.

---

## Part 1: DataView Design

### What is a DataView

A DataView represents the **queryable segment view** of a collection. A segment is added to the DataView only when it is ready to serve queries:

- **Flush**: StreamingNode is expected to complete sorting before flush, so flushed segments are immediately queryable. Flush and DataView update happen in a single atomic operation.
- **Compaction**: Output segments require index building before they can serve queries efficiently. This is a two-step process — persist segment metadata first, then update the DataView after indexes are ready.

Each DataView version is identified by a two-dimensional version number `(streaming_version, compact_version)`. Flush increments `streaming_version`; compaction increments `compact_version`. Each version records the segment distribution of the collection, organized by shard and partition.

### Flush: Single-Step

StreamingNode completes sorting during flush (`IsSorted=true`), so the flushed segment is immediately queryable. Segment update and DataView update are persisted in a single atomic ETCD batch write.

```
FlushSegment(ctx, collectionID, segmentID, vchannel, partitionID, operators...)
  ┌─ collLock.Lock(collectionID)
  │
  │  1. Prepare segment changes (operator pattern)
  │  2. Compute new DataView:
  │     streaming_version = currentVersion.streaming_version + 1
  │     compact_version   = currentVersion.compact_version (unchanged)
  │     Clone current view, add new segment
  │  3. catalog.AlterSegmentsAndSaveDataView()   ← single ETCD batch write
  │  4. Update segment in memory (SetSegment)
  │  5. Update DataView in memory (addDataViewForFlush)
  │
  └─ collLock.Unlock(collectionID)
```

### Compaction: Two-Step

Compaction output segments need indexes for efficient queries. Writing unindexed segments into the DataView while dropping the old segments would degrade query performance, so the process is split into two steps.

#### Step 1: CompleteCompactionMutation — Persist Segment Metadata

After compaction execution completes, persist compactTo segments immediately, but **do not drop compactFrom and do not update the DataView**.

```
CompleteCompactionMutation(ctx, task, result)
  ┌─ collLock.Lock(collectionID)
  │
  │  1. Validate input segments are healthy
  │  2. Create compactTo segments (Flushed state)
  │  3. Mark compactFrom: State=Dropped, Compacted=true
  │  4. catalog.AlterSegments()   ← segment meta only, DataView untouched
  │  5. Update segment in memory
  │  6. Do not update DataView
  │
  └─ collLock.Unlock(collectionID)
```

State at this point:
- compactFrom: `State=Dropped, Compacted=true, isCompacting=true`. **Still in DataView** — QueryNode continues to use it. `Dropped` is a metadata state; queryability is controlled by DataView. GC will not reclaim it because GC requires the segment to not be referenced by any active DataView version.
- compactTo: `State=Flushed, isCompacting=true`. Not in DataView, waiting for index.

`isCompacting=true` is held until handoff, preventing both compactFrom and compactTo from being selected for further compaction.

After completion, send `buildIndexCh` notifications for all compactTo segments to accelerate index task creation:

```go
for _, segID := range compactToSegIDs {
    select {
    case getBuildIndexChSingleton() <- segID:
    default:
    }
}
```

#### Step 2: CompleteCompactionHandoff — Update DataView After Index Ready

Once all compactTo segments have finished index building, atomically switch the DataView.

```
CompleteCompactionHandoff(ctx, collectionID, compactFromIDs, compactToIDs, vchannel, partitionID)
  ┌─ collLock.Lock(collectionID)
  │
  │  1. Build new DataView:
  │     streaming_version = unchanged
  │     compact_version   = currentVersion.compact_version + 1
  │     Clone current view, remove compactFrom segments, add compactTo segments
  │  2. catalog.SaveDataView()   ← write DataView only
  │  3. Update DataView in memory (addDataViewForCompaction)
  │  4. resetSegmentCompacting (release isCompacting)
  │
  └─ collLock.Unlock(collectionID)
```

### Compaction Task State Machine

All compaction types use a unified state machine:

```
pipelining → executing → meta_saved → indexing → completed → cleaned
```

State transitions and their relationship with DataView:

```
                        globalTaskScheduler                    compactionInspector
                     ┌─────────────────────────┐     ┌───────────────────────────────────┐
                     │  CreateTaskOnWorker      │     │  Process() → retryableProcess()   │
                     │  QueryTaskOnWorker       │     │                                   │
                     └─────────────────────────┘     └───────────────────────────────────┘
                              │    │                         │    │    │
                              ▼    ▼                         ▼    ▼    ▼
┌───────────┐ dispatch ┌───────────┐  query     ┌───────────┐     ┌───────────┐     ┌───────────┐
│           │    to    │           │  result,   │           │     │           │     │           │
│ pipelining├─────────►│ executing ├──────────►│meta_saved ├────►│  indexing ├────►│ completed │
│           │ DataNode │           │  write seg │           │     │           │     │           │
└───────────┘          └───────────┘   meta    └───────────┘     └───────────┘     └─────┬─────┘
                                                    │                │                  │
                                                    ▼                ▼                  ▼
                                           CompleteCompaction   Poll index          CompleteCompaction
                                              Mutation          status              Handoff
                                                    │                │              + completeTask()
                                                    ▼                ▼                  │
                                             ┌─────────────┐  ┌──────────────┐         ▼
                                             │ compactTo:   │  │ All result   │   ┌───────────┐
                                             │  Flushed     │  │ segments     │   │           │
                                             │ compactFrom: │  │ indexed?     │   │  cleaned  │
                                             │  Dropped     │  │              │   │   (GC)    │
                                             │ DataView:    │  │  No → wait   │   └───────────┘
                                             │  unchanged   │  │  Yes→complete│
                                             └─────────────┘  └──────────────┘

Segment and DataView state at each phase:

┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  pipelining / executing                                                      │
│  ├─ compactFrom: Flushed, isCompacting=true                                 │
│  ├─ compactTo:   does not exist                                             │
│  └─ DataView:    contains compactFrom                                       │
│                                                                              │
│  meta_saved / indexing                                                        │
│  ├─ compactFrom: Dropped, Compacted=true, isCompacting=true                  │
│  │               still in DataView, QueryNode continues to use               │
│  ├─ compactTo:   Flushed, isCompacting=true, waiting for index               │
│  │               not in DataView                                             │
│  └─ DataView:    still contains compactFrom (unchanged)                      │
│                                                                              │
│  completed (after handoff)                                                    │
│  ├─ compactFrom: Dropped                                                     │
│  │               removed from DataView, eligible for GC                      │
│  ├─ compactTo:   Flushed, indexed                                            │
│  │               added to DataView                                           │
│  └─ DataView:    compact_version+1, contains compactTo                       │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

Driver for each phase:

| Phase | Framework | Description |
|-------|-----------|-------------|
| pipelining → executing | `globalTaskScheduler` | `CreateTaskOnWorker` dispatches to DataNode |
| executing → meta_saved | `globalTaskScheduler` | `QueryTaskOnWorker` retrieves result, calls `CompleteCompactionMutation` |
| meta_saved → indexing | `compactionInspector` | `Process()` → `retryableProcess()` |
| indexing → completed | `compactionInspector` | `processIndexing()` polls index status, calls `completeTask()` when ready |
| completed → cleaned | `compactionInspector` | GC cleanup |

### Mix Compaction State Machine Extension

`mixCompactionTask` needs an `indexing` phase. Since sort compaction reuses `mixCompactionTask`, behavior must differ by compaction type:

**`processMetaSaved`** routes based on output segment state:

- **Sort compaction**: Output is always sorted. Index is handled by the upstream cluster compaction or indexInspector. Proceed directly to `completed`.
- **Mix compaction, mergeSortMultipleSegments path**: Output has `IsSorted=true`. Proceed to `indexing` and wait for index completion.
- **Mix compaction, mergeSplit path**: Output has `IsSorted=false`. Trigger sort compaction by sending compactTo segment IDs to `statsTaskCh`, then enter `statistic` phase to wait for sort completion before proceeding to `indexing`.

**`processStatistic`** (new for mix compaction): Similar to cluster compaction's `processStats`. Uses `GetCompactionTo` to check whether each compactTo segment has been processed by sort compaction. Once all are done, updates `ResultSegments` on the task with the sort output segment IDs and transitions to `indexing`.

**`processIndexing`**: Polls index status for all ResultSegments. When all are `IndexState_Finished`, calls `completeTask`, which invokes `CompleteCompactionHandoff` to perform step 2.

### IsInvisible Retirement

Under the new architecture, DataView fully replaces `IsInvisible` for query visibility control:

- Not in DataView = not queryable (equivalent to `IsInvisible=true`)
- compactTo segments are protected from re-selection by `isCompacting=true` (replaces `IsInvisible` filtering in `getCandidates`)
- `GetRecoveryInfo` reads directly from DataView segment lists (replaces `IsInvisible` checks in `GetQueryVChanPositions`)
- Cluster compaction tmpSegments are triggered for sort compaction through the dedicated `statsTaskCh` channel (bypasses `getCandidates` entirely)

`IsInvisible` must be retained during the transition period while `retrieveSegment` is still in use. It can be removed after DataView fully takes over query view management.

### Crash Recovery

No additional recovery mechanism is needed — the compaction task itself is persisted in ETCD:

| Crash Timing | Task State | Recovery Behavior |
|--------------|------------|-------------------|
| During compaction execution | `executing` | Re-dispatch to DataNode |
| Segment meta written, index not complete | `indexing` | Continue polling index status |
| Index complete, handoff in progress | `indexing` | Re-execute handoff (idempotent) |
| Handoff complete | `completed` | Normal cleanup |

**Idempotency guarantee**:
- `SaveDataView`: Writing the same DataView version is idempotent.
- `completeTask` sets `setState(completed)` as the last step; preceding steps can be safely retried.

**DataView is never in an intermediate state** — either it has not been updated yet (still using compactFrom), or it has been fully updated (using compactTo).

### GC and DataView

GC conditions for a Dropped segment:

1. `dropTolerance` expired
2. **Not referenced by any active DataView version**
3. Not loaded by any QueryNode (`loadedSegments` check)
4. Not referenced by any snapshot

---

## Part 2: Metadata Consolidation

### Motivation

Segment metadata (`SegmentsInfo`) and DataView changes must be coordinated: both flush and compaction handoff modify segment state and DataView simultaneously. Merging them into a single struct with a shared lock eliminates dual-lock coordination and reduces lock granularity to the collection level.

### Data Structures

`segmentViewMeta` merges `SegmentsInfo` and `dataViewMeta` into a unified manager:

```go
type segmentViewMeta struct {
    ctx      context.Context
    catalog  metastore.DataCoordCatalog
    collLock *lock.KeyLock[int64]  // per-collection RWMutex

    // Segments
    segments         map[UniqueID]*SegmentInfo
    coll2Segments    map[UniqueID]map[UniqueID]*SegmentInfo   // collectionID → segmentID → info
    channel2Segments map[string]map[UniqueID]*SegmentInfo     // vchannel → segmentID → info
    compactionTo     map[UniqueID][]UniqueID                  // fromSegID → toSegIDs

    // DataViews
    dataViews map[int64]*collectionDataViews  // collectionID → views
}

type collectionDataViews struct {
    views          map[dataViewVersionKey]*CollectionDataView
    currentVersion *viewpb.DataVersion
    versionList    []*viewpb.DataVersion // sorted ascending
}
```

#### DataView In-Memory Representation

The proto `DataViewOfCollection` uses repeated nested messages, which is suitable for serialization but not for lookups. In memory, these are converted to maps for O(1) reads and writes:

```go
// CollectionDataView is the in-memory representation of DataViewOfCollection,
// using maps instead of repeated nesting for fast segment add/remove/lookup.
type CollectionDataView struct {
    collectionID int64
    version      *viewpb.DataVersion
    shards       map[string]*ShardDataView // vchannel → shard
}

// ShardDataView is the in-memory representation of DataViewOfShard.
type ShardDataView struct {
    deleteApplyStartAfterTimetick uint64
    partitions                    map[int64]map[int64]struct{} // partitionID → segmentID set
}
```

- On load: `fromProto(*viewpb.DataViewOfCollection)` converts proto to `CollectionDataView`
- On persist: `toProto()` converts back to `*viewpb.DataViewOfCollection` for serialization

### Locking Model

`collLock` is a per-collection RWMutex (`lock.KeyLock[int64]`). Different collections are fully parallel:

- **Write path**: `collLock.Lock(collectionID)` → catalog I/O → update memory → unlock
- **Read path**: `collLock.RLock(collectionID)` → read memory → RUnlock

### Write Method Consolidation

The 9 segment write methods on the old `meta` are unified into these entry points:

| Old Method | Disposition |
|------------|-------------|
| `UpdateSegmentsInfo(ctx, operators...)` | Merged into `UpdateSegments(ctx, collectionID, operators...)` with added collectionID parameter |
| `SetState(ctx, segmentID, state)` | Retained as a wrapper over `UpdateSegments` |
| `SetStartPosition` / `SetLastExpire` / `SetLastFlushTime` / `SetLastWrittenTime` / `SetSegmentLevel` / `SetSegmentCompacting` | Confirmed unused or mergeable into `UpdateSegments` operator calls; standalone methods removed |
| `AddSegment(ctx, segment)` | Retained, with collectionID locking added |
| `DropSegment(ctx, collectionID, segmentID)` | Retained, with collectionID parameter and locking added |

`UpdateSegments` is the single general-purpose update entry point using the operator pattern. `updateSegmentPack` includes a collectionID field with cross-collection validation in `Get()`.

### Unused Method Cleanup

The following public methods on `SegmentsInfo` are never called and will not be carried over:

- Query methods: `HasSegments`, `GetSegmentsByChannel`, `GetSegmentsIDOfCollection`, `GetSegmentsIDOfCollectionWithDropped`, `GetSegmentsIDOfPartition`, `GetSegmentsIDOfPartitionWithDropped`, `GetSegmentsTotalNumRows`, `GetSegmentsChannels`
- Mutation methods: `SetLastExpire`, `SetLastFlushTime`, `SetLastWrittenTime`, `SetSegmentCompacting` (single-segment version), `SetSegmentLevel`

### Cross-Collection Operations

Batch operations spanning multiple collections (e.g., `BatchUpdateManifestV2`) are grouped by collectionID and processed per-group:

```go
groups := groupByCollection(items)
for collID, ops := range groups {
    m.segmentViewMeta.UpdateSegments(ctx, collID, ops...)
}
```

### meta Struct Integration

```go
type meta struct {
    ctx     context.Context
    catalog metastore.DataCoordCatalog

    collections     *typeutil.ConcurrentMap[UniqueID, *collectionInfo]
    segmentViewMeta *segmentViewMeta

    channelCPs   *channelCPs
    chunkManager storage.ChunkManager
    indexMeta    *indexMeta
    analyzeMeta  *analyzeMeta
    // ...
}
```

Removed: `segMu`, `segments *SegmentsInfo`.

meta.go retains thin wrappers for external call compatibility:

```go
func (m *meta) GetSegment(ctx context.Context, segID UniqueID) *SegmentInfo {
    return m.segmentViewMeta.GetSegment(segID)
}

func (m *meta) SetState(ctx context.Context, segmentID UniqueID, state commonpb.SegmentState) error {
    seg := m.segmentViewMeta.GetSegment(segmentID)
    if seg == nil { ... }
    return m.segmentViewMeta.UpdateSegments(ctx, seg.GetCollectionID(), UpdateStatusOperator(segmentID, state))
}
```

### CompactionMeta Interface

```go
type CompactionMeta interface {
    CompleteCompactionMutation(ctx, task, result) ([]*SegmentInfo, *segMetricMutation, error)
    CompleteCompactionHandoff(ctx, collectionID, compactFromIDs, compactToIDs, vchannel, partitionID) error
    ValidateSegmentStateBeforeCompleteCompactionMutation(task) error
    UpdateSegments(ctx, collectionID, operators...) error
    // ... other segment read methods
}
```

Removed: `GetDataViewMeta() *dataViewMeta`. Required methods are exposed directly.

### Method Reference

#### Write Methods (hold collLock WLock)

| Method | Description |
|--------|-------------|
| `AddSegment(ctx, segment)` | Persist + write to memory + maintain secondary indexes |
| `DropSegment(ctx, collectionID, segmentID)` | Persist deletion + clean up memory |
| `UpdateSegments(ctx, collectionID, operators...)` | Unified segment update entry point, operator pattern |
| `FlushSegment(ctx, collectionID, segmentID, vchannel, partitionID, operators...)` | Segment + DataView single-step atomic write |
| `CompleteCompactionMutation(ctx, task, result)` | Step 1: write compactTo segment meta only |
| `CompleteCompactionHandoff(ctx, collectionID, compactFromIDs, compactToIDs, vchannel, partitionID)` | Step 2: write DataView |
| `DropDataView(ctx, collectionID, version)` | Remove a specific DataView version |
| `DropDataViewsByCollection(ctx, collectionID)` | Remove all DataViews for a collection |

#### Read Methods (hold collLock RLock)

| Method | Description |
|--------|-------------|
| `GetSegment(segmentID)` | Lookup by ID |
| `GetSegments()` | Return all segments |
| `GetSegmentsBySelector(filters...)` | Filter-based query |
| `GetRealSegmentsForChannel(channel)` | By channel, excluding fake segments |
| `GetCompactionTo(segmentID)` | Lookup compaction mapping |
| `GetCurrentVersion(collectionID)` | Latest DataView version (deep clone) |
| `GetDataView(collectionID, version)` | Specific DataView version (deep clone) |
| `ListDataViews(collectionID)` | All versions in ascending order (deep clone) |

### KV Storage

DataView ETCD path:

```
datacoord-meta/dataview/{collectionID}/{streamingVersion}/{compactVersion}
  -> DataViewOfCollection (proto bytes)
```

Catalog interface:

```go
SaveDataView(ctx, collectionID, view) error
ListDataViews(ctx) (map[int64][]*viewpb.DataViewOfCollection, error)
DropDataView(ctx, collectionID, version) error
DropDataViewsByCollection(ctx, collectionID) error
AlterSegmentsAndSaveDataView(ctx, segments, collectionID, view, binlogs...) error
```

`AlterSegmentsAndSaveDataView` combines segment KVs and DataView KVs into a single `SaveByBatch` call for atomicity. When `view` is nil, it degrades to a plain `AlterSegments`.
