# LoadScheduler Design

> QN-side load scheduler that manages sealed segment lifecycle (loading, reference counting, unloading) across multiple QueryViews.
> Core implementation behind the `SegmentManager` interface defined in [worknode/qnview/segment_manager.go](worknode/qnview/segment_manager.go).
> Counterpart to the handler-side [QueryViewHandler](query_view_handler.md) which drives the state machine.

## 1. Overview

`loadScheduler` is the QN-side component responsible for:

1. **Reference counting**: Tracking which QueryViews hold references to which segments. A segment is only unloaded when its reference count drops to zero.
2. **Async loading**: Loading segments in the background via a worker pool, reporting progress through callbacks.
3. **Load deduplication**: When multiple views request the same segment concurrently, only one load is executed. Subsequent requesters wait for the in-flight load.
4. **Async release**: Releasing segments when no view references them, reporting completion through callbacks.

### Architecture Position

```
Coord (ReliableSyncer)
        │  gRPC stream push (QueryView proto)
        ▼
ViewSyncServer
        │  ApplyViews([]ApplyView)
        ▼
QNQueryViewHandler
        │  routes by ShardID
        ▼
qnShardView
        │  Acquire / Release
        ▼
segmentManagerImpl  (thin wrapper, implements SegmentManager)
        │  delegates to
        ▼
  loadScheduler     (core: ref counting, load dedup, worker pool)
        │
        ├── calls SegmentLoadInfoProvider ──→ DataCoord/MixCoord
        ├── calls segments.Loader ─────────→ load data into memory
        └── calls segments.SegmentManager ─→ remove from memory
```

### Design Principles

- **Callback-driven**: Progress is reported through `OnReady`, `OnUnrecoverable`, and `OnDropped` callbacks. No channels, no polling.
- **Async-only callbacks**: All callbacks are invoked from background goroutines, never synchronously during `Acquire`/`Release`. This prevents deadlocking the caller's shard mutex.
- **Stateless across restarts**: No persistence. On QN restart, Coord re-pushes all Preparing views, triggering fresh Acquire calls.

## 2. Dependencies

### 2.1 SegmentLoadInfoProvider

Fetches `SegmentLoadInfo` (binlog paths, index info, delta logs) for given segment IDs. The QueryView proto only carries segment IDs; the full load metadata must be fetched separately.

```go
type SegmentLoadInfoProvider interface {
    GetSegmentLoadInfos(ctx context.Context, collectionID int64, segmentIDs []int64) ([]*querypb.SegmentLoadInfo, error)
}
```

This is the only custom interface. The implementation calls DataCoord/MixCoord APIs (`GetSegmentInfo` + `GetIndexInfo` → `PackSegmentLoadInfo`). It is defined in `qnview` because no existing interface covers this exact use case.

### 2.2 segments.Loader (existing)

Loads sealed segments into the QueryNode's memory. Defined in `internal/querynodev2/segments/segment_loader.go`.

```go
// Used method:
Load(ctx context.Context, collectionID int64, segmentType SegmentType, version int64,
     segments ...*querypb.SegmentLoadInfo) ([]Segment, error)
```

Handles segment creation, data loading, bloom filter loading, delta log loading, and registering the segment in the segment manager. Includes built-in dedup via `loadingSegments` and memory/disk resource checks.

### 2.3 segments.SegmentManager (existing)

Manages loaded segments in memory. Defined in `internal/querynodev2/segments/manager.go`.

```go
// Used method:
Remove(ctx context.Context, segmentID UniqueID, scope querypb.DataScope) (int, int)
```

Removes segments from memory and decrements collection reference counts.

## 3. Interface

The `SegmentManager` interface (already defined in `segment_manager.go`):

```go
type SegmentManager interface {
    Acquire(req AcquireSegments)
    Release(req ReleaseSegments)
}
```

### AcquireSegments

```go
type AcquireSegments struct {
    Key          qviews.QueryViewKey
    CollectionID int64
    SegmentIDs  map[int64][]int64       // partitionID → segmentIDs
    Settings    *viewpb.QueryViewSettings
    OnReady     func(readySegments map[int64][]int64)  // incremental, may be called multiple times
    OnUnrecoverable func()                              // at most once, terminal
}
```

- `SegmentIDs` groups segment IDs by partition. The partition association is needed for `OnReady` reporting (the state machine tracks readiness per partition).
- `CollectionID` is required for loading (not derivable from `QueryViewKey`).

### ReleaseSegments

```go
type ReleaseSegments struct {
    Key        qviews.QueryViewKey
    SegmentIDs map[int64][]int64  // partitionID → segmentIDs
    OnDropped  func()             // exactly once
}
```

`SegmentIDs` mirrors the structure of `AcquireSegments.SegmentIDs`. The caller passes the same partition→segment mapping it used during Acquire.

## 4. Internal Design

### segmentManagerImpl (thin wrapper)

```go
type segmentManagerImpl struct {
    loadScheduler *loadScheduler
}

func (m *segmentManagerImpl) Acquire(req AcquireSegments) {
    m.loadScheduler.Acquire(req)
}

func (m *segmentManagerImpl) Release(req ReleaseSegments) {
    m.loadScheduler.Release(req)
}
```

### loadScheduler (core)

```go
type loadScheduler struct {
    mu sync.Mutex

    // Per-segment reference counting: segmentID → set of view keys.
    segmentRefs map[int64]map[QueryViewKey]struct{}

    // In-flight load deduplication: segmentID → loadTask.
    loadingTasks map[int64]*loadTask

    // Dependencies
    infoProvider SegmentLoadInfoProvider
    loader       segments.Loader
    segManager   segments.SegmentManager

    // Concurrency
    pool *conc.Pool[struct{}]
    wg   sync.WaitGroup
}

type loadTask struct {
    done chan struct{}  // closed when load completes (success or failure)
    err  error         // nil on success
}
```

### Segment Categorization

When `Acquire` is called, each segment falls into one of three categories (determined under `mu`):

| Category | Condition | Action |
|---|---|---|
| **toLoad** | First reference (ref 0→1), no loadTask | Create loadTask, submit to pool |
| **toWait** | Has existing loadTask (another view loading it) | Wait on `task.done` |
| **readyNow** | ref > 1, no loadTask (already loaded) | Report immediately |

### Reference Counting Rules

- **Acquire**: `segmentRefs[segID][viewKey] = struct{}{}` — one entry per (segment, view) pair.
- **Release**: For each segment in `req.SegmentIDs`, remove `viewKey` from `segmentRefs[segID]`. If the set becomes empty, the Release goroutine unloads the segment (immediately if loaded, or after waiting for in-flight load to complete).
- **Load failure**: Remove ALL view keys from `segmentRefs[segID]` (not just the requester). See [Section 7](#7-failure-handling) for rationale.

## 5. Acquire Flow

```
Acquire(req)
  ├─ under mu: record refs, categorize each segment
  │            → readyNow / toLoad (create loadTask) / toWait
  │
  └─ goroutine:
      ├─ readyNow → OnReady
      ├─ toLoad   → fetch LoadInfo → pool.Submit per segment
      │              success → close(task.done), OnReady
      │              failure → cleanup all refs, close(task.done)
      ├─ toWait   → <-task.done per segment
      │              success → OnReady
      │              failure → OnUnrecoverable
      └─ any failure at any phase → OnUnrecoverable
```

## 6. Release Flow

```
Release(req)
  ├─ under mu: for each segment in req.SegmentIDs, decrement refs
  │            ref=0 → toRelease (loaded) / toWaitThenRelease (has loadTask)
  │
  └─ goroutine:
      ├─ toRelease         → segManager.Remove
      ├─ toWaitThenRelease → <-task.done, then Remove if loaded
      └─ OnDropped
```

`OnDropped` is called only after all segments are truly cleaned up — including those that were still loading at Release time.

## 7. Failure Handling

### Load Failure: Full Ref Cleanup

When a segment load fails, **all** references to that segment are removed — not just the requester's.

Full cleanup ensures:
1. All views referencing the failed segment receive `OnUnrecoverable` (the requester directly, waiters via `task.err`).
2. Coord generates replacement views for all affected views.
3. Replacement views' Acquire calls see `ref == 0` → categorized as `toLoad` → fresh load attempt.

### InfoProvider Failure

If `GetSegmentLoadInfos` fails, all `toLoad` segments are cleaned up, and `OnUnrecoverable` is called.

### loadTask Lifecycle

| Event | loadTask state |
|---|---|
| Created (in Acquire, under mu) | `done` open, `err` nil |
| Load success | `err` stays nil, `done` closed |
| Load failure | `err` set, segment refs cleaned, `done` closed |

Waiters read `task.err` after `<-task.done` returns. No race: `err` is written before `close(done)`.

## 8. Concurrency Safety

### Lock Scope

Single `sync.Mutex` in `loadScheduler` protects `segmentRefs`, and `loadingTasks`. All three are read/written atomically within the same critical section during Acquire's categorization phase.

### Load/Release Ordering

- **Same view**: Acquire and Release are in separate `ApplyViews` calls (Coord pushes Preparing first, Dropped later). Ordering is natural — no special handling needed.
- **Cross-view (double buffer)**: A single `ApplyViews` batch may contain v2 Preparing + v1 Dropped. `qnShardView` processes all Preparing views before Dropped views, ensuring shared segments' ref counts are incremented (by v2 Acquire) before being decremented (by v1 Release). Without this ordering, the ref could momentarily hit zero, triggering an unnecessary unload-then-reload.
- **Cross shard**: Segments are shard-scoped in practice. Cross-shard sharing is theoretically possible but does not require special handling — the reference counting is correct regardless.

### Release During Loading

If `Release` is called while a segment is still loading (ref drops to zero, but `loadingTasks[segID]` exists):
- `Release` does NOT call `segManager.Remove` immediately (the segment is not in memory yet).
- Instead, the Release goroutine waits on `<-task.done` for the load to complete.
- On load success: `segManager.Remove` is called, then `OnDropped`.
- On load failure: no Remove needed (segment was never loaded), then `OnDropped`.
- The pool worker does not need to check ref counts — cleanup is fully owned by the Release path.

### Graceful Shutdown

`Close()` calls `wg.Wait()` to drain all in-flight goroutines, then `pool.Release()` to shut down the worker pool.

## 9. Package Location

```
internal/views/worknode/qnview/
    segment_manager.go           // SegmentManager interface, AcquireSegments, ReleaseSegments (existing)
    segment_manager_impl.go      // segmentManagerImpl thin wrapper (new)
    load_scheduler.go            // loadScheduler core: ref counting, load dedup, pool (new)
    load_scheduler_test.go       // loadScheduler tests (new)
    handler.go                   // QNQueryViewHandler (existing)
    shard_view.go                // qnShardView (existing, minor update: pass CollectionID + SegmentIDs)
    state_machine.go             // QNQueryViewStateMachine (existing, unchanged)
```
