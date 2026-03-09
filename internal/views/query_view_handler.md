# QueryViewHandler Implementation Design

> QN and SN implementations of the `QueryViewHandler` interface defined in `internal/views/nodeview/handler/handler.go`.

## 1. Overview

The `QueryViewHandler` interface is the work-node side contract for applying coord-pushed query views and reporting state changes back. QN and SN each provide independent implementations with different state machines, external dependencies, and persistence requirements.

| Aspect | QN | SN |
|---|---|---|
| SM type | `QNQueryViewStateMachine` | `SNQueryViewStateMachine` |
| Async events | `OnSegmentsReady` | `OnReady`, `OnRecoveringDone`, `OnUnrecoverable` |
| Persistence | None | Save/Delete via `PersistStore` |
| Recovery | None | `Recover()` from persisted Up views |
| External deps | Sealed Segment Manager | Growing Segment Manager, WAL/Flusher |

## 2. Concurrency: Shard-Granularity Locking

A work node manages multiple shards, each with multiple concurrent query views. A two-level locking scheme minimizes contention:

- **Outer `sync.RWMutex`**: Protects the shard map. `ApplyViews` takes a read lock (shards already exist) or write lock (create new shard entry). `Close` takes a write lock.
- **Per-shard `sync.Mutex`**: Protects SM instances within a single shard. All SM operations (create, state transition, callback replacement, destroy) are serialized per shard.

This ensures that views on different shards can be applied concurrently while SM operations within the same shard are serialized (SMs are not thread-safe).

## 3. QN QueryViewHandler

### 3.1 Dependencies

```
SegmentManager interface {
    // LoadSegments triggers async loading of sealed segments.
    // Calls onReady when segments are loaded (or partially loaded).
    LoadSegments(segments []int64, onReady func(readySegments map[int64][]int64))

    // ReleaseSegments releases previously loaded sealed segments.
    ReleaseSegments(segments []int64)
}
```

### 3.2 Data Structures

- `QNQueryViewHandler`: `mu sync.RWMutex`, `shards map[ShardID]*qnShardState`, `segmentManager SegmentManager`
- `qnShardState`: `mu sync.Mutex`, `views map[QueryViewVersion]*qnViewEntry`
- `qnViewEntry`: `sm *QNQueryViewStateMachine`, `onReport func(QueryViewAtWorkNode)`

### 3.3 ApplyViews Flow

For each `ApplyView`:

1. Extract `ShardID` and `QueryViewVersion` from `View.QueryViewKey()`.
2. Find or create `qnShardState` (outer RLock; upgrade to WLock if missing).
3. Lock shard (`shardState.mu.Lock()`).
4. Look up existing `qnViewEntry` by version:
   - **Exists**: Replace `onReport` callback. Call `sm.OnCoordStateDelivered(pushedState)`. If `sm.ConsumeReport()` is non-nil, invoke new `onReport`.
   - **New + Preparing**: Create `QNQueryViewStateMachine`. Register `onReport`. Trigger `segmentManager.LoadSegments(segmentIDs, onReadyCallback)`. The `onReadyCallback` locks the shard, calls `sm.OnSegmentsReady`, consumes report, invokes `onReport`.
   - **Dropped**: Call `sm.OnCoordStateDelivered(Dropped)`. Consume report, invoke `onReport`. Remove entry from map. Call `segmentManager.ReleaseSegments`.
5. Unlock shard.

### 3.4 Auto-Destroy on Dropped

When an SM reaches Dropped (either from a Coord push or async transition through Unrecoverable → Dropped), the entry is removed from the shard's view map after the final report is emitted.

### 3.5 No Persistence, No Recovery

QN is stateless. On restart, Coord re-pushes all Preparing views.

## 4. SN QueryViewHandler

### 4.1 Dependencies

```
PersistStore interface {
    // Save persists a query view for crash recovery.
    Save(key string, view *viewpb.QueryViewOfShard) error

    // Delete removes a persisted query view.
    Delete(key string) error

    // LoadAll returns all persisted query views.
    LoadAll() ([]*viewpb.QueryViewOfShard, error)
}
```

SN also depends on external components (Growing Segment Manager, WAL/Flusher) that drive async state transitions, but these are notified through explicit methods on the handler rather than injected interfaces.

### 4.2 Data Structures

- `SNQueryViewHandler`: `mu sync.RWMutex`, `shards map[ShardID]*snShardState`, `persistStore PersistStore`
- `snShardState`: `mu sync.Mutex`, `views map[QueryViewVersion]*snViewEntry`
- `snViewEntry`: `sm *SNQueryViewStateMachine`, `onReport func(QueryViewAtWorkNode)`

### 4.3 ApplyViews Flow

Same as QN (section 3.3) with two additions:

- After `sm.OnCoordStateDelivered`, also call `sm.ConsumePersist()`. If non-nil, call `persistStore.Save` or `persistStore.Delete` based on the persisted state.
- SN SM has richer Coord push handling (Up, Down states) compared to QN.

### 4.4 Async Event Methods

These methods are called by external components (WAL, Growing Segment Manager) to notify the handler of local state changes:

```
NotifyReady(shardID ShardID, version QueryViewVersion)
NotifyRecoveringDone(shardID ShardID, version QueryViewVersion)
NotifyUnrecoverable(shardID ShardID, version QueryViewVersion)
```

Each method: find shard → lock → find SM by version → call SM method → ConsumeReport → onReport → ConsumePersist → Save/Delete.

### 4.5 Crash Recovery

```
Recover(views []*viewpb.QueryViewOfShard)
```

Called once during SN startup, before any `ApplyViews` calls, with views loaded from `PersistStore.LoadAll()`.

For each persisted view (which was in Up state at crash time):
1. Create SM via `RecoverSNQueryViewStateMachine(meta, snView)` — starts in UpRecovering state.
2. Insert into shard map.
3. No report is generated until `NotifyRecoveringDone` is called (WAL catch-up).

Recovery happens per-shard: each shard independently completes WAL catch-up and transitions from UpRecovering → Up.

### 4.6 Persistence Semantics

The SM's `ConsumePersist()` output determines the persistence action:

| `ConsumePersist` state | Action |
|---|---|
| Up | `Save` (create or overwrite recovery info) |
| Down, Unrecoverable, Dropped | `Delete` (remove recovery info) |
| nil | No action |

## 5. File Organization

```
internal/views/nodeview/
├── handler/
│   ├── handler.go              # ApplyView, QueryViewHandler interface (existing)
│   ├── pending_reports.go      # pendingReports dedup structure (existing)
│   ├── sync_server.go          # ViewSyncServer gRPC handler (existing)
│   └── sync_server_test.go     # Unit tests (existing)
├── qnview/
│   ├── state_machine.go        # QN SM (existing)
│   ├── state_machine_test.go   # QN SM tests (existing)
│   ├── handler.go              # QNQueryViewHandler (new)
│   └── handler_test.go         # Unit tests (new)
└── snview/
    ├── state_machine.go        # SN SM (existing)
    ├── state_machine_test.go   # SN SM tests (existing)
    ├── handler.go              # SNQueryViewHandler (new)
    └── handler_test.go         # Unit tests (new)
```
