# QueryViewHandler Implementation Design

> QN and SN implementations of the `QueryViewHandler` interface defined in `internal/views/nodeview/handler/handler.go`.

## 1. Overview

The `QueryViewHandler` interface is the work-node side contract for applying coord-pushed query views and reporting state changes back. QN and SN each provide independent implementations with different state machines, external dependencies, and persistence requirements.

| Aspect | QN | SN |
|---|---|---|
| SM type | `QNQueryViewStateMachine` | `SNQueryViewStateMachine` |
| External deps | `SegmentManager` (callback-based) | `StreamingNodeResourceManager` (callback-based), `StreamingNodeCatalog` |
| Async callbacks | `OnReady`, `OnUnrecoverable`, `OnDropped` | `OnReady`, `OnRecoveringDone`, `OnUnrecoverable`, `OnDropped` |
| Persistence | None | Save/Delete via `StreamingNodeCatalog` |
| Recovery | None | `RecoverSNQueryViewHandler()` from persisted Up views |

## 2. Concurrency: Shard-Granularity Locking

A work node manages multiple shards, each with multiple concurrent query views. A two-level locking scheme minimizes contention:

- **Outer `sync.Mutex`**: Protects the shard map. `ApplyViews` and `getOrCreateShard` take this lock to find or create shard entries.
- **Per-shard `sync.Mutex`**: Protects SM instances within a single shard. All SM operations (create, state transition, callback replacement, destroy) are serialized per shard.

This ensures that views on different shards can be applied concurrently while SM operations within the same shard are serialized (SMs are not thread-safe).

## 3. QN QueryViewHandler

### 3.1 Interaction with SegmentManager

`SegmentManager` is injected into `QNQueryViewHandler` via the constructor (`NewQNQueryViewHandler(segMgr)`). Each `qnShardView` holds a reference to the same `SegmentManager`. Interaction is callback-based:

1. `ApplyViews` creates SMs in Preparing state.
2. The shard view calls `segMgr.Acquire(AcquireSegments{...})` with two callbacks: `OnReady` and `OnUnrecoverable`.
3. `SegmentManager` drives segment loading asynchronously and invokes the appropriate callback.
4. `OnReady` calls `sm.OnSegmentsReady(readySegments)` under the shard lock to advance the SM (Preparing -> Ready).
5. When a view transitions to Dropping, the shard view calls `segMgr.Release(ReleaseSegments{...})` with an `OnDropped` callback that drives the SM from Dropping -> Dropped.

The `SegmentManager` interface has two methods:
- `Acquire(req AcquireSegments)`: starts segment loading; must eventually call `OnReady` (with `map[int64][]int64` of partitionID -> segmentIDs) or `OnUnrecoverable`.
- `Release(req ReleaseSegments)`: decrements ref counts; must eventually call `OnDropped`.

All callbacks must be invoked asynchronously (not during the Acquire/Release call) to avoid deadlocking the shard mutex.

### 3.2 Data Structures

- `QNQueryViewHandler`: `mu sync.Mutex`, `shards map[ShardID]*qnShardView`, `segMgr SegmentManager`
- `qnShardView`: `mu sync.Mutex`, `views map[QueryViewVersion]*qnViewEntry`, `segMgr SegmentManager`
- `qnViewEntry`: embeds `handler.ApplyView` (carries `View` and `OnReport` callback), `sm *QNQueryViewStateMachine`

### 3.3 ApplyViews Flow

1. Group views by `ShardID`.
2. For each shard, call `getOrCreateShard(shardID)` (takes outer mutex) then `shard.ApplyViews(shardViews)` (takes shard mutex).
3. Within the shard, Preparing views are applied first (to increment segment ref counts before any Dropped views decrement them), then non-Preparing views.

For each individual view (`applyOneLocked`):

**View does not exist:**
- **Preparing**: Create `QNQueryViewStateMachine`, store entry, call `segMgr.Acquire` with `OnReady`/`OnUnrecoverable` callbacks.
- **Dropped**: Report Dropped immediately via `OnReport` (e.g., QN restarted, view unknown).
- **Other**: Report Unrecoverable immediately via `OnReport` (state lost after restart).

**View already exists:**
- Replace `OnReport` callback (update `entry.ApplyView`).
- Call `sm.OnCoordStateDelivered(pushedState)`.
- Call `consumeReportAndCleanup` to drain pending report, pending release, and delete entry if Dropped.

### 3.4 Async Callbacks (Internal)

Async events are routed through internal methods on `qnShardView`, not public methods on the handler:

- `notifySegmentsReady(version, readySegments)`: called by `OnReady` callback from `SegmentManager`. Calls `sm.OnSegmentsReady(readySegments)` under shard lock.
- `notifyUnrecoverable(version)`: called by `OnUnrecoverable` callback. Calls `sm.OnUnrecoverable()` under shard lock.
- `notifyDropped(version)`: called by `OnDropped` callback from `Release`. Calls `sm.OnDropped()` under shard lock.

Each method acquires the shard mutex, finds the entry by version, calls the SM method, then calls `consumeReportAndCleanup`.

### 3.5 consumeReportAndCleanup

This method (called under shard lock) performs three actions in order:

1. `sm.ConsumeReport()` -> if non-nil and `OnReport` is set, invoke `OnReport`.
2. `sm.ConsumeRelease()` -> if true, call `segMgr.Release` with an `OnDropped` callback.
3. If `sm.State() == Dropped`, delete entry from shard's view map.

### 3.6 No Persistence, No Recovery

QN is stateless. On restart, Coord re-pushes all Preparing views.

## 4. SN QueryViewHandler

### 4.1 Dependencies

`SNQueryViewHandler` takes two injected dependencies:

- **`StreamingNodeCatalog`**: Persistence interface with a single method `SaveQueryView(view)`. Semantics: if `Meta.State == Up` -> save/overwrite; if `Meta.State` is Down/Unrecoverable/Dropped -> delete.
- **`StreamingNodeResourceManager`**: Manages streaming resources (growing segments, BM25 IDF stats, etc.). Three methods:
  - `Acquire(AcquireResource)`: callbacks `OnReady`, `OnUnrecoverable`
  - `Recover(RecoverResource)`: callbacks `OnRecoveringDone`, `OnUnrecoverable`
  - `Release(ReleaseResource)`: callback `OnDropped`

All callbacks must be asynchronous.

### 4.2 Data Structures

- `SNQueryViewHandler`: `mu sync.Mutex`, `shards map[ShardID]*snShardView`, `catalog StreamingNodeCatalog`, `resMgr StreamingNodeResourceManager`
- `snShardView`: `mu sync.Mutex`, `views map[QueryViewVersion]*snViewEntry`, `catalog StreamingNodeCatalog`, `resMgr StreamingNodeResourceManager`
- `snViewEntry`: embeds `handler.ApplyView` (carries `View` and `OnReport` callback), `sm *SNQueryViewStateMachine`

### 4.3 ApplyViews Flow

1. Group views by `ShardID`.
2. For each shard, call `getOrCreateShard(shardID)` then `shard.ApplyViews(shardViews)`.
3. Within the shard, views are applied sequentially (no Preparing-first reordering unlike QN).

For each individual view (`applyOneLocked`):

**View does not exist:**
- **Preparing**: Create `SNQueryViewStateMachine` (constructor generates a Preparing report). Store entry. Call `consumeReport` to emit the initial report. Call `resMgr.Acquire` with `OnReady`/`OnUnrecoverable` callbacks.
- **Dropped**: Report Dropped immediately via `OnReport`.
- **Other**: Report Unrecoverable immediately via `OnReport`.

**View already exists:**
- Replace `OnReport` callback (update `entry.ApplyView`).
- Call `sm.OnCoordStateDelivered(pushedState)`.
- Call `consumeReportPersistAndCleanup`.

### 4.4 Async Callbacks (Internal)

Async events are routed through internal methods on `snShardView`:

- `notifyReady(version)`: called by `OnReady` callback from `Acquire`. Calls `sm.OnReady()` under shard lock.
- `notifyRecoveringDone(version)`: called by `OnRecoveringDone` callback from `Recover`. Calls `sm.OnRecoveringDone()` under shard lock.
- `notifyUnrecoverable(version)`: called by `OnUnrecoverable` callback. Calls `sm.OnUnrecoverable()` under shard lock.
- `notifyDropped(version)`: called by `OnDropped` callback from `Release`. Calls `sm.OnDropped()` under shard lock.

Each method acquires the shard mutex, finds the entry by version, calls the SM method, then calls `consumeReportPersistAndCleanup` (or `consumeReportAndCleanup` for `notifyDropped`).

### 4.5 consumeReportPersistAndCleanup

This method (called under shard lock) performs four actions in order:

1. `consumeAndPersist`: `sm.ConsumePersist()` -> if non-nil, call `catalog.SaveQueryView(persist)`.
2. `consumeReport`: `sm.ConsumeReport()` -> if non-nil and `OnReport` is set, invoke `OnReport`.
3. `consumeAndRelease`: `sm.ConsumeRelease()` -> if true, call `resMgr.Release` with an `OnDropped` callback.
4. If `sm.State() == Dropped`, delete entry from shard's view map.

**Ordering invariant**: Persist is done BEFORE report. If SN crashes after reporting but before persisting, Coord would believe the state advanced while SN lost it.

The separate `consumeReportAndCleanup` (used by `notifyDropped`) only drains report and cleans up, skipping persist/release (not expected during Dropping -> Dropped).

### 4.6 Crash Recovery

The handler is constructed via `RecoverSNQueryViewHandler(catalog, resMgr, views)`, which accepts the list of persisted views (loaded from `catalog` by the caller).

For each persisted view (which was in Up state at crash time):

1. Create SM via `RecoverSNQueryViewStateMachine(meta, snView)` — starts in UpRecovering state.
2. Group by shard, create `snShardView` via `recoverSnShardView`.
3. During `recoverSnShardView`, under shard lock, call `resMgr.Recover(RecoverResource{...})` for each view with `OnRecoveringDone`/`OnUnrecoverable` callbacks.
4. No report is generated until `notifyRecoveringDone` is called (WAL catch-up).

Note: Recovered entries have no `OnReport` callback initially (their `ApplyView` is zero-valued). Reports only flow once Coord re-pushes the view via `ApplyViews`, which sets the `OnReport` callback.

### 4.7 Persistence Semantics

The SM's `ConsumePersist()` output determines the persistence action via `catalog.SaveQueryView`:

| `ConsumePersist` Meta.State | Action |
|---|---|
| Up | `Save` (create or overwrite recovery info) |
| Down, Unrecoverable, Dropped | `Delete` (remove recovery info) |
| nil | No action |

### 4.8 SN-Specific SM Behavior

The SN state machine has richer state flow than QN:

- **Normal**: Preparing -> Ready -> Up -> Down -> Dropping -> Dropped
- **Error**: Preparing -> Unrecoverable -> Dropping -> Dropped
- **Abort**: Preparing -> Dropping -> Dropped, Ready -> Dropping -> Dropped
- **Recovery**: UpRecovering -> Up (WAL caught up)
- **Recovery error**: UpRecovering -> Unrecoverable (no report; detected via query path)

**UpRecovering -> Unrecoverable**: The SM does NOT generate a report or persist. Coord still believes the view is Up. Discovery happens through the query path: the query planner detects the unavailable view and reports to Coord. This avoids dependence on the `OnReport` callback (which may not be set during recovery) and prevents irreversible cleanup for transient failures. Persisted Up info is retained for possible retry on SN restart.

**Coord-visible state**: UpRecovering maps to Up when building reports (Coord is unaware of UpRecovering).

## 5. File Organization

```
internal/views/nodeview/
├── handler/
│   ├── handler.go              # ApplyView, QueryViewHandler interface
│   ├── pending_reports.go      # pendingReports dedup structure
│   ├── sync_server.go          # ViewSyncServer gRPC handler
│   └── sync_server_test.go     # Unit tests
├── qnview/
│   ├── state_machine.go        # QNQueryViewStateMachine
│   ├── state_machine_test.go   # QN SM tests
│   ├── handler.go              # QNQueryViewHandler
│   ├── handler_test.go         # Unit tests
│   ├── shard_view.go           # qnShardView (per-shard SM management)
│   └── segment_manager.go      # SegmentManager interface, AcquireSegments, ReleaseSegments
└── snview/
    ├── state_machine.go        # SNQueryViewStateMachine
    ├── state_machine_test.go   # SN SM tests
    ├── handler.go              # SNQueryViewHandler + RecoverSNQueryViewHandler
    ├── handler_test.go         # Unit tests
    ├── shard_view.go           # snShardView (per-shard SM management)
    ├── resource_manager.go     # StreamingNodeResourceManager interface, AcquireResource, RecoverResource, ReleaseResource
    └── catalog.go              # StreamingNodeCatalog interface
```
