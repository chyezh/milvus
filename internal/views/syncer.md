# Syncer Design

> A reliable message delivery layer over unreliable gRPC bidirectional streams.
> Package: [`coord/coordview/syncer/`](coord/coordview/syncer/), Proto: [view.proto](../../pkg/proto/view.proto)

## 1. Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                     Coord Manager (Caller)                       │
│         │ SyncViews(group)          ▲ OnSyncResponse / OnNodeLost│
│         ▼                           │                            │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                     ReliableSyncer                          │ │
│  │  • Watches node changes via ViewSyncClient                 │ │
│  │  • Lazily creates resumableSyncers per node                │ │
│  │  • Drains removed nodes (service discovery)                │ │
│  │                                                             │ │
│  │  ┌──────────────────┐ ┌──────────────────┐                 │ │
│  │  │ resumableSyncer  │ │ resumableSyncer  │ ...             │ │
│  │  │ (per work node)  │ │ (per work node)  │                 │ │
│  │  │ • pendingSync    │ │ • pendingSync    │                 │ │
│  │  │ • backoff retry  │ │ • backoff retry  │                 │ │
│  │  │ • send/recv loop │ │ • send/recv loop │                 │ │
│  │  └──────────────────┘ └──────────────────┘                 │ │
│  └─────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────┘
              │                     ▲
         gRPC Stream           gRPC Stream
              ▼                     │
         Work Node (SN/QN)
```

**ReliableSyncer**: Top-level component that:
1. Lazily creates per-node `resumableSyncer` instances on first `SyncViews` call (via `getOrCreateSyncer`).
2. Watches node changes via `ViewSyncClient` and drains `resumableSyncer` instances for removed nodes.
3. Detects node loss via service discovery (not reconnect timeout).

**resumableSyncer**: Per-node component that owns a `pendingSyncQueryViews` instance. Runs a single `loop()` goroutine that creates a stream, re-pushes all pending views, and runs send/recv loops in parallel. On stream break, reconnects with exponential backoff.

## 2. Interfaces

Defined in [`coord/coordview/syncer/reliable_syncer.go`](coord/coordview/syncer/reliable_syncer.go).

### SyncView

Pairs a query view with its callbacks:

- **View**: The `QueryViewAtWorkNode` to push. Target node determined by `View.WorkNode()`.
- **OnSyncResponse**: Invoked when the node sends a real response. Returns `true` to remove the entry from pending, `false` to keep monitoring.
- **OnNodeLost**: Invoked when the node is declared lost via service discovery. Pure notification; the entry is removed after calling this.

### SyncGroup

Pre-grouped batch: `ViewsByNode map[WorkNodeKey][]SyncView`.

### ReliableSyncer

- **SyncViews(ctx, group)**: Enqueues views for delivery. Non-blocking.
- **Close()**: Gracefully closes all streams. Must only be called during Coordinator shutdown.

### ViewSyncClient

Provides service discovery and gRPC stream creation for all work node types. Internally routes to the appropriate backend based on `NodeType`.

- **WatchNodeChanged(ctx)**: Returns a channel signaling node membership changes across all node types.
- **GetAllNodes(ctx)**: Returns all currently known nodes.
- **IsNodeAlive(ctx, node)**: Local cache lookup checking if a node is alive.
- **OpenSyncStream(ctx, node)**: Opens a `SyncQueryView` bidirectional stream.
- **Close()**: Releases resources.

## 3. Per-Node Pending Views

Each `resumableSyncer` owns a `pendingSyncQueryViews` instance that tracks views dispatched to its node.

```
pendingSyncQueryViews
├── mu sync.Mutex
├── entries map[QueryViewKey]SyncView   // pending entries awaiting response
├── unsent  []*QueryViewOfShard         // protos accumulated by Upsert, drained by sendLoop
└── notify  chan struct{} (cap 1)       // signaled by Upsert
```

### Operations

| Method | Description |
|---|---|
| `Upsert(sv)` | Insert/replace entry, append proto to `unsent`, signal `notify`. |
| `Ready()` | Returns the `notify` channel for `sendLoop` to select on. |
| `DrainUnsent()` | Atomically drain and return `unsent` protos. |
| `MatchResponse(pb)` | Match response to entry. If `OnSyncResponse` returns true, delete entry. Holds lock during callback to prevent stale deletion by concurrent Upsert. |
| `Drain()` | Remove all entries, invoke `OnNodeLost()` for each. Called on node loss. |
| `CollectProtos()` | Return protos for all entries. Used for re-push on reconnection. |

### Concurrency

- `Upsert` is called from `SyncViews` (caller goroutine).
- `MatchResponse` is called from `recvLoop` (per-node goroutine).
- `MatchResponse` holds the lock while calling `OnSyncResponse` to prevent a concurrent `Upsert` from replacing the entry between read and delete.

## 4. resumableSyncer

Per-node component that maintains a gRPC bidirectional stream.

### Lifecycle

```
loop() goroutine:
    for ctx not cancelled:
        stream = OpenSyncStream()       // backoff on failure
        rePush(stream)                  // DrainUnsent + CollectProtos → sendBatched
        if rePush fails → continue      // skip to reconnect

        start sendLoop goroutine        // Ready() → DrainUnsent → sendBatched
        recvLoop (current goroutine)    // Recv → MatchResponse

        stream broke → cancel sendLoop → wait → backoff → retry
```

### Batched Sending

`sendBatched` sends protos in batches of 16 (`sendBatchSize`). Used by both `sendLoop` (incremental) and `rePush` (full re-push on reconnection).

On reconnection, `rePush` clears stale `unsent` protos before collecting from `entries` to avoid duplicate sends.

### Close and Drain

- **Close()**: Cancels context, waits for `loop()` goroutine to exit. Does NOT drain pending views.
- **DrainPendingIfNodeLost()**: Must only be called after `Close()`, when the node is declared lost. Invokes `OnNodeLost()` for each remaining pending entry.

## 5. ReliableSyncer Implementation

### SyncViews Flow

```
SyncViews(ctx, group):
    for each (nodeKey, views) in group.ViewsByNode:
        rs, closed = getOrCreateSyncer(ctx, nodeKey, views)
        if closed → return ErrSyncerClosed
        if rs != nil → rs.Sync(views)
        else → OnNodeLost() for each view   // node not found
```

`getOrCreateSyncer` acquires the lock once and:
1. Returns existing `resumableSyncer` if found.
2. Otherwise calls `IsNodeAlive` (local cache lookup, safe under lock).
3. If alive, creates a new `resumableSyncer` and inserts it into the map.
4. If not alive, returns nil (views will be drained by caller).

### Node Loss Detection

Node loss is determined by **service discovery**, not by reconnect timeout.

A single background `watchNodes` goroutine watches for node membership changes via `ViewSyncClient.WatchNodeChanged`. On each change, `drainRemovedNodes` is called:

```
drainRemovedNodes():
    nodes = GetAllNodes()
    lock:
        for each syncer not in nodes:
            remove from map
    for each removed syncer:
        Close()
        DrainPendingIfNodeLost()   // OnNodeLost() for each pending entry
```

Key design decisions:
- **Lazy creation**: `resumableSyncer` instances are created on demand by `getOrCreateSyncer`, NOT by the background watcher. The watcher only handles removals.
- **No reconnect timeout**: `resumableSyncer` retries indefinitely with backoff until closed.
- **Separation of concerns**: Stream reconnection (resumableSyncer) is independent of node liveness (service discovery).

### Concurrency

- `getOrCreateSyncer` holds `s.mu` while calling `IsNodeAlive` (local cache lookup). This ensures mutual exclusion with `drainRemovedNodes`, preventing a race where a syncer is created for a node that was just removed.
- `rs.Sync(views)` is called after releasing the lock. If `drainRemovedNodes` closes the syncer concurrently, views added after drain are lost. This is acceptable because the upper-layer state machine will retry.

### Close

```
Close():
    set closed = true
    cancel context
    wait for watchNodes goroutine
    close all remaining resumableSyncers (no drain — graceful shutdown)
```

Must only be called during Coordinator shutdown. After Close, the ReliableSyncer cannot be reused — a new instance must be created via Coordinator recovery.

## 6. Key Scenarios

### 6.1 Normal Flow

```
Caller                   ReliableSyncer              Node
  │─SyncViews(group)──────►│                           │
  │                         │─pending[key]=sv           │
  │                         │─send(view)───────────────►│
  │                         │◄──recv(resp)──────────────│
  │                         │─OnSyncResponse(resp)→true │
  │  callback invoked       │─delete pending[key]       │
```

### 6.2 Stream Break + Reconnection

```
Caller                   ReliableSyncer              Node
  │─SyncViews(group)──────►│─pending[key]=sv           │
  │                         │─send(view)───────────────►│
  │                         │        ╳ stream breaks     │
  │                         │─backoff retry..            │
  │                         │─new stream opened──────────│
  │                         │─rePush: re-send pending──►│
  │                         │◄──recv(resp)──────────────│
  │                         │─OnSyncResponse(resp)→true │
  │  callback invoked       │─delete pending[key]       │
```

### 6.3 Node Lost (Service Discovery)

```
Caller                   ReliableSyncer              Node
  │─SyncViews(group)──────►│─pending[key]=sv           │
  │                         │─send(view)───────────────►│
  │                         │                            ╳ node crashes
  │                         │◄─service discovery: node removed
  │                         │─Close resumableSyncer
  │                         │─DrainPendingIfNodeLost
  │  OnNodeLost() invoked   │─delete pending[key]
```

### 6.4 OnSyncResponse Returns False (Continue Monitoring)

```
Caller                   ReliableSyncer              Node
  │─SyncViews(group)──────►│─pending[key]=sv           │
  │                         │─send(view)───────────────►│
  │                         │◄──recv(resp1)─────────────│
  │                         │─OnSyncResponse(resp1)→false
  │  (continue)             │◄──recv(resp2)─────────────│
  │                         │─OnSyncResponse(resp2)→true│
  │  (done)                 │─delete pending[key]       │
```

### 6.5 Entry Replacement

```
Caller                   ReliableSyncer
  │─SyncViews({v1,cb1})───►│─pending[key]={v1,cb1}
  │                         │─send(v1)──►
  │─SyncViews({v2,cb2})───►│─pending[key]={v2,cb2}
  │                         │  (cb1 silently replaced)
  │                         │─send(v2)──►
```

## 7. Internal Architecture

```
reliableSyncer
├── client ViewSyncClient                           // unified service discovery + stream creation
├── mu sync.Mutex
├── resumableSyncers map[WorkNodeKey]*resumableSyncer
├── closed bool
├── ctx / cancel
└── watchNodes goroutine: watch changes → drainRemovedNodes

resumableSyncer
├── node WorkNode
├── client ViewSyncClient
├── pending *pendingSyncQueryViews                  // per-node pending tracker
├── ctx / cancel
└── loop goroutine:
        create stream → rePush → sendLoop + recvLoop → on break, backoff → retry

pendingSyncQueryViews
├── mu sync.Mutex
├── entries map[QueryViewKey]SyncView
├── unsent []*QueryViewOfShard
└── notify chan struct{} (cap 1)
```

## 8. File Organization

```
internal/views/coord/coordview/
├── syncer/
│   ├── reliable_syncer.go          # ReliableSyncer interface, SyncView, SyncGroup, ViewSyncClient
│   ├── syncer_impl.go              # reliableSyncer implementation + node watcher
│   ├── resumable_syncer.go         # Per-node stream with backoff retry
│   └── pending_sync_query_views.go # Per-node pending view tracker
└── state_machine.go                # CoordQueryView state machine
```
