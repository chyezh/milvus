# View Handler Design

> Work-node side components that receive Coord-pushed query views and report state machine changes back.
> Counterpart to the Coord-side [Syncer](coordview/syncer/syncer.md). Proto: [view.proto](../../pkg/proto/view.proto)

## 1. Architecture

```
Coord (ReliableSyncer → resumableSyncer → gRPC stream)
         │                          ▲
    gRPC Stream (bidi)         gRPC Stream (bidi)
         ▼                          │
Component A: ViewSyncServer (per-stream, SN/QN shared)
    │  recv loop: parse SyncRequest → call handler.ApplyViews
    │  send loop: drain pendingReports → stream.Send
    │
    ▼
Component B: QueryViewHandler (per-node singleton, SN/QN each implement)
    │  ApplyViews: create/find SM → drive state transition → register/replace callback
    │  Local events: SegmentManager/WAL drive SM → SM change → invoke OnReport callback
    │
    ▼
SM instances (QNQueryViewStateMachine / SNQueryViewStateMachine)
```

Two components with distinct lifecycles and responsibilities:

- **ViewSyncServer** (Component A): Per-stream gRPC server handler. Implements `SyncQueryView` bidirectional stream. Bridges the gRPC transport with the `QueryViewHandler`. SN and QN share the same implementation.
- **QueryViewHandler** (Component B): Per-node singleton managing all SM instances. SN and QN each provide their own implementation. Outlives individual gRPC streams.

## 2. Component B: QueryViewHandler

### 2.1 Interfaces

**ApplyView** pairs a coord-pushed query view with its report callback:

- `View` — the coord-pushed query view state. Target SM is determined by `View.QueryViewKey()`.
- `OnReport` — invoked whenever the SM produces a new report for this view (both immediate and asynchronous). Replaced on re-apply of the same `QueryViewKey` (stream reconnection). Must be non-blocking.

**QueryViewHandler** interface:

- `ApplyViews(views []ApplyView)` — atomically applies a batch of coord-pushed views. All state reports (immediate or asynchronous) are delivered exclusively through the `OnReport` callback in each `ApplyView`. No return value.
- `Close()` — releases all resources.

### 2.2 Symmetry with Coord-Side SyncView

| Coord side (`SyncView`)             | Work-node side (`ApplyView`)           |
|--------------------------------------|----------------------------------------|
| `View QueryViewAtWorkNode`           | `View QueryViewAtWorkNode`             |
| `OnSyncResponse func(resp) bool`     | `OnReport func(report)`               |
| `OnNodeLost func()`                  | _(not needed; stream close suffices)_  |

### 2.3 SM Lifecycle Management

- **Auto-create**: When `ApplyViews` receives an unknown `QueryViewKey` with Preparing state, a new SM instance is created.
- **Auto-destroy**: When an SM reaches the Dropped terminal state (whether from a sync apply or async event), it is removed from the internal map after the report is emitted.
- **Callback replacement**: On re-apply of the same `QueryViewKey`, the `OnReport` callback is replaced. The old callback is never invoked after replacement.

### 2.4 SN vs QN Implementations

The `QueryViewHandler` interface is the boundary where SN/QN differences are encapsulated:

| Aspect | QN Implementation | SN Implementation |
|---|---|---|
| SM type | `QNQueryViewStateMachine` | `SNQueryViewStateMachine` |
| Local events | Segment loading progress → `OnSegmentsReady` | WAL catch-up → `OnRecoveringDone`, resource prep → `OnReady` |
| Persistence | None (stateless) | Up recovery info (persist/delete via `ConsumePersist`) |
| Recovery | None | `RecoverSNQueryViewStateMachine` from persisted Up view |
| Connected components | Sealed Segment Manager, Delete Stream | Growing Segment Manager, WAL/Flusher |

## 3. Component A: ViewSyncServer

### 3.1 Overview

Implements the gRPC `SyncQueryView(stream)` server handler. Per-stream — created when a stream is established, destroyed when the stream ends. Holds a reference to the per-node `QueryViewHandler` singleton.

### 3.2 Internal Components

**pendingReports** — a thread-safe data structure that:

- Maintains a map of `QueryViewKey → latest QueryViewAtWorkNode`, ensuring only the most recent state per view key is kept (deduplication).
- Provides a `Ready()` notification channel (cap 1) to signal the send loop.
- Supports `SetCloseResponse()` to enqueue a graceful close signal.
- Supports `Close()` to shut down the notification channel, causing the send loop to drain and exit.

**Two concurrent loops** run for each stream:

**recv loop** (blocks on `stream.Recv` in main goroutine):
1. Receive `SyncRequest` from Coord.
2. If `SyncQueryViewsRequest`: convert each proto to `QueryViewAtWorkNode`, construct `ApplyView` with `OnReport` callback that calls `pendingReports.Update()`, then call `handler.ApplyViews(views)`.
3. If `SyncCloseRequest`: call `pendingReports.SetCloseResponse()` and return.

**send loop** (background goroutine — the only goroutine that calls `stream.Send`):
1. Select on `pendingReports.Ready()` and `ctx.Done()`.
2. On ready: drain all pending reports via `pendingReports.Drain()`, batch into `SyncResponse` and send. If close flag is set, send `SyncCloseResponse` and exit.
3. On `ctx.Done()`: exit.

### 3.3 Data Flow

All reports (immediate and asynchronous) follow the same path:

```
handler.ApplyViews / local event (SegmentManager, WAL, ...)
    → OnReport callback
    → pendingReports.Update(report)         # non-blocking, dedup by QueryViewKey
    → pendingReports.Ready() signaled
    → send loop: pendingReports.Drain()
    → stream.Send(SyncResponse)
```

Thread safety: `OnReport` only writes to `pendingReports` (lock-protected map + non-blocking channel signal). The send loop is the sole caller of `stream.Send`, avoiding concurrent writes to the gRPC stream.

### 3.4 Stream Lifecycle

1. **Stream established**: `SyncQueryView` creates a `pendingReports` instance, starts send loop goroutine, then enters recv loop.
2. **Coord re-push**: On reconnection, Coord's `resumableSyncer.rePush` sends all pending views. `ApplyViews` replaces old callbacks with new ones pointing to the new stream's `pendingReports`. `OnReport` reports give Coord the full current state.
3. **Graceful close**: Coord sends `SyncCloseRequest`. recv loop calls `pendingReports.SetCloseResponse()` and returns. Send loop drains remaining reports, sends `SyncCloseResponse`, and exits.
4. **Stream broken**: recv loop returns error. `pendingReports.Close()` shuts down notify channel, send loop drains and exits. Old `OnReport` callbacks become stale (writes to a stopped `pendingReports` are silently ignored). Coord detects stream break and reconnects with backoff.

### 3.5 Callback Safety

The `OnReport` callback calls `pendingReports.Update()`, which is non-blocking and safe to call from any goroutine. When the stream ends:
- `pendingReports.Close()` is called, after which `Update()` calls are silently ignored (signal is skipped on a stopped instance).
- The callback is eventually replaced by the next stream's callback via re-apply.
- Between stream break and re-apply, the old callback may still be invoked — `Update()` on a closed `pendingReports` is a no-op, no panic or blocking.

## 4. Key Scenarios

### 4.1 Normal Flow

```
Coord                    ViewSyncServer              QueryViewHandler
  │─send(Preparing)──────►│                           │
  │                        │─ApplyViews([{view,cb}])──►│
  │                        │                           │─create SM
  │                        │                           │─SM.OnCoordStateDelivered
  │                        │                           │─cb(Preparing report)
  │                        │◄──pendingReports.Update───│
  │◄─send(SyncResponse)───│                           │
  │                        │         ... async ...     │
  │                        │                           │─SegmentManager: OnSegmentsReady
  │                        │                           │─SM → Ready
  │                        │                           │─cb(Ready report)
  │                        │◄──pendingReports.Update───│
  │◄─send(SyncResponse)───│                           │
```

### 4.2 Stream Reconnection

```
Coord                    ViewSyncServer(old)         QueryViewHandler
  │─send(Preparing)──────►│                           │
  │                        │─ApplyViews([{view,cb1}])─►│
  │         ╳ stream breaks │                          │
  │                        ╳ (destroyed)               │
  │                                                    │
  │                    ViewSyncServer(new)              │
  │─re-push(Preparing)───►│                           │
  │                        │─ApplyViews([{view,cb2}])─►│
  │                        │                           │─replace cb1 with cb2
  │                        │                           │─cb2(current report)
  │                        │◄──pendingReports.Update───│
  │◄─send(SyncResponse)───│                           │
```

### 4.3 Async Report After SM State Change

```
Coord                    ViewSyncServer              QueryViewHandler
  │                        │                           │
  │                        │                           │─WAL catches up
  │                        │                           │─SM: UpRecovering → Up
  │                        │                           │─cb(Up report)
  │                        │◄──pendingReports.Update───│
  │◄─send(SyncResponse)───│                           │
```

### 4.4 SM Auto-Destroy on Dropped

```
Coord                    ViewSyncServer              QueryViewHandler
  │─send(Dropped)─────────►│                           │
  │                        │─ApplyViews([{view,cb}])──►│
  │                        │                           │─SM.OnCoordStateDelivered(Dropped)
  │                        │                           │─SM → Dropped
  │                        │                           │─remove SM from map
  │                        │                           │─cb(Dropped report)
  │                        │◄──pendingReports.Update───│
  │◄─send(SyncResponse)───│                           │
```

### 4.5 Graceful Close

```
Coord                    ViewSyncServer              pendingReports
  │─send(SyncCloseReq)───►│                           │
  │                        │─SetCloseResponse()───────►│
  │                        │  (recv loop returns)      │─signal Ready
  │                        │                           │
  │                        │  send loop: Drain()◄──────│
  │                        │  (reports + closing=true) │
  │◄─send(SyncResponse)───│  (buffered reports)       │
  │◄─send(SyncCloseResp)──│  (close response)         │
  │                        │  (send loop exits)        │
```

## 5. File Organization

```
internal/views/
├── view_handler.md                    # This document
├── coordview/
│   ├── syncer/                        # Coord-side syncer (existing)
│   └── state_machine.go              # Coord SM (existing)
├── handler/
│   ├── handler.go                    # ApplyView, QueryViewHandler interface
│   ├── pending_reports.go            # pendingReports dedup data structure
│   ├── sync_server.go                # ViewSyncServer gRPC handler
│   └── sync_server_test.go           # Unit tests
├── qnview/
│   ├── state_machine.go              # QN SM (existing)
│   └── handler.go                    # QN QueryViewHandler implementation (new)
└── snview/
    ├── state_machine.go              # SN SM (existing)
    └── handler.go                    # SN QueryViewHandler implementation (new)
```
