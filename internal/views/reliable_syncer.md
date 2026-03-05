# ReliableSyncer Design

> A reliable message delivery layer over unreliable gRPC bidirectional streams.
> Interface: [reliable_syncer.go](coordview/reliable_syncer.go), Proto: [view.proto](../../pkg/proto/view.proto)

## 1. Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                          Caller                                    │
│              │ SyncViews(group)      ▲ callback(resp)              │
│              ▼                       │                             │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                     ReliableSyncer                          │  │
│  │  • Outstanding: tracks all pending view requests            │  │
│  │  • Watches node changes from SN/QN Clients                 │  │
│  │  • Creates/destroys ResumableSyncers per node               │  │
│  │  • Node loss = service discovery removal (not timeout)      │  │
│  │                                                             │  │
│  │  ┌──────────────────┐ ┌──────────────────┐                 │  │
│  │  │ ResumableSyncer  │ │ ResumableSyncer  │ ...             │  │
│  │  │ (per SN/QN node) │ │ (per SN/QN node) │                │  │
│  │  │ • backoff retry  │ │ • backoff retry  │                 │  │
│  │  │ • push/repush    │ │ • push/repush    │                 │  │
│  │  │ • send/recv loop │ │ • send/recv loop │                 │  │
│  │  └──────────────────┘ └──────────────────┘                 │  │
│  └─────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────┘
              │                     ▲
         gRPC Stream           gRPC Stream
              ▼                     │
         Work Node (SN/QN)
```

**ResumableSyncer**: Per-node component that continuously tries to maintain a gRPC bidirectional stream (with exponential backoff) and push/re-push outstanding views. Modeled after [`producer_resuming.go`](../../internal/distributed/streaming/internal/producer/producer_resuming.go).

**ReliableSyncer**: Top-level component that:
1. Owns the global `Outstanding` data structure tracking all pending view requests.
2. Watches node changes via SN/QN clients and creates/destroys ResumableSyncers accordingly.
3. Detects node loss via service discovery (not reconnect timeout).

## 2. ReliableSyncer Interface

Defined in [`coordview/reliable_syncer.go`](coordview/reliable_syncer.go). See Go source for full documentation.

## 3. Outstanding

A shared data structure tracking all pending view requests that are waiting for responses.

```
Outstanding
├── mu sync.Mutex
├── entries map[viewKey]outstandingEntry
└── viewKey = { replicaID, vchannel, dataVersion, queryVersion }

outstandingEntry = { view, callback, onNodeLost }
```

The Outstanding is **global** (not per-node), because:
- View keys are globally unique (replicaID + vchannel + version).
- On node loss, ReliableSyncer needs to find all entries targeting that node.
- On `SyncViews`, entries are upserted regardless of which node they target.

### Operations

| Event | Action |
|---|---|
| `SyncViews(group)` | Upsert `entries[key]` for each view. Route proto to target node's ResumableSyncer. |
| Response received | Match `entries[key]`. If found: `callback(resp)`. If true → delete. |
| Node declared lost (via service discovery) | For all entries targeting this node: `callback(onNodeLost())`. Delete entries. |
| Entry replaced | Old callback silently replaced; NOT invoked. |

### Response-to-Callback Routing

```
for each viewProto in response.QueryViews:
    resp = qviews.NewQueryViewAtWorkNodeFromProto(viewProto)
    key = viewKeyFrom(resp)
    if entry exists in outstanding[key]:
        if entry.callback(resp):
            delete outstanding[key]
```

Responses not matching any outstanding entry are ignored.

## 4. ResumableSyncer

Per-node component that maintains a gRPC bidirectional stream. Modeled after `producerWithResumingError` in [`producer_resuming.go`](../../internal/distributed/streaming/internal/producer/producer_resuming.go).

### Core Pattern (from producer_resuming.go)

Uses `syncutil.ContextCond` to swap streams:
- A background goroutine continuously creates streams (with backoff on failure) and calls `SwapStream(stream, err)`.
- The send loop calls `GetStreamAfterAvailable()` to wait for a usable stream, then pushes outstanding views and reads from the send channel.
- The recv loop reads responses and routes them to Outstanding callbacks.

### Lifecycle

```
ResumableSyncer created (node discovered via service discovery)
    │
    ├── Background goroutine: continuously create gRPC stream with backoff
    │       │
    │       ├── SwapStream(newStream, nil) on success
    │       │       → send loop wakes up, re-pushes outstanding, reads sendCh
    │       │       → recv loop reads responses, routes to Outstanding
    │       │
    │       └── SwapStream(nil, err) on failure → backoff → retry
    │
    ├── Send: accept views via sendCh
    │
    └── Close: cancel context, close stream
```

### Stream Creation

Different node types use different clients to create streams:

- **StreamingNode**: Uses the StreamingNode HandlerClient (channel-assignment-based discovery).
  The HandlerClient already provides `ViewSyncServiceClient` via its gRPC connection to StreamingNodes.

- **QueryNode**: Uses a new QueryNode Client (etcd session-based discovery, following ManagerClient pattern).
  Located in `internal/querynodev2/client/`. Uses `resolver.NewSessionBuilder(etcdCli, ...)` to discover all QueryNodes from etcd sessions, similar to how [`manager_client.go`](../../internal/streamingnode/client/manager/manager_client.go) discovers StreamingNodes.

Both clients provide:
1. `WatchNodeChanged(ctx) (<-chan struct{}, error)` — subscribe to node membership changes.
2. A way to create `ViewSyncService_SyncQueryViewClient` streams to specific nodes (via `contextutil.WithPickServerID`).

## 5. Node Loss Detection

Node loss is determined by **service discovery**, not by reconnect timeout.

```
Service discovery detects node removal
    │
    ├── SN removed: HandlerClient's watcher reports channel reassignment
    │   → ReliableSyncer closes the SN's ResumableSyncer
    │   → All outstanding entries for this SN: callback(onNodeLost())
    │
    └── QN removed: QN Client's session watcher reports session deletion
        → ReliableSyncer closes the QN's ResumableSyncer
        → All outstanding entries for this QN: callback(onNodeLost())
```

Key differences from the old design:
- **No reconnect timeout**: ResumableSyncer retries indefinitely with backoff until closed.
- **No `lostNodes` set**: Node loss is event-driven from service discovery. If a node with the same identity re-registers, service discovery will emit a new add event.
- **Separation of concerns**: Stream reconnection (ResumableSyncer) is independent of node liveness (service discovery).

## 6. Node Discovery and ResumableSyncer Management

ReliableSyncer runs background tasks to watch node changes from both clients:

```
Background Task: Watch SN/QN node changes
    │
    loop:
        wait for node change signal (from WatchNodeChanged)
        │
        ├── Fetch current node set (GetAllNodes)
        │
        ├── For new nodes (in current set but no ResumableSyncer):
        │       create ResumableSyncer
        │
        └── For removed nodes (have ResumableSyncer but not in current set):
                close ResumableSyncer
                drain outstanding entries → callback(onNodeLost())
```

This runs for both SN and QN independently:
- SN watcher: uses StreamingNode HandlerClient's node change events.
- QN watcher: uses QueryNode Client's `WatchNodeChanged`.

## 7. Key Scenarios

### 7.1 Normal Flow

```
Caller                   ReliableSyncer              Node
  │─SyncViews({[{v,cb}]})─►│                           │
  │                         │─outstanding[vk]={v,cb}    │
  │                         │─ResumableSyncer.Send(v)──►│
  │                         │◄──recv(resp)──────────────│
  │                         │─cb(resp) → true           │
  │  cb invoked             │─delete outstanding[vk]    │
```

### 7.2 Stream Break + Reconnection

```
Caller                   ReliableSyncer              Node
  │─SyncViews({[{v,cb}]})─►│─outstanding[vk]={v,cb}    │
  │                         │─send(v)──────────────────►│
  │                         │        ╳ stream breaks     │
  │                         │─ResumableSyncer backoff..  │
  │                         │─new stream opened─────────│
  │                         │─re-push outstanding──────►│
  │                         │◄──recv(resp)──────────────│
  │                         │─cb(resp) → true           │
  │  cb invoked             │─delete outstanding[vk]    │
```

### 7.3 Node Lost (Service Discovery)

```
Caller                   ReliableSyncer              Node
  │─SyncViews({[{v,cb}]})─►│─outstanding[vk]={v,cb}    │
  │                         │─send(v)──────────────────►│
  │                         │                            ╳ node crashes
  │                         │◄─service discovery: node removed
  │                         │─close ResumableSyncer
  │                         │─resp = onNodeLost()
  │                         │─cb(resp)
  │  cb invoked             │─delete outstanding[vk]
```

### 7.4 Callback Returns False (Continue Monitoring)

```
Caller                   ReliableSyncer              Node
  │─SyncViews({[{v,cb}]})─►│─outstanding[vk]={v,cb}    │
  │                         │─send(v)──────────────────►│
  │                         │◄──recv(resp1)─────────────│
  │                         │─cb(resp1) → false         │
  │  (continue)             │◄──recv(resp2)─────────────│
  │                         │─cb(resp2) → true          │
  │  (done)                 │─delete outstanding[vk]    │
```

### 7.5 Entry Replacement

```
Caller                   ReliableSyncer
  │─SyncViews({[{v1,cb1}]})►│─outstanding[vk]={v1,cb1}
  │                          │─send(v1)──►
  │─SyncViews({[{v2,cb2}]})►│─outstanding[vk]={v2,cb2}
  │                          │  (cb1 silently replaced)
  │                          │─send(v2)──►
```

## 8. QueryNode Client

Located in `internal/querynodev2/client/`. Follows the same pattern as StreamingNode's [`ManagerClient`](../../internal/streamingnode/client/manager/manager_client.go).

```go
// QueryNodeClient provides service discovery and gRPC connections to QueryNodes.
// Wraps etcd Session Service Discovery.
type QueryNodeClient interface {
    // WatchNodeChanged returns a channel that signals QueryNode membership changes.
    WatchNodeChanged(ctx context.Context) (<-chan struct{}, error)

    // GetAllQueryNodes fetches all discovered QueryNode info.
    GetAllQueryNodes(ctx context.Context) (map[int64]*QueryNodeInfo, error)

    // GetViewSyncClient returns a ViewSyncServiceClient for making streaming RPCs.
    // Use contextutil.WithPickServerID to route to specific nodes.
    GetViewSyncClient(ctx context.Context) (viewpb.ViewSyncServiceClient, error)

    Close()
}
```

Implementation:
- Uses `resolver.NewSessionBuilder(etcdCli, discoverer.OptSDPrefix(queryNodeRole), ...)` for etcd session discovery.
- Uses `picker.ServerIDPickerBalancerName` for node-pinned routing.
- Creates `viewpb.NewViewSyncServiceClient(conn)` over the lazy gRPC connection.

## 9. Internal Architecture

```
ReliableSyncer
├── outstanding *Outstanding              // global outstanding entry tracking
├── snClient StreamingNodeHandlerClient   // SN service discovery + stream creation
├── qnClient QueryNodeClient             // QN service discovery + stream creation
├── mu sync.RWMutex
├── resumableSyncers map[string]*ResumableSyncer  // keyed by node identity
├── ctx / cancel
├── bgSNWatcher goroutine: watch SN changes → create/destroy ResumableSyncers
└── bgQNWatcher goroutine: watch QN changes → create/destroy ResumableSyncers

Outstanding
├── mu sync.Mutex
├── entries map[viewKey]outstandingEntry
└── methods: Upsert, Remove, DrainByNode, MatchResponse

ResumableSyncer
├── node WorkNode
├── cond *syncutil.ContextCond            // for stream swap (producer_resuming pattern)
├── stream ViewSyncService_SyncQueryViewClient
├── err error
├── sendCh chan []*viewpb.QueryViewOfShard
├── ctx / cancel
├── bgStreamCreator goroutine: backoff retry → SwapStream()
├── sendGoroutine: GetStreamAfterAvailable() → re-push outstanding → sendCh loop
└── recvGoroutine: stream.Recv() → Outstanding.MatchResponse()
```

## 10. File Organization

```
internal/views/coordview/
├── state_machine.go       # (existing)
├── reliable_syncer.go     # ReliableSyncer interface, SyncView, SyncGroup (existing)
├── outstanding.go         # Outstanding data structure
├── resumable_syncer.go    # ResumableSyncer (per-node stream with backoff retry)
├── syncer_impl.go         # ReliableSyncer implementation + node watcher
├── view_key.go            # viewKey type
└── syncer_impl_test.go    # tests

internal/querynodev2/client/
├── client.go              # QueryNodeClient interface
└── client_impl.go         # etcd session discovery implementation
```

## 11. Implementation Steps

1. `internal/querynodev2/client/` — QueryNodeClient (etcd session discovery, following ManagerClient pattern)
2. `coordview/view_key.go` — viewKey type and extraction
3. `coordview/outstanding.go` — Outstanding data structure
4. `coordview/resumable_syncer.go` — ResumableSyncer (backoff stream retry, send/recv loops)
5. `coordview/syncer_impl.go` — ReliableSyncer (node watchers, ResumableSyncer lifecycle)
6. `coordview/syncer_impl_test.go` — tests
