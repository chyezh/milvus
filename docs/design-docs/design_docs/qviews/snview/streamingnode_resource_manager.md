# StreamingNode Query Resource Manager Design

> StreamingNode-side query resource ownership for QueryView.
> This document defines the `StreamingNodeResourceManager` boundary used by WAL
> recovery and the StreamingNode QueryView state machine. Query execution,
> QueryCoord placement, and QueryNode sealed segment lifecycle are out of scope.

## 1. Purpose

`StreamingNodeResourceManager` prepares query resources from WAL state and keeps
them alive while QueryViews reference them.

It has two upstream callers:

```text
RecoveryStorage -> StreamingNodeResourceManager
QueryViewStateMachine -> StreamingNodeResourceManager
```

The same concrete component implements both WAL-side and QueryView-side
interfaces:

- `walview.LoadConfigListener`
- `snview.QueryViewResourceManager`

The purpose of this component is to:

1. receive WAL-captured load intent from `RecoveryStorage`;
2. prepare local StreamingNode query resources from `VChannelWALView`;
3. track initialization and QueryView references;
4. report resource readiness to QueryView state machines;
5. release resources only after all references are gone.

`Up` is not a resource-manager event. An `Up` QueryView is persisted only as
WAL-bound QueryView meta for StreamingNode crash recovery. Resource lifetime is
driven by `OnAlterLoadConfig`, `OnDropLoadConfig`, `Acquire`, and `Release`.

## 2. Dependency Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `RecoveryStorage` | Observes `AlterLoadConfig` / `DropLoadConfig`, restores WAL metadata on startup, builds valid `VChannelWALView`, and calls `StreamingNodeResourceManager` through `LoadConfigListener`. | It does not build csegments, fetch BM25 resources, or wait for query resources to become ready. WALView capture details are defined in [StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md). |
| `VChannelModule` | Persists `VChannelMeta.load_config`. | It is an input provider for `RecoveryStorage`, not a direct dependency of `StreamingNodeResourceManager`. |
| `SegmentModule` | Provides the visible segment snapshot for a requested base `DataVersion`. | It owns segment metadata and segment GC. The resource manager does not query or mutate it directly. |
| `TransformLogModule` | Provides historical transform replay and transform frontier for the WAL view. | It owns TransformLog storage. The resource manager consumes only the scanner/frontier already packaged in `VChannelWALView`. |
| `StreamingNodeResourceManager` | Owns StreamingNode query resources, resource references, readiness checks, and resource release. | It implements both `LoadConfigListener` and `QueryViewResourceManager`. |
| `QueryViewStateMachine` | Owns QueryView state transitions and calls `Acquire` when a QueryView starts using resources, and `Release` when a QueryView is dropped. | It does not manage csegments, BM25 resources, live observers, or resource GC directly. |
| `QueryView Meta` | WAL-bound metadata persisted for crash recovery. | It is used directly by recovery and `Acquire`; no extra resource-layer abstraction is required. |
| `QueryCoord` | Generates QueryViews and may provide sealed BM25 resources through RPC. | This document treats it only as an external dependency. |

The key dependency boundary is:

```text
VChannelModule / SegmentModule / TransformLogModule
        -> RecoveryStorage builds VChannelWALView
        -> StreamingNodeResourceManager
```

`StreamingNodeResourceManager` consumes `VChannelWALView` as the complete WAL
input package. It must not call back into WAL modules to rebuild a snapshot.
The WALView structure, capture order, live observer contract, and historical
delete replay contract are defined by
[StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md).

## 3. Component Relationships And Invariants

### 3.1 Relationship Model

```text
AlterLoadConfig / DropLoadConfig WAL message
        |
        v
RecoveryStorage
        |
        | LoadConfigListener
        v
StreamingNodeResourceManager
        ^
        | QueryViewResourceManager
        |
QueryViewStateMachine
```

`RecoveryStorage` creates or removes initialization intent. The QueryView state
machine creates or removes QueryView references. The resource manager is the
only component that owns the resulting resources.

### 3.2 Reference Model

The resource manager tracks references per loaded vchannel:

```text
resource refs =
  optional initRef(load_config)
  + queryViewRefs[QueryViewVersion]
```

Reference rules:

1. `OnAlterLoadConfig` creates an initialization reference, `initRef`.
2. `Acquire(QueryView)` creates a `queryViewRef`.
3. The first successful `Acquire` atomically transfers ownership from
   `initRef` to the QueryView lifecycle by registering the QueryView reference
   and removing `initRef` in the same state update.
4. Later `Acquire` calls only add QueryView references.
5. `Release(QueryView)` removes the corresponding QueryView reference.
6. `OnDropLoadConfig` removes only `initRef`.
7. Resources can be released only when both `initRef` and all
   `queryViewRefs` are absent.

### 3.3 Resource State

The component maintains vchannel-local state:

```text
vchannelResourceState
  loadConfigPresent bool
  initRef bool
  queryViewRefs map[QueryViewVersion]QueryViewMeta
  runtimes map[DataVersion]runtimeState
  idfOracleRuntime IDFOracleRuntime
  loading currentLoadTask
```

`runtimeState` contains StreamingNode-side query resources retained for a
specific resource DataVersion:

- csegment-backed growing data;
- retained flushed-as-growing data required by older QueryViews;
- historical delete replay result;
- live WAL observer and apply frontier;
- other query resources tied to the same DataVersion model.

`idfOracleRuntime` is a vchannel-level singleton resource. It is initialized by
the same `OnAlterLoadConfig` / recovery build flow, but it is not retained as a
`DataVersion -> oracle` map. Its lifecycle and asynchronous advancement are
defined in [StreamingNode IDF Oracle Runtime Design](idf_oracle_runtime.md).

### 3.4 Invariants

1. `RecoveryStorage` depends on `StreamingNodeResourceManager` only through
   `LoadConfigListener`.
2. `QueryViewStateMachine` depends on `StreamingNodeResourceManager` only
   through `QueryViewResourceManager`.
3. `StreamingNodeResourceManager` is the only owner of StreamingNode query
   resource lifetime.
4. `VChannelModule`, `SegmentModule`, and `TransformLogModule` are WALView input
   providers through `RecoveryStorage`; the resource manager does not query them
   directly.
5. `AlterLoadConfig` creates `initRef` and starts resource preparation.
6. `DropLoadConfig` removes only `initRef`.
7. QueryView `Acquire` creates QueryView references; QueryView `Release`
   removes them.
8. `Up` persistence is recovery metadata only and is not a resource-manager
   signal.
9. Recovery acquires QueryViews in QueryViewVersion order.
10. Resources are released only after all resource-manager references are gone.

## 4. Interface Description

### 4.1 Component Interface

```go
type StreamingNodeResourceManager interface {
    walview.LoadConfigListener
    snview.QueryViewResourceManager
}
```

### 4.2 WAL-Side Interface

```go
type LoadConfigListener interface {
    OnAlterLoadConfig(view walview.VChannelWALView) walview.VChannelLiveObserver
    OnDropLoadConfig(event walview.DropLoadConfigEvent)
}
```

`OnAlterLoadConfig` receives a complete WAL input view and starts asynchronous
resource preparation. It returns the live observer that `RecoveryStorage` uses
to deliver later resource events.

`OnDropLoadConfig` removes the initialization reference for the vchannel. It is
not a QueryView cleanup command.

### 4.3 QueryView-Side Interface

```go
type QueryViewResourceManager interface {
    Acquire(req AcquireResource)
    Release(req ReleaseResource)
}
```

`Acquire` registers a QueryView reference and eventually reports either ready or
unrecoverable.

`Release` removes a QueryView reference and releases resources if no references
remain.

Persisted QueryViews are WAL-bound meta. Recovery uses the same ordered
`Acquire` operation as the normal flow and does not introduce a separate
resource path.

### 4.4 Readiness Contract

`Acquire` must eventually invoke exactly one terminal callback:

- ready, when all resources required by the QueryView are available;
- unrecoverable, when the requested version cannot be served.

Readiness checks are read-only with respect to WAL modules. They can wait for an
existing WAL-triggered preparation task, but they must not pull a new historical
snapshot from `SegmentModule` or `TransformLogModule`.

Repeated `Acquire` for the same QueryView version is idempotent and returns the
same logical readiness result.

## 5. Actual Behavior

### 5.1 Normal AlterLoadConfig Flow

`AlterLoadConfig` is the only WAL trigger that starts StreamingNode-side query
resource preparation.

```text
RecoveryStorage observes AlterLoadConfig
  -> VChannelModule persists VChannelMeta.load_config
  -> SegmentModule provides a visible segment snapshot
  -> TransformLogModule provides delete replay and transform frontier
  -> RecoveryStorage builds VChannelWALView
  -> StreamingNodeResourceManager.OnAlterLoadConfig(view)
  -> StreamingNodeResourceManager creates initRef
  -> StreamingNodeResourceManager starts asynchronous resource preparation
```

The normal preparation base is:

```text
normalBaseDataVersion = SegmentModule.MaxDataVersion(vchannel)
```

`RecoveryStorage` builds `VChannelWALView` for this base DataVersion. The
WALView capture rules, segment snapshot contents, live observer, and delete
replay are defined in
[StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md).
The segment visibility rule is owned by [Segment View Module](../../wal/segment_view_module.md).

The resource manager prepares resources from the WAL view only. It must not call
back into `SegmentModule` or `TransformLogModule` to rebuild inputs.

### 5.2 QueryView Acquire Flow

When the StreamingNode QueryView state machine receives or recovers a QueryView
that needs local resources, it calls:

```text
StreamingNodeResourceManager.Acquire(QueryViewMeta)
```

`Acquire` registers a QueryView reference and waits until the resource runtime
needed by the QueryView is ready.

For the first QueryView reference on a vchannel, `Acquire` also removes the
initialization reference created by `OnAlterLoadConfig`. This is the ownership
transfer from load-config initialization to QueryView lifecycle management.

After the QueryView reference is registered and the initial resource is ready,
`Acquire` may notify the vchannel-level IDF oracle runtime of the QueryView
DataVersion through `MaybeAdvance`. This is an asynchronous IDF maintenance
signal and does not make QueryView readiness wait for IDF advancement. The IDF
runtime remains protected by the QueryView reference after the initialization
reference is removed.

### 5.3 QueryView Up Flow

When a QueryView becomes `Up`, the state machine persists the QueryView meta for
StreamingNode crash recovery. The resource manager is not notified.

The first QueryView `Up` report for a vchannel must wait for the IDF oracle
initial catchup described in
[StreamingNode IDF Oracle Runtime Design](idf_oracle_runtime.md). Later
QueryViews do not wait for asynchronous IDF advancement before reporting `Up`.

This keeps responsibilities separate:

- the state machine persists recovery state;
- the resource manager manages resources through references;
- `Up` itself does not change resource lifetime.

### 5.4 QueryView Release Flow

When the QueryView state machine drops a view, it calls:

```text
StreamingNodeResourceManager.Release(QueryViewMeta)
```

`Release` removes the QueryView reference. If no initialization reference and no
QueryView references remain, all resources owned by that vchannel can be
released.

Resource release may close live observers, cancel in-flight preparation, release
csegments, and close the vchannel-level IDF oracle runtime. IDF sealed BM25
cache leases are released by `IDFOracleRuntime` during its own close path.

Segment metadata GC remains owned by `SegmentModule`. The resource manager can
release its query resources, but it does not delete SegmentModule metadata
directly.

### 5.5 DropLoadConfig Flow

`DropLoadConfig` removes only the load intent:

```text
RecoveryStorage observes DropLoadConfig
  -> VChannelModule persists VChannelMeta.load_config = nil
  -> StreamingNodeResourceManager.OnDropLoadConfig(...)
  -> StreamingNodeResourceManager removes initRef
```

`DropLoadConfig` must not remove QueryView references. If resources have already
been acquired by QueryViews, they remain alive until the QueryView state machine
calls `Release`.

If no QueryView has acquired the resources, removing `initRef` makes the
resources unreferenced and therefore releasable.

### 5.6 Recovery Flow

StreamingNode recovery restores both WAL-owned load intent and persisted
QueryView meta.

For each vchannel:

```text
1. RecoveryStorage reads VChannelMeta and persisted QueryView meta.
2. If persisted Up QueryViews exist:
     sort them by QueryViewVersion ascending
     recoveryBaseDataVersion = first QueryView.DataVersion
   Else:
     recoveryBaseDataVersion = SegmentModule.MaxDataVersion(vchannel)
3. RecoveryStorage builds a valid VChannelWALView(recoveryBaseDataVersion).
4. RecoveryStorage calls StreamingNodeResourceManager.OnAlterLoadConfig(view).
5. QueryViewStateMachine recovers persisted Up QueryViews and calls Acquire
   sequentially in QueryViewVersion order.
```

The ordered `Acquire` requirement is part of the contract:

```text
Acquire order per vchannel:
  DataVersion ascending, then QueryVersion ascending
```

This guarantees that the oldest recovered Up QueryView establishes the minimum
resource boundary first. Higher-version QueryViews only add references on top of
the already recovered resource base.

### 5.7 Unrecoverable Conditions

A QueryView is unrecoverable when:

- load intent is absent and no recovered QueryView path created a valid WAL
  view;
- the requested DataVersion is older than the retained resource boundary;
- the requested resources failed to prepare;
- the QueryView is acquired out of version order during recovery.

### 5.8 Cleanup Rule

The resource manager releases a vchannel's resources only when:

```text
initRef == false
and len(queryViewRefs) == 0
```

`DropLoadConfig` alone is not a cleanup command. It only removes `initRef`; the
normal reference rule then decides whether resources are releasable.
