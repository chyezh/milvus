# StreamingNode Query Resource Manager Design

> StreamingNode-side query resource ownership for QueryView.
> This document defines the PChannel-local resource manager, the vchannel-level
> `QueryRuntime`, and the reference model used by WAL recovery and the
> StreamingNode QueryView state machine. Query execution, QueryCoord placement,
> and QueryNode sealed segment lifecycle are out of scope.

## 1. Purpose

`StreamingNodeResourceManager` prepares and owns StreamingNode-local query
resources for QueryView.

The manager is scoped to one `PChannelRuntime`. A StreamingNode may run many
PChannel runtimes, and each PChannel runtime owns one resource manager instance.
The manager does not serve resources across PChannels.

Inside one `PChannelRuntime`, it has two upstream callers:

```text
RecoveryStorage -> StreamingNodeResourceManager
QueryViewStateMachine -> StreamingNodeResourceManager
```

The same PChannel-local component implements both WAL-side and QueryView-side
interfaces:

- `walview.LoadConfigListener`
- `snview.StreamingNodeResourceManager`

The manager owns resource lifetime. The actual vchannel resources are held by a
single `QueryRuntime` per loaded vchannel:

```text
StreamingNodeResourceManager
  -> QueryRuntime
       -> GrowingRuntime
       -> IDFOracleRuntime
```

`QueryRuntime` is the only `walview.VChannelLiveObserver` for the vchannel. It
buffers live resource events while preparing, initializes all query resource
modules, drains the pending buffer into every module, and reports catchup only
after the whole vchannel resource is ready.

`Up` is not a resource-manager event. An `Up` QueryView is persisted only as
WAL-bound QueryView meta for StreamingNode crash recovery. Resource lifetime is
driven by `OnAlterLoadConfig`, `OnDropLoadConfig`, `Acquire`, and `Release`.

## 2. Dependency Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `PChannelRuntime` | Owns one PChannel WAL instance and PChannel-local WAL submodules, including `RecoveryStorage`, `QueryViewStateMachine`, and `StreamingNodeResourceManager`. | It coordinates WAL open, recovery, handoff close, and module close order. It does not build vchannel query resources directly. |
| `RecoveryStorage` | Observes `AlterLoadConfig` / `DropLoadConfig`, restores WAL metadata on startup, builds valid `VChannelWALView`, and calls the resource manager through `LoadConfigListener`. | It does not build csegments, fetch BM25 resources, wait for query resources to become ready, or manage QueryView lifecycle. WALView capture details are defined in [StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md). |
| `StreamingNodeResourceManager` | Owns PChannel-local resource references, creates vchannel `QueryRuntime` instances, submits initialization tasks, waits for resource catchup on `Acquire`, advances DataVersion watermarks, and releases resources. | It does not apply WAL events to concrete resources directly. It does not own QueryView state transitions. |
| `QueryRuntime` | VChannel-level singleton resource runtime. Implements `VChannelLiveObserver`, owns pending live-event buffering, initializes resource modules, drains pending events into modules, exposes whole-resource catchup, and broadcasts DataVersion advancement. | It does not own QueryView references, `load_config` meta, or WAL module snapshots. |
| `QueryRuntimeModule` | Common lifecycle interface implemented by resource modules. | It does not manage references or live observer registration. |
| `GrowingRuntime` | QueryRuntime module that owns growing segment resources for the vchannel. | It does not implement `VChannelLiveObserver`, maintain pending buffers, or decide QueryView lifecycle. Details are defined in [StreamingNode Growing Segment Runtime Design](growing_segment_runtime.md). |
| `IDFOracleRuntime` | QueryRuntime module that owns the vchannel singleton BM25 / IDF oracle. | It does not implement `VChannelLiveObserver`, expose external truncation, or own QueryView references. Details are defined in [StreamingNode IDF Oracle Runtime Design](idf_oracle_runtime.md). |
| `Scheduler` | Runs `QueryRuntime` initialization tasks with bounded concurrency. | It does not know QueryView references, resource lifetime, or DataVersion watermarks. |
| `QueryViewStateMachine` | PChannel-local WAL submodule that owns QueryView transitions, calls `Acquire` when a QueryView starts using local resources, calls `Release` when a QueryView leaves this PChannel runtime, and drains local QueryViews before WAL handoff close. | It does not manage csegments, BM25 resources, live observers, or resource GC directly. |
| `QueryView Meta` | WAL-bound metadata persisted for crash recovery and owned by the PChannel-local QueryView state machine. | It is stored under the PChannel WAL identity, used by QueryView recovery and `Acquire`, and must not be scoped by StreamingNode node ID. |

The key dependency boundary is:

```text
VChannelModule / SegmentModule / TransformLogModule
        -> RecoveryStorage builds VChannelWALView
        -> StreamingNodeResourceManager
        -> QueryRuntime
        -> QueryRuntimeModule

QueryViewStateMachine
        -> StreamingNodeResourceManager
```

`StreamingNodeResourceManager` consumes `VChannelWALView` as the complete WAL
input package. It must not call back into WAL modules to rebuild a snapshot.
The WALView structure, capture order, live observer contract, and historical
delete replay contract are defined by
[StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md).

## 3. Component Relationships And Invariants

### 3.1 Relationship Model

Normal load:

```text
AlterLoadConfig WAL message
        |
        v
RecoveryStorage
        |
        | OnAlterLoadConfig(VChannelWALView)
        v
StreamingNodeResourceManager
        |
        | create vchannel singleton
        v
QueryRuntime(Preparing, VChannelLiveObserver)
        |
        | submit initialization
        v
Scheduler
```

Live resource events:

```text
RecoveryStorage
        |
        | ObserveEvent
        v
QueryRuntime
        |
        | ordered ApplyLiveEvent
        v
QueryRuntimeModule
```

QueryView references:

```text
QueryViewStateMachine
        |
        | Acquire / Release
        v
StreamingNodeResourceManager
        |
        | Advance(oldestDataVersion)
        v
QueryRuntime
        |
        | module.Advance(oldestDataVersion)
        v
GrowingRuntime / IDFOracleRuntime
```

### 3.2 Reference Model

The resource manager tracks references per loaded vchannel:

```text
resource refs =
  optional initRef(load_config)
  + queryViewRefs[QueryViewVersion]
```

Reference rules:

1. `OnAlterLoadConfig` creates `initRef`.
2. `Acquire(QueryView)` creates a `queryViewRef`.
3. The first successful `Acquire` atomically registers the QueryView reference
   and removes `initRef`.
4. Later `Acquire` calls only add QueryView references.
5. `Release(QueryView)` removes the corresponding QueryView reference.
6. `OnDropLoadConfig` removes only `initRef`.
7. Resources can be released only when both `initRef` and all
   `queryViewRefs` are absent.
8. WAL handoff close drains `queryViewRefs` through
   `QueryViewStateMachine.CloseForHandoff` before the resource manager is
   finalized.

### 3.3 VChannel Resource State

The manager maintains one resource state per loaded vchannel:

```text
resources map[vchannel]vchannelResourceState

vchannelResourceState
  initRef bool
  queryViewRefs map[QueryViewVersion]QueryViewMeta
  runtime QueryRuntime
  buildTask QueryRuntimeBuildTask
```

There is no `DataVersion -> runtime` map. A loaded vchannel owns exactly one
`QueryRuntime`.

The resource key is only `vchannel`. `collectionID` is a consistency property
inside `VChannelWALView`, `QueryRuntime`, and resource modules. It is not part
of the manager's resource identity. If a repeated load for the same vchannel
contains a different collection ID, it is a critical WAL/resource consistency
bug and must fail by assertion.

The runtime owns all module state:

```text
QueryRuntime
  collectionID
  vchannel
  baseDataVersion
  state Preparing | Ready | Closed
  pendingEvents []VChannelResourceEvent
  modules []QueryRuntimeModule
  growingRuntime GrowingRuntime
  idfOracleRuntime IDFOracleRuntime
  catchupDone
  catchupErr
```

### 3.4 DataVersion Advancement

The resource manager computes one watermark from active QueryView references:

```text
oldestDataVersion = min(queryViewRefs.DataVersion)
```

It calls `QueryRuntime.Advance(oldestDataVersion)` only when at least one
QueryView reference exists.

`QueryRuntime.Advance` broadcasts the same watermark to every module:

```text
GrowingRuntime.Advance(oldestDataVersion)
IDFOracleRuntime.Advance(oldestDataVersion)
```

Module-specific meaning:

- `GrowingRuntime` uses the watermark to release growing segment state no longer
  needed by any active QueryView.
- `IDFOracleRuntime` uses the watermark to asynchronously advance BM25 / IDF
  oracle state. The oracle must not advance beyond the oldest active QueryView.

### 3.5 Invariants

1. `RecoveryStorage` depends on `StreamingNodeResourceManager` only through
   `LoadConfigListener`.
2. `QueryViewStateMachine` depends on `StreamingNodeResourceManager` only
   through `QueryViewResourceManager`.
3. `StreamingNodeResourceManager` is the only owner of StreamingNode query
   resource lifetime for its PChannel.
4. A loaded vchannel has at most one `QueryRuntime`.
5. `QueryRuntime` is the only live observer returned to `RecoveryStorage`.
6. Resource modules do not implement `VChannelLiveObserver`.
7. `QueryRuntime.CatchupDone` represents whole-resource catchup, not a single
   module's catchup.
8. `AlterLoadConfig` creates `initRef` and starts `QueryRuntime`
   initialization.
9. `DropLoadConfig` removes only `initRef`.
10. QueryView `Acquire` creates QueryView references; QueryView `Release`
    removes them.
11. QueryView `Acquire` does not create or schedule a new runtime.
12. Recovery acquires QueryViews in QueryViewVersion order.
13. Resources are released only after all resource-manager references are gone.
14. PChannel handoff close must first unmount local QueryViews through
    `QueryViewStateMachine`, then close the resource manager.

## 4. Interface Description

### 4.1 PChannel Resource Manager

```go
type StreamingNodeResourceManager interface {
    walview.LoadConfigListener
    snview.StreamingNodeResourceManager
    Close()
}
```

There is one `StreamingNodeResourceManager` instance per `PChannelRuntime`.
`Close` is called by `PChannelRuntime` after the PChannel-local QueryView state
machine has drained local QueryViews.

### 4.2 WAL-Side Interface

```go
type LoadConfigListener interface {
    OnAlterLoadConfig(view walview.VChannelWALView) walview.VChannelLiveObserver
    OnDropLoadConfig(event walview.DropLoadConfigEvent)
}
```

`OnAlterLoadConfig` receives a complete WAL input view, creates the vchannel
singleton `QueryRuntime` in `Preparing` state, submits its initialization task,
and returns the runtime as the live observer.

`OnDropLoadConfig` removes the initialization reference for the vchannel. It is
not a QueryView cleanup command.

### 4.3 QueryView-Side Interface

```go
type QueryViewResourceManager interface {
    Acquire(ctx context.Context, req AcquireResource) error
    Release(ctx context.Context, req ReleaseResource)
}
```

`Acquire` waits for `QueryRuntime.CatchupDone` before QueryView can report `Up`.
After reference registration, it advances the runtime with the oldest active
QueryView DataVersion.

`Release` removes the QueryView reference. If QueryView references remain, it
advances the runtime with the new oldest active QueryView DataVersion. If no
reference remains and `initRef` is absent, it closes the runtime.

### 4.4 QueryRuntime

```go
type QueryRuntime interface {
    walview.VChannelLiveObserver

    Initialize(ctx context.Context) error

    CatchupDone() <-chan struct{}
    CatchupError() error

    Advance(oldestDataVersion qviews.DataVersion)

    Close()
}
```

`Initialize` prepares all modules and drains the pending live-event buffer.

`CatchupDone` closes only after:

1. every module has finished `Prepare`;
2. the runtime has entered `Ready`;
3. all pending events captured during `Preparing` have been applied to every
   module in WAL order.

`Advance` is called only when at least one QueryView reference exists.

### 4.5 QueryRuntimeModule

```go
type QueryRuntimeModule interface {
    Prepare(ctx context.Context) error
    ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent)
    Advance(oldestDataVersion qviews.DataVersion)
    Close()
}
```

`GrowingRuntime` and `IDFOracleRuntime` both implement this interface. Concrete
query-facing accessors are exposed by their own module-specific interfaces, not
by `QueryRuntimeModule`.

`ApplyLiveEvent` has no recoverable error return. Failure to apply valid live
input means the WALView input or local runtime state is corrupted and the
StreamingNode must fail critically.

### 4.6 Scheduler

```go
type Scheduler interface {
    Submit(task QueryRuntimeBuildTask)
    Close()
}

type QueryRuntimeBuildTask interface {
    Key() string // vchannel

    Run()
    Done() <-chan struct{}
    Result() (QueryRuntime, error)

    Cancel()
}
```

The scheduler guarantees bounded initialization concurrency. It does not manage
references, create tasks from QueryView `Acquire`, choose DataVersions, or apply
resource events.

## 5. Actual Behavior

### 5.1 Normal Load

```text
RecoveryStorage observes AlterLoadConfig
  -> builds VChannelWALView
  -> StreamingNodeResourceManager.OnAlterLoadConfig(view)
  -> manager creates initRef
  -> manager creates QueryRuntime(Preparing)
  -> manager submits QueryRuntimeBuildTask
  -> manager returns QueryRuntime as VChannelLiveObserver
```

The returned observer receives live resource events immediately. Events observed
before runtime readiness are stored in `QueryRuntime.pendingEvents`.

### 5.2 QueryRuntime Initialization

```text
Scheduler runs QueryRuntimeBuildTask
  -> QueryRuntime.Initialize
  -> GrowingRuntime.Prepare
  -> IDFOracleRuntime.Prepare
  -> QueryRuntime enters Ready
  -> QueryRuntime drains pendingEvents in WAL order
  -> each event is applied to every QueryRuntimeModule
  -> QueryRuntime closes CatchupDone
```

After `Ready`, new live events are still serialized by `QueryRuntime` and
applied to modules in the same module order.

### 5.3 First QueryView Acquire

```text
QueryViewStateMachine.Acquire(qv)
  -> manager registers queryViewRef
  -> manager removes initRef in the same state update
  -> manager waits for QueryRuntime.CatchupDone
  -> manager calls QueryRuntime.Advance(qv.DataVersion)
  -> QueryView may report Up
```

The first QueryView transfers ownership from the load-config initialization
reference to QueryView references.

### 5.4 Later QueryView Acquire

```text
QueryViewStateMachine.Acquire(qv)
  -> manager registers queryViewRef
  -> manager waits for QueryRuntime.CatchupDone
  -> manager computes oldestDataVersion from all queryViewRefs
  -> manager calls QueryRuntime.Advance(oldestDataVersion)
```

`Acquire` never schedules another runtime. A vchannel already has a singleton
runtime.

### 5.5 QueryView Release

```text
QueryViewStateMachine.Release(qv)
  -> manager removes queryViewRef
  -> if queryViewRefs is non-empty:
         manager calls QueryRuntime.Advance(oldestDataVersion)
     else if initRef is absent:
         manager closes QueryRuntime
```

### 5.6 Drop Load Config

```text
RecoveryStorage observes DropLoadConfig
  -> removes VChannelMeta.load_config
  -> StreamingNodeResourceManager.OnDropLoadConfig(event)
  -> manager removes initRef
  -> if queryViewRefs is empty:
         manager closes QueryRuntime
```

`DropLoadConfig` does not directly clean QueryView references and does not
delete QueryView meta.

### 5.7 Crash Recovery

Recovery rebuilds state from WAL metadata:

1. `RecoveryStorage` reads load config and QueryView metadata.
2. For each loaded vchannel, it chooses the recovery base DataVersion:
   - if persisted `Up` QueryViews exist, use the oldest `Up` QueryView
     DataVersion;
   - otherwise use the SegmentModule-provided maximum DataVersion.
3. `RecoveryStorage` builds a valid `VChannelWALView` for the chosen base.
4. `RecoveryStorage` calls `OnAlterLoadConfig(view)`.
5. `QueryViewStateMachine` replays QueryView metadata and calls `Acquire` in
   QueryViewVersion order.

### 5.8 WAL Handoff Close

WAL handoff means this node should release local resources because QueryViews
will be transferred to another node.

The close order is:

```text
PChannelRuntime.CloseForHandoff
  -> QueryViewStateMachine.CloseForHandoff
       -> Release all local QueryView refs
  -> StreamingNodeResourceManager.Close
       -> close remaining initRef-only runtimes
```

Persisted QueryView meta is not deleted by this resource close path. It remains
WAL-bound metadata for the next owner to recover.
