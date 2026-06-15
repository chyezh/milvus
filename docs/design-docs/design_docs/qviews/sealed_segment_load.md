# QueryNode Sealed Segment Load Design

> Stage 1 QueryNode-side sealed segment acquisition for QueryView.
> References: [Distributed Query View Design](README.md), [QueryViewHandler Design](query_view_handler.md), [TransformLog View Module](../wal/transform_log_view_module.md), [view.proto](../../../../pkg/proto/view.proto).
> Future async index/stats alignment is described in [QueryNode Sealed Segment Async Alignment](sealed_segment_async_alignment.md).

## 1. Goal

QueryView moves sealed segment ownership to QueryNode. When Coord pushes a
Preparing QueryView, QueryNode must:

1. verify that the local TransformLog buffer can cover the QueryView transform
   start point before starting any physical segment load;
2. ensure each assigned sealed segment reaches the local `Loaded` state;
3. register each physically loaded segment with the local TransformLog buffer;
4. wait until that segment catches up to the registration tail;
5. report per-partition `ready_segment_ids` incrementally.

This document covers the first implementation path only. DataCoord readiness
watching, async index/stats alignment, and redundant resource cleanup are
outside Stage 1.

The central model is a per-segment lifecycle:

```text
NotLoad -> Loading -> TransformCatchingUp -> Loaded -> Released
```

`Released` is not a retained steady state. After release completes, the segment
state is removed from the local store; a future request observes the segment as
`NotLoad` again.

## 2. Module Relationship

### 2.1 Main Chain

```text
Coord
  |
  | SyncQueryView(Preparing / Dropped)
  v
ViewSyncServer
  |
  v
QN QueryViewHandler / qnShardView / QNStateMachine
  |
  | qnview.SegmentManager.Acquire / Release
  v
TransformAwareSegmentManager
  |
  | first: TransformLogBuffer.Acquire(QueryView)
  | then: CollectionRuntimeManager.Acquire(QueryView)
  | then: SegmentLifecycleManager.Ensure for each segment
  v
SegmentLifecycleManager
  |
  |-- SegmentStateStore
  |-- SegmentLoadScheduler
  `-- TransformLogBuffer.RegisterSegment
          |
          v
      TransformSegment
```

The key boundaries are:

1. `TransformAwareSegmentManager` is the QueryView-level wrapper. It acquires
   QueryView-scoped guards first: TransformLog buffer range and collection
   runtime. If either fails, it reports the failure through the handler
   callback path before any segment load task is submitted.
2. `SegmentLifecycleManager` owns per-segment state transitions, refs, waiter
   notification, TransformLog registration, catch-up, and segment-owned
   physical release.
3. `SegmentLoadScheduler` owns physical load preparation and execution:
   metadata snapshot fetch, load-info build, resource admission, concurrency,
   and best-effort cancellation. It does not decide QueryView readiness.
4. `QNStateMachine` only sees the asynchronous callbacks defined by
   [QueryViewHandler Design](query_view_handler.md).

### 2.2 External Dependencies

```text
MetadataProvider
  |-- MixCoord / RootCoord: DescribeCollection, database/load metadata
  `-- DataCoord: GetSegmentInfo, GetIndexInfo, ListIndexes

SegmentLoadScheduler
  |-- MetadataProvider
  |-- SegmentResourceEstimator
  `-- PhysicalSegmentLoader.Load

CollectionRuntimeManager
  `-- segcore.CCollection

TransformLogBuffer
  `-- StreamingNode TransformLog subscription
```

## 3. Module Responsibilities

| Module | Owns | Does not own |
|---|---|---|
| `ViewSyncServer` | gRPC stream receive/send, converting Coord pushes to handler calls, sending reports back to Coord. | Resource loading, metadata fetch, transform apply. |
| `QN QueryViewHandler` / `qnShardView` | Per-shard QueryView state machine lifecycle, callback replacement, invoking `SegmentManager`. | Segment metadata, physical load, transform subscription. |
| `QNStateMachine` | `Preparing -> Ready/Unrecoverable -> Dropping -> Dropped`, `ready_segment_ids` accumulation. | I/O, retry, physical resource ownership. |
| `TransformAwareSegmentManager` | QueryView-level TransformLog guard acquisition, collection runtime guard acquisition, guard release, dispatching each assigned segment to `SegmentLifecycleManager`. | Physical segment load, metadata fetch, segment ref-count internals. |
| `SegmentLifecycleManager` | Segment state machine, segment refs, waiter notification, TransformLog registration/catch-up, invoking segment-owned release after ref count reaches zero. | QueryView state-machine transitions, upstream TransformLog subscription implementation, physical load admission. |
| `SegmentStateStore` | In-memory `segmentID -> SegmentState` and `QueryViewKey -> segment refs` for resource release. | QueryView state-machine idempotency, callback replacement, network calls, loader calls. |
| `SegmentLoadScheduler` | Segment-level load queueing/concurrency, metadata snapshot fetch, single-segment load-info build, resource estimation/reservation, physical load execution, best-effort cancellation. | TransformLog registration/catch-up and QueryView ready reporting. |
| `MetadataProvider` | Snapshot reads needed to build one segment load request. | Watching future metadata changes. |
| `SegmentResourceEstimator` | Estimate and reserve memory/disk resources for one physical segment load task. | Segment lifecycle state transitions, physical file loading. |
| `PhysicalSegmentLoader` | Loading one sealed segment and its segment-owned PK candidate/BloomFilterSet from a prepared `SegmentLoadInfo` and borrowed collection runtime. | Segment lifecycle state transitions, resource admission, TransformLog catch-up, segment-owned release. |
| `CollectionRuntimeManager` | QueryView-scoped `segcore.CCollection` guard lifecycle for loading and query plan construction. | Segment lifecycle state transitions, physical segment ownership. |
| `TransformLogBuffer` | Vchannel-level transform subscription, local retained buffer, QueryView guard range pins, segment-consumer registration, catch-up notification. | Physical sealed segment lifetime. |
| `TransformSegment` | Segment-side transform consumer interface that applies TransformLog entries, exposes apply progress, owns physical release, and uses its PK candidate/BloomFilterSet to prefilter Delete entries. | Buffer subscription and QueryView reporting. |

## 4. Interfaces

### 4.1 Handler-Visible SegmentManager

`qnview.SegmentManager` remains the only resource interface visible to the QN
handler:

```go
type SegmentManager interface {
    Acquire(req AcquireSegments)
    Release(req ReleaseSegments)
}

type AcquireSegments struct {
    Key  qviews.QueryViewKey
    Meta *viewpb.QueryViewMeta
    View *viewpb.QueryViewOfQueryNode

    OnReady         func(readySegments map[int64][]int64)
    OnUnrecoverable func()
}

type ReleaseSegments struct {
    Key       qviews.QueryViewKey
    OnDropped func()
}
```

`AcquireSegments` carries the full QueryView context because `Meta` owns the
vchannel and transform start point, while `View` owns the partition-to-segment
assignment. Duplicate QueryViewKey handling is owned by the outer state machine;
the asynchronous callback and liveness contracts for each issued operation are
defined in [QueryViewHandler Design](query_view_handler.md).

### 4.2 Transform-Aware Wrapper

```go
type TransformAwareSegmentManager struct {
    lifecycle   SegmentLifecycleManager
    buffer      TransformLogBuffer
    collections CollectionRuntimeManager

    transformGuards  map[qviews.QueryViewKey]TransformLogGuard
    collectionGuards map[qviews.QueryViewKey]CollectionRuntimeGuard
}
```

`transformGuards` and `collectionGuards` are resource handle indexes used only
to release QueryView-scoped resources. They are not a QueryView state machine,
do not handle duplicate Coord pushes, and do not replace callbacks.

On `Acquire`, this wrapper:

1. builds `qviews.QueryViewAtQueryNode` from `Meta + View`;
2. calls `TransformLogBuffer.Acquire` first;
3. reports the QueryView as unrecoverable through the request callback if the
   buffer cannot serve the QueryView transform start point;
4. calls `CollectionRuntimeManager.Acquire` and stores the returned guard under
   `QueryViewKey`;
5. calls `SegmentLifecycleManager.Ensure` for every assigned segment, passing
   the borrowed collection runtime;
6. reports ready with an empty map when the assigned segment set is empty.

On `Release`, it:

1. calls `SegmentLifecycleManager.ReleaseView`;
2. releases the QueryView TransformLog guard;
3. releases the QueryView collection runtime guard;
4. completes the request callback after local refs and guards are released.

### 4.3 TransformLogBuffer

```go
type TransformLogBuffer interface {
    Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (TransformLogGuard, error)
    RegisterSegment(ctx context.Context, segment TransformSegment) (TransformRegistration, error)
}

type TransformLogGuard interface {
    Release()
}

type TransformRegistration interface {
    WaitCatchup(ctx context.Context) error
    Unregister()
}

type TransformSegment interface {
    ID() int64
    VChannel() string
    PartitionID() int64
    TransformStartAfterTimeTick() uint64
    ApplyTransform(ctx context.Context, entry *streamingpb.TransformLogEntry) error
    AppliedTransformTimeTick() uint64
    Release(ctx context.Context) error
}
```

`Acquire` is QueryView-scoped. It creates or reuses a local vchannel buffer and
returns a guard. Each guard pins a local buffer range whose
`StartFromTimeTick` is the QueryView transform start point. The local retained
window is controlled by the minimum `StartFromTimeTick` across all live guards.

`RegisterSegment` is segment-scoped. It does not create an upstream
subscription. It finds the local buffer by `segment.VChannel()`, verifies that
the retained range covers `segment.TransformStartAfterTimeTick()`, replays
buffered entries after that tick, continues live delivery, and exposes
`WaitCatchup`.

During `ApplyTransform`, Delete entries are filtered conservatively. The
segment first filters entries by partition, then uses its loaded PK candidate
or BloomFilterSet as a maybe-hit filter before invoking the underlying
`CSegment` Delete interface. Keys that are definitely absent are skipped; keys
that may exist are passed to Delete. If the candidate is absent, not
initialized, disabled, or cannot answer a batch safely, the segment must treat
all relevant keys as maybe-hit and call Delete. BloomFilter false positives are
acceptable; false negatives must not cause a Delete to be dropped.

`Release` is owned by the loaded segment object. It releases physical segment
resources and must be safe to call more than once as object-level cleanup
defense. This is not QueryViewKey idempotency.

### 4.4 SegmentLifecycleManager

```go
type SegmentLifecycleManager interface {
    Ensure(ctx context.Context, req EnsureSegmentRequest)
    ReleaseView(ctx context.Context, key qviews.QueryViewKey) error
}

type EnsureSegmentRequest struct {
    Key         qviews.QueryViewKey
    Meta        *viewpb.QueryViewMeta
    SegmentID   int64
    PartitionID int64
    Collection  CollectionRuntime

    OnReady         func(partitionID int64, segmentID int64)
    OnUnrecoverable func(error)
}
```

The lifecycle manager is the only module that mutates `SegmentState`. It owns
state transitions and ref accounting. `OnReady` and `OnUnrecoverable` are
operation callbacks supplied by the current `SegmentManager.Acquire` invocation;
they are segment waiters, not QueryView state-machine callbacks. For a new
QueryView ref:

1. if the segment is `Loaded`, it schedules a ready report for the caller;
2. if the segment is `Loading` or `TransformCatchingUp`, it only adds a waiter;
3. if the segment is `NotLoad`, it moves to `Loading` and submits a load task;
4. if the segment is absent because it was released, that is equivalent to
   `NotLoad`.

### 4.5 Segment State

```go
type SegmentLoadState int

const (
    SegmentStateNotLoad SegmentLoadState = iota
    SegmentStateLoading
    SegmentStateTransformCatchingUp
    SegmentStateLoaded
)

type SegmentState struct {
    SegmentID   int64
    PartitionID int64
    VChannel    string
    State       SegmentLoadState

    ViewRefs map[qviews.QueryViewKey]SegmentRef
    Waiters  map[qviews.QueryViewKey]SegmentWaiter

    Segment      TransformSegment
    Registration TransformRegistration
}
```

`Released` is represented by deleting the `SegmentState` after unregistering
the TransformLog consumer and calling `TransformSegment.Release`. The next
`Ensure` recreates the state from `NotLoad`.

`ViewRefs` and the reverse `QueryViewKey -> segment refs` index are resource
ownership records only. They allow `ReleaseView` to detach the segments that a
view operation attached, but they do not implement QueryViewKey idempotency.

### 4.6 SegmentLoadScheduler

```go
type SegmentLoadScheduler interface {
    Submit(task SegmentLoadTask)
    Cancel(segmentID int64)
}

type SegmentLoadTask struct {
    SegmentID   int64
    PartitionID int64
    VChannel    string
    Collection  CollectionRuntime

    TransformStartAfterTimeTick uint64

    OnLoaded        func(segment TransformSegment)
    OnUnrecoverable func(error)
}
```

The scheduler owns physical load execution and admission only:

1. queueing, backpressure, and segment-level concurrency;
2. metadata snapshot reads and single-segment `querypb.SegmentLoadInfo` build;
3. resource estimation, reservation, and reservation release;
4. physical load execution through `PhysicalSegmentLoader`;
5. best-effort cancellation when no QueryView still references the
   segment.

`Submit` does not return an error. Scheduler failures, including metadata
fetch failure, resource rejection, queue shutdown, and physical load failure,
are reported asynchronously through `task.OnUnrecoverable`.

`Cancel` is best-effort. Correctness does not depend on a canceled task stopping
before it calls back; `SegmentLifecycleManager` must still validate current
segment state when late success or failure callbacks arrive.

`SegmentLifecycleManager` guarantees one submitted task per live
`SegmentState`. The scheduler may defensively deduplicate by segment ID, but it
does not own segment waiters or QueryView readiness.

### 4.7 SegmentResourceEstimator

```go
type SegmentResourceEstimator interface {
    Reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (ResourceReservation, error)
}

type ResourceReservation interface {
    Release()
}
```

The estimator owns the resource prediction and pending-resource accounting
needed before physical load. Stage 1 should extract the resource-estimation
logic from the existing QueryNode loader, while keeping reservation state in the
scheduler. A reservation must be released after physical load finishes or
fails.

### 4.8 CollectionRuntimeManager

```go
type CollectionRuntimeManager interface {
    Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, error)
}

type CollectionRuntimeGuard interface {
    CollectionRuntime
    Release()
}

type CollectionRuntime interface {
    CollectionID() int64
    DatabaseName() string
    Schema() *schemapb.CollectionSchema
    SchemaVersion() int64
    CCollection() *segcore.CCollection
}
```

`CollectionRuntimeManager` owns QueryView-scoped references to collection
runtime. The `segcore.CCollection` is used both by sealed segment loading and
by query-time search/retrieve plan construction, so its lifetime is pinned by
the QueryView rather than by individual segments. `SegmentLifecycleManager`,
`SegmentLoadScheduler`, and `PhysicalSegmentLoader` borrow
`CollectionRuntime`; they must not release it.

### 4.9 MetadataProvider

```go
type MetadataProvider interface {
    DescribeCollection(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
    GetSegmentInfo(ctx context.Context, segmentIDs ...int64) ([]*datapb.SegmentInfo, error)
    ListIndexes(ctx context.Context, collectionID int64) ([]*indexpb.IndexInfo, error)
    GetIndexInfo(ctx context.Context, collectionID int64, segmentIDs ...int64) (map[int64][]*querypb.FieldIndexInfo, error)
}
```

Stage 1 uses this interface as a snapshot fetch only. It does not subscribe to
future index or stats readiness changes.

### 4.10 PhysicalSegmentLoader

```go
type PhysicalSegmentLoader interface {
    Load(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error)
}
```

`PhysicalSegmentLoader` is a QV-specific sealed segment loader. It may reuse
loading utility functions extracted from the old QueryNode loader, but it must
not call the old `segments.Loader.Load` entrypoint, put segments into the old
`segments.Manager`, or depend on `LocalSegment` lifecycle ownership. It borrows
the QueryView-pinned collection runtime and does not release it.

The returned `TransformSegment` must already own the PK candidate used by
TransformLog Delete prefiltering. For a regular sealed segment, the physical
loader loads the BloomFilterSet from statslog paths in `SegmentLoadInfo` after
the base segment data and historical deltalogs are loaded. For an external
collection or a configuration where BloomFilter is disabled, the loader must
install an explicit safe candidate or no-prefilter mode whose behavior is
equivalent to "every relevant PK may exist". A BloomFilter load error is a
segment load error unless this explicit no-prefilter mode was selected before
the attempt.

## 5. Segment Lifecycle

### 5.1 NotLoad -> Loading

Triggered by the first `Ensure` for a segment that has no local state.

Actions:

1. create `SegmentState`;
2. add the QueryView ref and waiter;
3. submit one scheduler task for the segment;
4. move the state to `Loading`.

Any later `Ensure` while loading only adds refs and waiters. It must not submit
another physical load task.

### 5.2 Loading -> TransformCatchingUp

Triggered by scheduler load success.

Actions:

1. store the returned `TransformSegment`;
2. call `TransformLogBuffer.RegisterSegment(segment)`;
3. store the returned `TransformRegistration`;
4. move the state to `TransformCatchingUp`;
5. wait for catch-up asynchronously.

Load failure is fatal for all waiting QueryViews for that segment.

### 5.3 TransformCatchingUp -> Loaded

Triggered by `TransformRegistration.WaitCatchup` success.

Actions:

1. move the state to `Loaded`;
2. notify all waiters for the segment;
3. keep the registration alive for future TransformLog delivery.

Catch-up failure is fatal for all waiting QueryViews for that segment.

### 5.4 Loaded

`Loaded` means the segment is physically loaded, registered with
TransformLogBuffer, caught up to the registration tail, and safe to report as
ready for any QueryView whose ref is attached to this state.

New `Ensure` calls for a loaded segment enqueue a ready report for that
QueryView.

### 5.5 Loaded -> Released -> NotLoad

Triggered when `ReleaseView` removes the last QueryView ref for a segment.

Actions:

1. unregister the TransformLog registration;
2. call `TransformSegment.Release`;
3. delete the `SegmentState`.

After deletion, the next request observes the segment as `NotLoad`.

## 6. QueryView Acquire Flow

```text
QueryViewHandler
  -> TransformAwareSegmentManager.Acquire(req)
       -> submit asynchronous acquire operation

Acquire operation
  -> TransformLogBuffer.Acquire(QueryViewAtQueryNode)
     | fail: report unrecoverable, stop
     | ok: store TransformLogGuard
  -> CollectionRuntimeManager.Acquire(QueryViewAtQueryNode)
     | fail: release TransformLogGuard, report unrecoverable, stop
     | ok: store CollectionRuntimeGuard
  -> if no assigned segment:
       -> report ready(empty map)
  -> for each assigned segment:
       -> SegmentLifecycleManager.Ensure(segment ref, borrowed collection runtime)

SegmentLifecycleManager.Ensure
  -> Loaded:
       -> report ready(partitionID, segmentID)
  -> Loading / TransformCatchingUp:
       -> add waiter
  -> NotLoad:
       -> create state and waiter
       -> SegmentLoadScheduler.Submit(segment task)

SegmentLoadScheduler task
  -> MetadataProvider snapshot reads
  -> build one SegmentLoadInfo helper
  -> SegmentResourceEstimator.Reserve
  -> PhysicalSegmentLoader.Load
       -> load base sealed segment data
       -> load historical deltalogs
       -> load segment-owned PK candidate/BloomFilterSet for Delete prefiltering
  -> ResourceReservation.Release
  -> SegmentLifecycleManager.OnPhysicalLoaded

SegmentLifecycleManager after load callback
  -> TransformLogBuffer.RegisterSegment
  -> TransformRegistration.WaitCatchup
  -> report ready(partitionID, segmentID)
```

Rules:

1. `Acquire` itself does not run the load flow inline; it submits an
   asynchronous acquire operation and returns.
2. TransformLog buffer acquisition is the first resource operation for a
   QueryView.
3. Collection runtime acquisition is the second QueryView-scoped resource
   operation. It must succeed before segment load tasks are submitted.
4. A TransformLog or collection-runtime acquire failure prevents all physical
   segment load work for that QueryView.
5. Segment load and TransformLog catch-up are independent per segment.
6. A slow segment must not block another segment from registering and reporting
   ready.
7. QN state machine transitions to Ready only after all assigned segments have
   been reported ready.
8. Multiple segment failures in the same acquire operation are collapsed before
   invoking the operation's unrecoverable callback.
9. Scheduler load failures are delivered asynchronously. `Submit` does not
   synchronously fail the acquire operation.

All ready, unrecoverable, and dropped notifications in this flow are delivered
through the operation callback contract defined by
[QueryViewHandler Design](query_view_handler.md).

## 7. QueryView Release Flow

```text
QueryViewHandler
  -> TransformAwareSegmentManager.Release(req)
       -> submit asynchronous release operation

Release operation
  -> SegmentLifecycleManager.ReleaseView(key)
       -> remove this QueryView ref from every segment
       -> for each segment whose ref count becomes zero:
            -> registration.Unregister
            -> TransformSegment.Release
            -> delete SegmentState
  -> TransformLogGuard.Release if a guard exists for key
  -> CollectionRuntimeGuard.Release if a guard exists for key
  -> complete dropped callback
```

Rules:

1. `Release` itself does not run cleanup inline; it submits an asynchronous
   release operation and returns.
2. Segment state is removed after the last ref releases and physical cleanup
   finishes; removed state is equivalent to `NotLoad`.
3. Guard release only affects TransformLog buffer retention. It must not depend
   on segment state.
4. Collection runtime guard release only affects QueryView-level
   `segcore.CCollection` lifetime. It must happen after segment refs are
   released.
5. Segment unregister removes the segment consumer from live TransformLog
   delivery.
6. Missing segment refs, missing TransformLog guard, or missing collection
   runtime guard are valid no-op cleanup cases for a release operation; the
   dropped callback is still completed.

## 8. Ready Semantics

A segment can be reported in `ready_segment_ids` only after all of the
following are true:

1. the QueryView-level TransformLog guard has been acquired;
2. the QueryView-level collection runtime guard has been acquired;
3. the physical sealed segment is loaded;
4. required fields and blocking index files selected for Stage 1 are loaded;
5. historical deltalogs are loaded;
6. the segment-owned PK candidate/BloomFilterSet needed for TransformLog Delete
   prefiltering is loaded, or an explicit no-prefilter safe mode is active;
7. the segment is registered with TransformLogBuffer;
8. the registration has caught up to the TransformLogBuffer tail observed at
   registration time.

Ready is reported incrementally at segment granularity. The QN state machine
transitions from Preparing to Ready only when all assigned segments have been
reported ready.

The following do not block Stage 1 Ready:

1. `optional_partitions`;
2. indexes that are missing or not ready in the metadata snapshot;
3. future index build completion after the segment is already Ready;
4. async stats/index reopen;
5. redundant field/index cleanup.

## 9. Failure Handling

| Scenario | Behavior |
|---|---|
| `TransformLogBuffer.Acquire` cannot serve the QueryView start point | Report unrecoverable through the handler callback path; no segment load task is submitted for that QueryView. |
| `CollectionRuntimeManager.Acquire` fails | Release the TransformLog guard, report unrecoverable through the handler callback path, and submit no segment load task. |
| Metadata fetch fails in a load task | Fail all waiters attached to that segment, then remove or reset only that segment state to `NotLoad` after cleanup. |
| Resource estimation or reservation rejects a load task | Fail all waiters attached to that segment; no physical load is started. |
| Required segment metadata is missing | Fail all waiters attached to that segment. |
| Physical segment load fails | Fail all waiters attached to that segment and release partial resources for that segment. |
| PK candidate/BloomFilterSet load fails | Fail all waiters attached to that segment unless no-prefilter safe mode was explicitly selected before loading. |
| Transform registration fails | Fail all waiters attached to that segment and call `TransformSegment.Release` if no refs remain. |
| Segment catch-up fails before Ready | Fail all waiters attached to that segment and call `TransformSegment.Release` if no refs remain. |
| QueryView is dropped while segment is `Loading` or `TransformCatchingUp` | Remove that QueryView waiter/ref; cancel or cleanup the segment only if no refs remain. |
| Transform stream breaks after a segment is `Loaded` | Buffer reconnects and resumes if the required range is still available; not-yet-ready waiters fail if catch-up cannot complete. |

Segment-level failures are isolated to the failed segment. If a QueryView is
reported unrecoverable because one segment failed, the lifecycle manager must
not detach that QueryView from its other successfully loaded or still-loading
segments in the failure path. Those resources remain shareable by other views
and are released only through `ReleaseView` or their own segment-level failure.

## 10. Invariants

1. QueryView does not carry QueryNode load metadata.
2. `qnview.SegmentManager` is the only resource interface visible to
   `QueryViewHandler`.
3. TransformLog `Acquire` is the first QueryView-scoped resource operation.
4. Collection runtime `Acquire` is the second QueryView-scoped resource
   operation and pins `segcore.CCollection` for both loading and query-time plan
   construction.
5. No physical load task is submitted if either QueryView-scoped resource
   acquire fails.
6. There is at most one physical load task per segment state.
7. Segment readiness is independent. One loaded/caught-up segment can be
   reported before other assigned segments finish loading.
8. `SegmentLoadScheduler` does not own TransformLog registration or QueryView
   readiness.
9. `Released` state is not retained. Cleanup deletes local state, and future
   acquire starts from `NotLoad`.
10. Physical segment lifetime is at least the union of all local QueryView refs.
11. `CCollection` lifetime is pinned by QueryView collection runtime guards,
    not by individual segment refs.
12. All Delete delivery for a loaded segment uses the same registered
    `TransformSegment` consumer path.
13. Delete prefiltering is conservative. PK candidate or BloomFilter absence,
    uncertainty, or disabled state must result in calling Delete for the
    relevant keys, not skipping them.
14. Segment failure cleanup is segment-scoped. It must not release other
    segment resources held by the same QueryView.
15. Segment content/index/stats changes after a DataView joins a QueryView do
    not advance QueryViewVersion; they are handled by local reload/reopen paths.

## 11. Stage Boundary

Stage 1 implements:

1. QN handler to `SegmentManager` request shape with full QN view context.
2. QueryView-level TransformLog guard and collection runtime guard acquisition
   with fast fail before physical load.
3. Segment lifecycle state machine:
   `NotLoad -> Loading -> TransformCatchingUp -> Loaded -> Released -> NotLoad`.
4. Segment-level scheduler for physical load concurrency, metadata snapshot
   fetch, load-info build, and resource admission.
5. Resource estimation extracted from the existing QueryNode loader, with
   reservation state owned by the scheduler.
6. QueryView-scoped collection runtime guard for `segcore.CCollection`.
7. Physical segment load/release adapter, including segment-owned PK
   candidate/BloomFilterSet loading for TransformLog Delete prefiltering.
8. Incremental ready reporting after each segment catches up.

Future stages are described in
[QueryNode Sealed Segment Async Alignment](sealed_segment_async_alignment.md):

1. DataCoord readiness watcher.
2. Async index and stats alignment after a view is Ready.
3. Reopen and redundant resource cleanup.
4. Retry/backoff and stale callback generation handling.

## 12. Open Follow-Ups

1. Decide whether the proto field `delete_apply_start_after_timetick` is renamed
   to `transform_start_after_timetick` before Stage 1 implementation or only
   documented as an alias during migration.
2. Define the first scheduler resource policy. Stage 1 can start with a
   QueryNode-level semaphore and later add per-vchannel or resource-group
   controls.
3. Define the load-task dedup key. Stage 1 likely uses `segmentID`; future
   settings-aware reload may need `segmentID + required resources`.
4. Decide whether collection metadata and index metadata should be cached per
   QueryView or fetched inside each segment task.
5. Define the exact metadata provider implementation on QueryNode and whether
   it talks to MixCoord directly or through an existing client wrapper.
6. Define how in-flight physical load tasks protect borrowed
   `CollectionRuntime` when the QueryView is released. Candidate solutions are
   a task-scoped temporary collection runtime pin, or making release wait until
   scheduler callbacks for that QueryView's submitted tasks have completed.
