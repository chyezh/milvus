# QueryNode QueryView Resource Preparation Design

> References: [Distributed Query View Design](../README.md),
> [QueryView State Machine Per-Node Analysis](../query_view_state_machine.md),
> [QueryView Handler Design](../query_view_handler.md),
> [view.proto](../../../../../pkg/proto/view.proto), and
> [query_coord.proto](../../../../../pkg/proto/query_coord.proto).

## 1. Goal and Scope

This document describes how QueryNode prepares local resources for a QueryView
in `Preparing` state and reports resource readiness back to QueryCoord.

The resource preparation flow covers:

1. pinning collection runtime needed by the view;
2. loading assigned sealed segments from object storage;
3. registering loaded sealed segments with TransformLog;
4. waiting until each segment catches up to the required transform frontier;
5. reporting incremental segment readiness to the local QN state machine;
6. releasing view-scoped references after the view is dropped.

This document does not cover:

1. QueryCoord view generation, balancing, or global state-machine persistence;
2. StreamingNode growing-side resource preparation;
3. post-Ready asynchronous index/stats alignment;
4. query execution or result reduction.

Post-Ready index/stats resource alignment is intentionally outside this
resource-preparation design.

## 2. Design Principles

1. QueryCoord owns the distributed QueryView state. QueryNode only prepares
   resources for the view portion assigned to the current node.
2. QueryNode resource lifetime is view-scoped. Shared physical segments stay
   alive while any local QueryView references them.
3. Collection-level metadata still comes from `DescribeCollection`.
4. Segment-level load metadata comes from `GetQueryViewSegmentLoadInfo`.
   DataCoord owns the packing of complete `SegmentLoadInfo`; QueryNode does
   not reconstruct it by calling multiple metadata APIs.
5. TransformLog catch-up is part of QueryView readiness. A physically loaded
   sealed segment is not reported Ready until it is registered and caught up.
6. Cancellation is best-effort. Correctness must rely on ref-count validation,
   context cancellation, stale callback checks, and release waiting, not on the
   scheduler being able to forcibly stop object-storage load work.

## 3. Component Layering

The QueryNode entrypoint wires the resource managers as:

```text
QueryNode.NewQueryViewSegmentManager
  -> TransformAwareSegmentManager
       -> ViewAwareSealedSegmentManager
            -> DefaultSegmentLoadScheduler
                 -> MetadataProvider.GetQueryViewSegmentLoadInfo
                 -> PhysicalSegmentLoader.Load
       -> TransformLogBuffer
       -> CollectionRuntimeManager
```

Main components:

| Component | Responsibility | Not Responsible For |
|---|---|---|
| `QNQueryViewHandler` | Applies coord-pushed views, owns per-shard QN state machines, calls `SegmentManager.Acquire/Release`. | Segment loading, metadata fetch, TransformLog catch-up. |
| `TransformAwareSegmentManager` | Pins TransformLog and collection runtime, tracks view/segment transform refs, registers loaded segments, waits catch-up, reports segment Ready. | DataCoord metadata packing, object-storage segment load. |
| `ViewAwareSealedSegmentManager` | Tracks physical segment refs by QueryView, submits missing segment loads, handles late load callbacks and in-flight load completion on release. | TransformLog registration, collection runtime creation. |
| `DefaultSegmentLoadScheduler` | Fetches DataCoord-packed segment load info, updates collection index meta, reserves resources, invokes physical loader. | QueryView state transitions, view-level ref ownership. |
| `MetadataProvider` | Provides collection metadata and DataCoord-packed segment load metadata through MixCoord/QueryCoord. | Watching future index/stats readiness changes. |
| `CollectionRuntimeManager` | Pins local collection runtime with schema/load meta and exposes index meta update for segment load. | Segment ref counting. |
| `TransformLogBuffer` | Pins view-level transform range and registers loaded sealed segments for catch-up. | Persisting QueryView state. |

Current implementation note: the live `TransformSegment.Release` call happens
in `TransformAwareSegmentManager` after transform registration is detached.
`ViewAwareSealedSegmentManager.Release` finalizes physical load bookkeeping,
cancels still-loading last refs, and waits for in-flight load callbacks. This
keeps TransformLog unregister and segment release in one layer.

## 4. Acquire Flow

When QueryNode receives a new `Preparing` QueryView:

1. `QNQueryViewHandler` creates a per-view QN state machine and calls
   `SegmentManager.Acquire`.
2. `TransformAwareSegmentManager.Acquire` clones the request and starts async
   acquisition.
3. It acquires a view-level `TransformLogGuard`.
4. It acquires a `CollectionRuntimeGuard`.
   `CollectionRuntimeManager` uses `DescribeCollection` to fetch collection
   schema and load metadata, then pins the local collection runtime through
   `PutOrRef`.
5. It records the view reference:
   - attaches the view to every assigned segment;
   - records waiters for segments that are not transform-ready;
   - immediately reports already transform-ready segments.
6. If the current QN has no required assigned segments, it reports `OnReady`
   with an empty map so the local QN state machine can advance.
7. For missing segments, it filters the QN view to those segment IDs and calls
   `ViewAwareSealedSegmentManager.Acquire`.
8. The physical manager records physical refs and submits load tasks only for
   segments that are missing or previously reset.
9. Each load task fetches one segment's load metadata, loads it, and reports it
   back to the physical manager.
10. The transform manager receives the physically loaded segment, registers it
    with `TransformLogBuffer`, waits for catch-up, and then reports the segment
    Ready.

`OnReady` is incremental. It carries a `partitionID -> segmentIDs` delta and
may be called multiple times for the same QueryView. The QN state machine
deduplicates segment IDs and transitions to Ready only after all required
assigned segments are ready.

Optional partitions do not block the Ready transition. If
`required_partitions` is set, only those partitions count as required. If it is
not set, every partition except `optional_partitions` counts as required.

## 5. Metadata and RPC Boundary

`qnview.MetadataProvider` has only two metadata methods:

```go
type MetadataProvider interface {
    DescribeCollection(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
    GetQueryViewSegmentLoadInfo(ctx context.Context, collectionID int64, segmentIDs ...int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error)
}
```

The RPC is defined on QueryCoord proto:

```proto
message GetQueryViewSegmentLoadInfoRequest {
    common.MsgBase base = 1;
    int64 collectionID = 2;
    repeated int64 segmentIDs = 3;
}

message GetQueryViewSegmentLoadInfoResponse {
    common.Status status = 1;
    repeated SegmentLoadInfo infos = 2;
    repeated index.IndexInfo index_info_list = 3;
}
```

There is intentionally no `load_priority` in the request. Load priority is not
an input dimension for QueryView resource preparation. DataCoord currently
packs QueryView load info with high priority in the returned `SegmentLoadInfo`
and index params.

The call path is:

```text
QueryNode lazyQueryViewMetadataProvider
  -> MixCoord client
  -> QueryCoord.GetQueryViewSegmentLoadInfo
       -> health check
       -> mixCoord.GetQueryViewSegmentLoadInfo
            -> DataCoord.GetQueryViewSegmentLoadInfo
```

In production deployment QueryCoord and DataCoord are colocated in the
coordinator process, so the MixCoord hop calls the DataCoord server directly
inside the process instead of requiring an extra gRPC round trip between
coordinator sub-services.

DataCoord owns the segment-level packing:

1. validates health, collection ID, and requested segment IDs;
2. returns empty success for an empty segment list;
3. fetches collection-level index definitions once;
4. fetches segment index metadata for requested segment IDs;
5. validates each segment belongs to the requested collection;
6. recalculates row count when needed;
7. packs `querypb.SegmentLoadInfo` with binlogs, deltalogs, stats logs,
   manifest path, index info, storage version, data version, commit timestamp,
   and other loader inputs;
8. returns the collection index info list used by QueryNode to update local
   collection index meta before loading the segment.

The RPC supports multiple segment IDs. The current scheduler still submits load
tasks per segment, so a task usually calls the RPC with one segment ID. The
important boundary is that QueryNode no longer assembles `SegmentLoadInfo` from
multiple metadata calls; DataCoord is the single owner of the complete
segment-level load snapshot.

## 6. Physical Segment Load Flow

`ViewAwareSealedSegmentManager` maintains:

1. `views`: local QueryView refs and callbacks;
2. `segments`: physical segment state by segment ID;
3. `cancels`: view-level cancellation functions.

Each physical segment state stores:

1. loaded `TransformSegment`, if load has completed;
2. partition ID;
3. loading flag and load cancellation function;
4. set of referencing QueryView keys.

Acquire behavior:

1. record or replace the view ref;
2. add the view key to every assigned segment's ref set;
3. create load state only for missing segments;
4. submit load tasks for missing/not-loading segments;
5. if all requested segments are already loaded, call `OnLoaded` with the
   collected segment set.

Load task behavior:

1. call `GetQueryViewSegmentLoadInfo(collectionID, segmentID)`;
2. require exactly one returned `SegmentLoadInfo`;
3. update collection index meta with returned collection index definitions;
4. reserve resources through the optional estimator;
5. call `PhysicalSegmentLoader.Load`;
6. wrap the segment with `TransformStartAfterTimeTick` if the QueryView meta
   has a delete-apply start timetick;
7. call the physical manager's load callback.

On physical load completion, the manager validates that the segment is still
referenced before keeping it. If no view still references the segment, it
releases the loaded segment immediately and drops the stale result.

## 7. Transform Registration and Catch-Up

Physical load alone is not enough for QueryView readiness. After physical load:

1. `TransformAwareSegmentManager` marks the segment as physically loaded.
2. It registers the segment with `TransformLogBuffer`.
3. It stores the registration and catch-up cancellation function.
4. It waits for `TransformRegistration.WaitCatchup`.
5. After catch-up succeeds, it marks the segment transform-loaded and reports
   all waiting QueryViews through `OnReady`.

If a second QueryView references a segment that is already transform-loaded,
the transform manager reports it Ready immediately without reloading or
re-registering the segment.

If catch-up or registration fails, the segment is considered unrecoverable for
the waiting QueryViews:

1. cancel catch-up;
2. unregister from TransformLog if registration exists;
3. release the loaded segment if present;
4. reset the physical segment state through `PhysicalSegmentResetter`;
5. notify affected QueryViews with `OnUnrecoverable`.

Resetting the physical state allows a later QueryView acquire to retry the
segment from the beginning instead of reusing a partially registered segment.

## 8. Release Flow

When Coord pushes a view as `Dropped`, the local QN state machine enters
Dropping and calls `SegmentManager.Release`.

Release behavior:

1. `TransformAwareSegmentManager` detaches the view from transform refs.
2. For segments whose last transform ref is removed, it cancels catch-up,
   unregisters TransformLog, and releases the loaded segment.
3. It calls `ViewAwareSealedSegmentManager.Release` to remove the physical
   view ref.
4. The physical manager cancels still-loading segments only when the released
   view was the last physical ref.
5. It waits for the view's in-flight load callbacks to finish.
6. The transform manager releases the view-level TransformLog guard and
   collection runtime guard.
7. It invokes `OnDropped`, allowing the QN state machine to report Dropped.

The scheduler's `Cancel` method is currently best-effort and no-op in the
default implementation. Release correctness therefore depends on context
cancellation plus callback validation and `loadWG.Wait`, not on synchronous
load termination.

## 9. Failure Semantics

| Failure | Behavior |
|---|---|
| TransformLog guard acquire fails | The view is reported Unrecoverable. |
| Collection runtime acquire fails | The view is reported Unrecoverable and the TransformLog guard is released. |
| Segment metadata RPC fails | The affected segment load fails and waiting views are reported Unrecoverable. |
| RPC returns zero/multiple load infos for one segment task | The segment load is treated as unrecoverable for the waiting views. |
| Collection index meta update fails | The segment load is treated as unrecoverable. |
| Resource reservation fails | The segment load is treated as unrecoverable. |
| Physical loader fails | The segment load is treated as unrecoverable. |
| Transform registration fails | The loaded segment is released, physical state is reset, waiting views are reported Unrecoverable. |
| Transform catch-up fails | The registration is removed, loaded segment is released, physical state is reset, waiting views are reported Unrecoverable. |
| Release races with load completion | Late callback is validated against current refs; unreferenced loaded segment is released and ignored. |
| Repeated acquire for the same QueryView key | Not part of the current handler flow. If future settings diff/reconfigure requires it, the physical manager must explicitly remove segment refs that existed only in the old view. |

Unrecoverable is view-local. QueryNode does not generate replacement views.
QueryCoord observes the report and higher-level view generation/balancing
decides the next view.

## 10. Invariants

1. `Acquire` and `Release` callbacks are always asynchronous.
2. Every `Acquire` must eventually produce `OnReady` or `OnUnrecoverable`.
3. Every `Release` must eventually produce exactly one `OnDropped`.
4. QueryNode reports Ready only for required assigned segments that have
   completed physical load and TransformLog catch-up.
5. Optional partitions do not block the Ready transition.
6. A physical segment load is submitted at most once while a live segment state
   is already loading or loaded.
7. A loaded segment is retained only while at least one local QueryView
   references it.
8. TransformLog registration and segment release happen in the transform-aware
   layer so transform consumption is detached before the segment is released.
9. QueryNode does not assemble `SegmentLoadInfo` from partial metadata APIs.
   DataCoord owns the complete segment-level load snapshot.
10. Collection schema/load metadata and segment load metadata intentionally use
    separate APIs: `DescribeCollection` for collection runtime and
    `GetQueryViewSegmentLoadInfo` for segment load.
11. The current handler issues one acquire per `QueryViewKey`. Same-key view
    diff/reconfigure is a future extension and needs explicit physical ref
    diffing before it can be treated as a resource-lifecycle guarantee.

## 11. Relationship to Other Documents

1. [Distributed Query View Design](../README.md) describes the global QV
   architecture, versioning, and Coord/Node responsibility split.
2. [QueryView State Machine Per-Node Analysis](../query_view_state_machine.md)
   describes per-node state transitions and report behavior.
3. [QueryView Handler Design](../query_view_handler.md) describes handler-level
   application of coord-pushed views.
4. StreamingNode resource preparation is covered by the `snview` documents and
   is intentionally independent from the QueryNode sealed segment flow.
