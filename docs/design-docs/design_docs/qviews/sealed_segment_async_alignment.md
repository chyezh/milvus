# QueryNode Sealed Segment Async Alignment Design

> Future QueryNode-side resource alignment after Stage 1 sealed segment load.
> References: [QueryNode Sealed Segment Load Design](sealed_segment_load.md), [Distributed Query View Design](README.md), [DataView Design](data_view.md), [view.proto](../../../../pkg/proto/view.proto).

## 1. Goal

Stage 1 QueryView loading makes a segment queryable with the metadata snapshot
available during acquire. This document describes later alignment work that
does not block the current QueryView Ready transition:

1. watching DataCoord-owned readiness changes for indexes and stats;
2. asynchronously loading newly ready index files;
3. reopening or realigning local segment state for stats/index changes;
4. cleaning up redundant resources after overlapping QueryViews no longer need
   them.

These mechanisms must not drive QueryView state transitions directly. They
only improve local segment resources after the QueryView is already queryable
or prepare resources for future QueryViews.

## 2. Requirements

1. Missing or not-ready index metadata in the Stage 1 snapshot does not block
   the current QueryView.
2. QueryNode can register node-level interests for future index/stats readiness.
3. DataCoord owns readiness and invalidation semantics for index, stats, and
   segment lifecycle metadata.
4. Async callbacks are generation-checked. Stale callbacks are ignored.
5. Async alignment failures do not make an already Ready QueryView
   Unrecoverable unless the physical segment itself becomes invalid.
6. Cleanup must preserve the union of resources required by live QueryViews.

## 3. Architecture

```
ViewAwareSealedSegmentManager
        |
        | register interests for nonblocking resources
        v
NodeLevelReadinessWatcher
        |
        | bidirectional stream
        v
DataCoord Watcher Service

NodeLevelReadinessWatcher
        |
        | ready / invalid callbacks
        v
AsyncAlignmentRunner
        |
        |-- segments.Loader.LoadIndex
        |-- segments.Loader.ReopenSegments
        `-- cleanup after ref/resource diff
```

The watcher is a QueryNode node-level component. It is not scoped to one
collection, shard, segment, or QueryView. Callers register interests, the
watcher deduplicates them, reconnects when the stream breaks, and routes
readiness or invalidation events back to the owner.

## 4. Interest Model

```
RegisterInterest {
    interest_id
    collection_id
    segment_id
    field_id
    expected_index_id
    target_kind
    generation
}

UnregisterInterest {
    interest_id
}
```

`target_kind` distinguishes index readiness, stats readiness, and future
segment-level alignment targets. The interest key should include `field_id` and
`expected_index_id` when an index target is known. If the target changes, the
caller registers a new generation and stale events are ignored.

## 5. Event Model

```
InterestReady {
    interest_id
    collection_id
    segment_id
    target_kind
    generation
    ready_payload
}

InterestInvalid {
    interest_id
    generation
    reason
}
```

For index readiness, `ready_payload` should carry enough segment-level index
metadata for QueryNode to call `LoadIndex` without another metadata lookup. For
stats readiness, it should carry enough metadata to build the reopen or stats
alignment load info.

`InterestInvalid` is used for dropped segment, dropped index, dropped
collection, compacted-away segment, or unrecoverable watch target changes.

## 6. Stream Lifecycle

1. QueryNode opens a bidirectional stream to the DataCoord watcher service.
2. The watcher sends registrations for all active interests.
3. DataCoord sends readiness or invalidation events.
4. On stream break, QueryNode reconnects and re-registers active interests.
5. Callbacks carry `interest_id` and `generation`; stale callbacks are ignored.

The watcher does not report to Coord and does not advance QueryView states. It
only schedules local alignment work.

## 7. Async Alignment Flow

When a readiness event arrives:

1. Resolve the owning segment state and validate generation.
2. Check that the physical segment is still loaded and still referenced by at
   least one live QueryView or retained for future alignment.
3. Execute the alignment:
   - load newly ready index files with `LoadIndex`;
   - reopen segments when stats/index metadata requires a `ReopenSegments`
     path;
   - update local loaded-resource bookkeeping after success.
4. Retry recoverable failures according to the alignment retry policy.
5. Drop or unregister the interest when the target becomes invalid or no live
   local reference can consume it.

Async alignment should not make a ready segment disappear from query results.
If the alignment fails, the segment keeps serving through the already loaded
resources and the alignment can retry.

## 8. Settings Diff and Cleanup

The first implementation ignores `optional_partitions` and avoids fine-grained
unload. Future cleanup can use the union of active view requirements:

| Diff | Behavior |
|---|---|
| Newly required field | Load as blocking work for the acquiring view if the view is still Preparing; otherwise schedule async alignment. |
| Newly ready index | Schedule `LoadIndex` through async alignment. |
| Stats metadata changed | Schedule `ReopenSegments` or stats-specific alignment. |
| Removed field or narrowed setting | Keep loaded state while any live QueryView may still need it. |
| Redundant index | Cleanup only after no live view requires the resource and cleanup cost is justified. |

Cleanup must be conservative. Releasing a resource required by another active
view violates the QueryView multi-version resource-lifetime invariant.

## 9. DataCoord Ownership

The watcher service belongs to DataCoord semantics. DataCoord owns:

1. index build readiness;
2. stats readiness;
3. segment lifecycle invalidation such as drop or compaction replacement.

QueryCoord does not forward index-ready or stats-ready signals and does not
participate in watcher routing. QueryNode may access the service through an
existing MixCoord client path, but the protocol owner is DataCoord.

## 10. Failure Handling

| Scenario | Behavior |
|---|---|
| Metadata snapshot says index is not ready | Register watcher interest and continue Stage 1 Ready. |
| Watch stream breaks | Reconnect and re-register active interests. |
| Watch event generation is stale | Ignore. |
| Watch target invalidated | Cancel async alignment for that target. |
| Async index load fails after Ready | Keep segment queryable on existing resources and retry if recoverable. |
| Async stats/reopen fails after Ready | Keep segment queryable on existing resources and retry if recoverable. |
| Physical segment is dropped or compacted away | Stop local alignment; QueryView replacement is handled by higher-level DataView/QueryView generation. |

## 11. Invariants

1. Async alignment never blocks the Stage 1 Ready transition.
2. Watcher callbacks are generation-checked.
3. DataCoord owns readiness and invalidation semantics.
4. QueryCoord and QueryView syncer do not route watcher events.
5. Already Ready QueryViews stay queryable on existing loaded resources while
   async alignment is retried.
6. Cleanup never removes resources still required by a live QueryView.

## 12. Open Follow-Ups

1. Define the exact DataCoord watcher proto messages and service name.
2. Decide whether stats readiness uses the same watcher interest type as index
   readiness or a separate target kind.
3. Define metadata cache invalidation rules on QueryNode.
4. Define retry and backoff policy for async alignment failures.
5. Define when redundant index cleanup is worth doing versus waiting for
   physical segment release.
