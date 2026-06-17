# StreamingNode IDF Oracle Runtime Design

> VChannel-level BM25 / IDF resource preparation for StreamingNode QueryView.
> This document defines the resource lifecycle and preparation flow of
> `IDFOracleRuntime`. Query execution, scoring behavior, and query plan format
> are out of scope.

## 1. Purpose

`IDFOracleRuntime` is the vchannel-level singleton resource that prepares and
maintains BM25 statistics used by the StreamingNode IDF oracle.

Unlike growing segment resources, IDF oracle state is not retained per
DataVersion. A vchannel owns at most one `IDFOracleRuntime`. The runtime keeps a
single current oracle and advances it in the background when later QueryViews
need a newer DataVersion.

The purpose of `IDFOracleRuntime` is to:

1. initialize BM25 statistics from the `VChannelWALView` base DataVersion;
2. fetch sealed BM25 resources from QueryCoord for the initialized DataVersion;
3. continuously generate growing-segment BM25 statistics from WAL input;
4. record each flushed growing segment's sealed `DataVersion`;
5. asynchronously advance the current oracle when a later QueryView DataVersion
   is prepared;
6. atomically apply BM25 statistics diffs so readers never observe a partially
   advanced oracle;
7. clean obsolete internal BM25 statistics and sealed cache leases by itself.

The runtime does not select QueryView placement, own QueryView references, build
csegments, read `SegmentModule` directly, or block later QueryView activation on
background IDF advancement.

Although the runtime is a vchannel-level singleton, its lifetime is protected by
the same resource reference model as growing segment resources. The
PChannel-local `StreamingNodeResourceManager` keeps the runtime alive while
either the load-config initialization reference or any QueryView reference
exists.

## 2. Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `StreamingNodeResourceManager` | PChannel-local component that creates the vchannel resource build task from `OnAlterLoadConfig` or recovery. Owns QueryView/init references and closes the whole IDF runtime when the vchannel resource is released or when the PChannel runtime finalizes after QueryView handoff. | It does not compute BM25 stats diffs and does not evict IDF internal segment stats. |
| `VChannelWALView` | Provides the initial schema, settings, segment snapshot, historical insert input, and no-gap live resource event stream. | Its capture and no-gap contract are defined in [StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md). |
| `IDFOracleRuntimeBuilder` | Builds the initial vchannel-level `IDFOracleRuntime` from one `VChannelWALView`. | It does not submit build tasks, manage QueryView references, or advance the oracle for later QueryViews. |
| `IDFOracleRuntime` | Owns the vchannel singleton oracle, growing BM25 stats store, sealed contribution leases, current DataVersion, initial catchup state, and advance worker. | It does not expose external truncation; current-oracle cleanup is internal. |
| `SealedBM25ResourceProvider` | Calls QueryCoord to fetch the complete sealed BM25 resource set for a target DataVersion. | It does not cache local files or merge oracle stats. |
| `SealedBM25SegmentCache` | Downloads, parses, reuses, and leases sealed BM25 stats. | It does not decide DataVersion advancement or contribution membership. |
| `GrowingBM25StatsStore` | Maintains local BM25 stats for growing segments generated from snapshot and live WAL events, plus flushed/sealed metadata. | It does not fetch sealed resources from QueryCoord. |
| `IDFAdvanceWorker` | Serializes asynchronous oracle advancement requests and coalesces them to the largest pending DataVersion. | It is internal to `IDFOracleRuntime` and is not the resource build scheduler. |

## 3. Component Relationships And Invariants

### 3.1 Relationship Model

```text
PChannelRuntime
        |
        | Owns
        v
StreamingNodeResourceManager
        |
        | BuildTask created by OnAlterLoadConfig / recovery
        v
IDFOracleRuntimeBuilder
        |
        | BuildInitial(VChannelWALView)
        v
IDFOracleRuntime
        |                         |
        | sealed resources         | growing WAL/resource events
        v                         v
SealedBM25ResourceProvider   GrowingBM25StatsStore
        |
        v
SealedBM25SegmentCache
```

Initial construction is driven by the normal vchannel resource build task. Later
oracle advancement is driven by QueryView preparation notifications and runs in
the runtime's own serial background worker.

### 3.2 Runtime State

```text
IDFOracleRuntime
  collectionID
  vchannel
  settings
  currentDataVersion
  currentStats
  currentSealedContributions map[segmentID]SealedBM25Contribution
  currentGrowingContributions map[segmentID]GrowingBM25Contribution
  growingStore GrowingBM25StatsStore
  sealedCache SealedBM25SegmentCache
  provider SealedBM25ResourceProvider
  catchupDone
  advanceWorker
  close/cancel
```

`currentDataVersion` describes the sealed/growing contribution boundary of the
current oracle. It is advanced by atomic diff commit. Live growing stats may
continue to update while the sealed baseline stays at the same DataVersion.

### 3.3 Contribution Model

For a target DataVersion `D`, the oracle contribution set is:

```text
ContributionSet(D):
  sealed contributions:
    complete sealed BM25 resource set returned by QueryCoord for D

  growing contributions:
    local growing BM25 stats whose segment is not in the sealed set for D
    and whose sealedAtDataVersion is absent or > D
```

The sealed set always comes from QueryCoord. StreamingNode must not infer sealed
membership for a target DataVersion from local segment metadata alone.

The local `sealedAtDataVersion` is still recorded because it determines when a
growing segment can stop contributing to the oracle and when its local growing
BM25 stats can be removed.

### 3.4 Invariants

1. There is only one `IDFOracleRuntime` per loaded vchannel.
2. There is no `DataVersion -> IDFOracle` map.
3. Initial construction is triggered by `OnAlterLoadConfig` or recovery, not by
   QueryView `Acquire`.
4. The initialized oracle DataVersion is the `VChannelWALView` base
   DataVersion.
5. Initial sealed BM25 resources are fetched from QueryCoord.
6. Initial growing BM25 stats are generated from the WALView segment snapshot
   and the no-gap live resource stream.
7. The first QueryView `Up` report must wait for `IDFOracleRuntime` initial
   catchup.
8. Later QueryView preparation may notify the runtime of a target DataVersion,
   but it does not wait for IDF advancement to finish.
9. IDF advancement is vchannel-local, serial, asynchronous, and monotonic by
   target DataVersion.
10. BM25 stats diff is computed outside the commit path.
11. The current oracle is changed only by one atomic diff commit.
12. The runtime owns cleanup of obsolete growing stats, sealed leases, and
    abandoned advance-task resources.
13. The PChannel-local `StreamingNodeResourceManager` protects the whole runtime
    with the same `initRef + queryViewRefs` reference model used for other
    vchannel resources.
14. The PChannel-local `StreamingNodeResourceManager` only closes the whole
    runtime; it does not call a truncate or eviction method on IDF internals.
15. A valid live event that cannot be applied is a critical StreamingNode
    corruption, not a recoverable QueryView resource condition.

## 4. Interface Description

### 4.1 IDFOracleRuntimeBuilder

```go
type IDFOracleRuntimeBuilder interface {
    BuildInitial(
        ctx context.Context,
        view walview.VChannelWALView,
    ) (IDFOracleRuntime, error)
}
```

`BuildInitial` creates the vchannel singleton runtime for the WALView base
DataVersion. It fetches the initial sealed resources, initializes the growing
stats store from the WALView segment snapshot, and starts the ordered live event
apply path.

The builder is not a scheduler. Runtime build concurrency is still controlled by
the resource build scheduler described in
[StreamingNode Growing Segment Runtime Design](growing_segment_runtime.md).

### 4.2 IDFOracleRuntime

```go
type IDFOracleRuntime interface {
    CollectionID() int64
    VChannel() string

    CurrentDataVersion() qviews.DataVersion

    CatchupDone() <-chan struct{}
    CatchupError() error

    ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent)

    MaybeAdvance(target qviews.DataVersion)

    Close()
}
```

`CatchupDone` is the gate for the first QueryView `Up` report. It closes only
after the initial oracle has consumed the WALView historical inputs and caught up
to the initial live-resource handoff point.

`MaybeAdvance` is called during later QueryView preparation. If `target` is not
newer than `CurrentDataVersion`, it is ignored. Otherwise it enqueues an
asynchronous advance request. The caller does not wait for the request to
finish.

`ApplyLiveEvent` is called by the vchannel resource runtime in the same order as
the WALView live observer delivers events. The method does not return a
recoverable error.

There is intentionally no external `Truncate` method. Obsolete IDF internal
state is cleaned by the runtime after diff commit, segment sealed observation,
or advance-task cancellation.

### 4.3 SealedBM25ResourceProvider

```go
type SealedBM25ResourceProvider interface {
    GetSealedBM25Resources(
        ctx context.Context,
        collectionID int64,
        vchannel string,
        dataVersion qviews.DataVersion,
        settings *viewpb.QueryViewSettings,
    ) ([]*querypb.StreamingNodeBM25Resource, error)
}
```

The returned resources are the full sealed BM25 resource set for the requested
DataVersion. They are not a diff from StreamingNode's current local cache.

### 4.4 SealedBM25SegmentCache

```go
type SealedBM25SegmentCache interface {
    Acquire(
        ctx context.Context,
        resource *querypb.StreamingNodeBM25Resource,
    ) (SealedBM25Lease, error)
}

type SealedBM25Lease interface {
    SegmentID() int64
    PartitionID() int64
    Stats() BM25Stats
    Release()
}
```

`Acquire` may download remote BM25 binlogs, reuse existing local files, and parse
stats. The returned lease keeps local sealed BM25 resources alive while the
current oracle or an in-flight diff references them.

`Stats` returns BM25 stats for diff calculation. Callers must treat returned
stats as read-only or clone them before mutation.

### 4.5 GrowingBM25StatsStore

```go
type GrowingBM25StatsStore interface {
    RegisterSegment(segmentID int64, partitionID int64)
    AppendInsert(segmentID int64, stats BM25Stats)
    MarkFlushed(segmentID int64)
    MarkSealed(segmentID int64, sealedAt qviews.DataVersion)

    SnapshotForDataVersion(
        target qviews.DataVersion,
        targetSealedSegments map[int64]struct{},
    ) map[int64]GrowingBM25Contribution

    Cleanup(currentDataVersion qviews.DataVersion, currentGrowing map[int64]struct{})
    Close()
}
```

`SnapshotForDataVersion` includes a growing segment only when:

```text
segmentID not in targetSealedSegments
and (sealedAtDataVersion is absent or sealedAtDataVersion > target)
```

The `targetSealedSegments` argument prevents double counting when QueryCoord
already reports a segment as sealed but the local `SegmentSealedEvent` has not
yet been delivered to the IDF runtime.

### 4.6 Diff Structures

```go
type BM25Stats map[int64]*storage.BM25Stats

type SealedBM25Contribution struct {
    SegmentID   int64
    PartitionID int64
    Lease       SealedBM25Lease
}

type GrowingBM25Contribution struct {
    SegmentID   int64
    PartitionID int64
    Stats       BM25Stats
}

type IDFContributionSnapshot struct {
    DataVersion qviews.DataVersion

    SealedSegments  map[int64]SealedBM25Contribution
    GrowingSegments map[int64]GrowingBM25Contribution
}

type IDFStatsDiff struct {
    TargetDataVersion qviews.DataVersion

    Positive BM25Stats
    Negative BM25Stats

    NextSealedSegments  map[int64]SealedBM25Contribution
    NextGrowingSegments map[int64]GrowingBM25Contribution

    AcquiredLeases []SealedBM25Lease
}
```

`Positive` contains BM25 stats that should be added to the current oracle.
`Negative` contains BM25 stats that should be subtracted from the current
oracle. The commit path merges the diff once and replaces contribution metadata.

## 5. Actual Behavior

### 5.1 Initial Build

Initial construction is triggered only by `OnAlterLoadConfig` or recovery:

```text
RecoveryStorage observes AlterLoadConfig / restores load intent
  -> builds VChannelWALView(baseDataVersion)
  -> StreamingNodeResourceManager.OnAlterLoadConfig(view)
  -> resource BuildTask starts
  -> IDFOracleRuntimeBuilder.BuildInitial(view)
```

`BuildInitial` performs:

1. derive BM25-enabled loaded fields from `view.Schema` and QueryView settings;
2. create an empty ready runtime if no loaded field requires BM25;
3. call QueryCoord through `SealedBM25ResourceProvider` with the WALView base
   DataVersion;
4. acquire and parse the initial sealed BM25 resources through
   `SealedBM25SegmentCache`;
5. initialize `GrowingBM25StatsStore` from
   `view.SegmentSnapshot.Segments`;
6. generate BM25 stats from each snapshot segment's persisted BM25 binlogs and
   snapshot insert messages;
7. merge sealed and growing stats into the initial current oracle;
8. attach ordered live event application;
9. close `CatchupDone` after the initial no-gap live handoff is caught up.

Snapshot insert messages may be Insert or Txn(Insert) messages. Consumers must
use the shared WALView insert parser and select only rows assigned to the target
segment. A WAL message must not be blindly counted for every segment.

### 5.2 Initial Catchup And First Up

The first QueryView `Up` report waits for IDF catchup:

```text
QueryViewStateMachine acquires first QueryView resource
  -> waits for resource readiness
  -> waits for IDFOracleRuntime.CatchupDone
  -> reports QueryView Up
```

This does not make `Up` a resource-manager event. `Up` remains QueryView
state-machine metadata used for recovery. The catchup wait only ensures the
first QueryView is not marked Up before the vchannel singleton IDF oracle has
finished its initial no-gap resource preparation.

Later QueryViews do not wait for background IDF advancement.

### 5.3 Live Growing BM25 Generation

After initialization, the runtime continuously applies ordered live resource
events:

| Event | IDF behavior |
|---|---|
| `CreateSegment` | Register a local growing BM25 stats slot for the segment if the segment belongs to the loaded vchannel and settings. |
| `Insert` / `Txn(Insert)` | Parse BM25 output sparse fields, append the stats to `GrowingBM25StatsStore`, and update the current oracle if the segment is in the current growing contribution set. |
| `Flush` | Mark the segment as flushed and stop accepting later inserts for that segment. |
| `SegmentSealedEvent` | Record the segment's `sealedAtDataVersion`. If the segment is no longer referenced by the current oracle, internal cleanup may remove its growing stats. |
| Collection / partition lifecycle messages | Remove or ignore stats according to the WALView resource contract and loaded settings. |

Deletes are not subtracted from BM25 stats. BM25 stats follow the existing
binlog/statistics model; row visibility is handled by growing/sealed data
resources, not by mutating IDF statistics for each delete.

### 5.4 QueryView Preparation Notification

When a QueryView starts using the vchannel resource, the resource manager first
registers the QueryView reference. Only after the reference is recorded may it
notify the IDF runtime:

```text
QueryView Acquire / Prepare for DataVersion D
  -> register queryViewRef
  -> if this is the first QueryView ref, remove initRef in the same state update
  -> verify the initial resource exists and initial catchup is ready
  -> IDFOracleRuntime.MaybeAdvance(D)
  -> QueryView can become effective without waiting for IDF advancement
```

This notification is protected by the same reference transfer as growing segment
resources. A later `DropLoadConfig` cannot close the IDF runtime while the
QueryView reference remains.

The notification is not a resource build task. It does not use the StreamingNode
resource build scheduler and does not block QueryView readiness.

`MaybeAdvance` only enqueues work when `D > CurrentDataVersion`. Multiple
notifications are coalesced by keeping the largest pending target:

```text
current = D10
pending = D11
new notification = D12
  -> pending becomes D12
```

### 5.5 Advance Worker

The advance worker is vchannel-local and serial:

```text
pending target DataVersion
  -> snapshot current contributions
  -> compute diff outside commit path
  -> atomically commit diff if target is still newer
  -> cleanup obsolete resources
```

If an in-flight task finishes after a newer task has already committed, the old
task releases its acquired sealed leases and discards its diff.

Failures in background advancement do not block the QueryView that triggered the
notification. The worker can retry or wait for the next newer notification
according to the runtime policy. A failure to apply valid WAL live input remains
critical.

### 5.6 Diff Calculation

For target DataVersion `D`, the worker computes a complete target contribution
set before modifying the current oracle:

```text
1. QueryCoord returns target sealed BM25 resources for D.
2. SealedBM25SegmentCache acquires leases and stats for target sealed resources.
3. GrowingBM25StatsStore returns target growing contributions for D.
4. The worker compares current contributions with target contributions.
5. The worker builds Positive and Negative BM25Stats.
```

Diff rules:

```text
negative:
  current sealed segment not in target sealed set
  current growing segment not in target growing set

positive:
  target sealed segment not in current sealed set
  target growing segment not in current growing set
```

When a segment moves from growing to sealed at the target DataVersion, the diff
contains both:

```text
negative: old growing BM25 stats for the segment
positive: new sealed BM25 stats for the segment
```

All stats used by the diff are computed or cloned outside the commit lock.

### 5.7 Atomic Diff Commit

The commit path is the only place where `currentStats`,
`currentDataVersion`, and contribution metadata change together:

```text
commitDiff(diff):
  lock runtime
  if diff.TargetDataVersion <= currentDataVersion:
      unlock
      release diff.AcquiredLeases
      return false

  currentStats.Minus(diff.Negative)
  currentStats.Merge(diff.Positive)
  currentDataVersion = diff.TargetDataVersion
  replace current sealed contribution metadata
  replace current growing contribution metadata
  compute old sealed leases no longer referenced
  cleanup obsolete growing stats
  unlock

  release old sealed leases outside lock
  return true
```

The oracle is never advanced by partially applying sealed resources, growing
stats, or contribution metadata.

### 5.8 Internal Cleanup

Cleanup is owned by `IDFOracleRuntime`. There is no external truncation API.

The runtime may remove growing stats when:

```text
segment is not in currentGrowingContributions
and sealedAtDataVersion exists
and sealedAtDataVersion <= currentDataVersion
```

The runtime may release a sealed BM25 lease when:

```text
segment is not in currentSealedContributions
and segment is not referenced by any in-flight diff task
```

Cleanup is triggered after:

1. successful diff commit;
2. discarded or superseded advance task;
3. `SegmentSealedEvent` if the segment is already outside current growing
   contributions;
4. runtime `Close`.

`StreamingNodeResourceManager` does not call `Truncate` on IDF internals. It only
closes the whole runtime when the vchannel resource has no remaining references.

### 5.9 Recovery

Recovery uses the same path as normal initialization:

```text
PChannelRuntime restores WAL state and QueryView meta
  -> QueryViewStateMachine provides the oldest recovered Up QueryView DataVersion
  -> RecoveryStorage selects recovery base DataVersion
  -> builds VChannelWALView(recoveryBaseDataVersion)
  -> StreamingNodeResourceManager.OnAlterLoadConfig(view)
  -> IDFOracleRuntimeBuilder.BuildInitial(view)
```

If persisted Up QueryViews exist, the recovery base DataVersion is the oldest Up
QueryView DataVersion selected by the PChannel recovery rule. Otherwise it is
the base DataVersion provided by WALView construction.

After recovery initializes the singleton oracle, recovered QueryViews are
acquired in QueryViewVersion order. Later QueryView preparation notifications may
asynchronously advance the oracle as usual.

### 5.10 Close

`Close` is the runtime lifecycle end:

1. stop the advance worker;
2. discard pending diffs and release their acquired leases;
3. close the growing stats store;
4. release all current sealed contribution leases;
5. close `CatchupDone` if the runtime is closed before initial catchup finishes.

`Close` is idempotent.

## 6. Summary Invariants

1. `IDFOracleRuntime` is a vchannel-level singleton.
2. The initial oracle is built from one `VChannelWALView` base DataVersion.
3. Initial sealed stats come from QueryCoord RPC.
4. Initial and live growing stats come from WALView snapshot and ordered live
   resource events.
5. The first QueryView `Up` report waits for IDF initial catchup.
6. Later QueryView preparation calls `MaybeAdvance` only after the QueryView
   reference is registered; it does not wait.
7. Advancement is serial, asynchronous, and monotonic.
8. QueryCoord provides the complete sealed BM25 set for the target DataVersion.
9. Diff computation happens outside the commit path.
10. Current oracle mutation is one atomic diff commit.
11. IDF internal cleanup is owned by `IDFOracleRuntime`.
12. ResourceManager closes the whole runtime but never truncates IDF internals.
