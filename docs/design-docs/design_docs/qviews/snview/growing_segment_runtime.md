# StreamingNode Growing Segment Runtime Design

> StreamingNode-side growing segment resource runtime for QueryView.
> This document defines the `GrowingSegmentRuntime` resource model and its
> preparation flow. Query execution, query plan format, and QueryNode sealed
> segment lifecycle are out of scope.

## 1. Purpose

`GrowingSegmentRuntime` is the vchannel-level resource that owns the growing-side
data prepared from `VChannelWALView`.

It is created by the PChannel-local `StreamingNodeResourceManager` when WAL load
intent or StreamingNode crash recovery requires local query resources:

```text
PChannelRuntime
  -> RecoveryStorage
  -> VChannelWALView
  -> StreamingNodeResourceManager
  -> Scheduler
  -> GrowingSegmentRuntimeBuilder
  -> GrowingSegmentRuntime
```

The purpose of `GrowingSegmentRuntime` is to:

1. own all growing-side segment resources for one vchannel resource base
   `DataVersion`;
2. load historical visible segment data from `VChannelWALView.SegmentSnapshot`;
3. apply historical deletes from `VChannelWALView.DeleteReplay`;
4. attach the no-gap live WAL input provided by `VChannelLiveObserver`;
5. track runtime health and close all segment resources when released.

The runtime does not select DataVersion, compute visible segment membership, read
WAL modules directly, manage QueryView references, or decide final resource
release. Those responsibilities belong to `RecoveryStorage`,
`VChannelWALView`, `SegmentModule`, and `StreamingNodeResourceManager`.

## 2. Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `StreamingNodeResourceManager` | PChannel-local owner of QueryView/init references and build state. Submits runtime build jobs and releases completed runtimes for its PChannel. | It does not build individual growing segments directly and does not serve resources across PChannels. |
| `Scheduler` | Runs runtime build jobs with bounded concurrency. | It does not know QueryView references, priority, or resource lifecycle. |
| `GrowingSegmentRuntimeBuilder` | Builds one `GrowingSegmentRuntime` from one `VChannelWALView` and the corresponding live stream. | It does not submit jobs, manage references, or choose DataVersion. |
| `GrowingSegmentRuntime` | Owns the vchannel-level segment map, live apply loop, applied frontiers, truncation watermark, and close path. | It does not access `SegmentModule` or `TransformLogModule`. |
| `GrowingSegment` | Owns one segment's local resource handle and applies segment-scoped persisted data, inserts, and deletes. | It does not own vchannel-level message dispatch or live observer registration. |
| `VChannelWALView` | Provides no-gap WAL input for the selected resource base DataVersion. | Its contract is defined in [StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md). |
| `SegmentModule` | Owns segment metadata, visible snapshot construction, and segment metadata GC. | It is only consumed through `VChannelWALView`; runtime components do not call it directly. |
| `TransformLogModule` | Owns transform log storage and delete replay scanner construction. | It is only consumed through `VChannelWALView.DeleteReplay`. |

## 3. Component Relationships And Invariants

### 3.1 Relationship Model

```text
PChannelRuntime
        |
        | Owns
        v
StreamingNodeResourceManager
        |
        | Submit build job
        v
Scheduler
        |
        | Run with concurrency limit
        v
GrowingSegmentRuntimeBuilder
        |
        | Build from VChannelWALView
        v
GrowingSegmentRuntime
        |
        | Owns many
        v
GrowingSegment
```

The scheduler controls how many build jobs run concurrently. The builder
executes one job. The runtime owns the resulting resources.

### 3.2 Runtime State

```text
GrowingSegmentRuntime
  collectionID
  vchannel
  baseDataVersion
  schema
  segments map[segmentID]GrowingSegment
  liveStream
  liveApplyTask
  appliedGrowingTimeTick
  appliedTransformTimeTick
  truncateDataVersion
  close/cancel
```

`GrowingSegmentRuntime` is a vchannel-level data collection. There is no
separate `GrowingSegmentSet` abstraction; the runtime itself owns segment
membership and vchannel-level WAL message dispatch.

### 3.3 Segment State

```text
GrowingSegment
  segmentID
  partitionID
  sealedAtDataVersion
  local segment resource handle
  closed state
```

`GrowingSegment` is the single-segment resource wrapper. It hides the concrete
storage or segcore implementation from the runtime builder.

### 3.4 Invariants

1. `GrowingSegmentRuntimeBuilder` only consumes `VChannelWALView` and the live
   stream created from the returned `VChannelLiveObserver`.
2. Runtime preparation never reads `SegmentModule` or `TransformLogModule`
   directly.
3. `VChannelWALView` owns the no-gap input guarantee.
4. `GrowingSegmentRuntime` owns the vchannel segment map and live resource event
   dispatch.
5. Each `GrowingSegment` owns exactly one segment resource handle.
6. Snapshot segment membership during preparation comes only from
   `VChannelWALView.SegmentSnapshot.Segments`.
7. Persisted segment data is loaded before snapshot insert messages are replayed
   for the same segment.
8. `DeleteReplay` is drained and applied before the runtime is reported ready.
9. The live stream is attached before the runtime is reported ready.
10. `SealedAtDataVersion` updates after the WALView capture point are delivered
    by resource events from RecoveryStorage, not by direct SegmentModule reads.
11. Runtime live apply is not a recoverable resource-level error path. If a
    ready runtime cannot apply valid live input, the input or runtime is
    corrupted and the StreamingNode must fail critically.
12. `Scheduler` only controls build-job concurrency; it does not create build
    jobs from QueryView `Acquire`.
13. QueryView `Acquire` never submits or rebuilds a runtime build job.

## 4. Interface Description

### 4.1 Scheduler

```go
type Scheduler interface {
    Submit(task BuildTask)
    Close()
}

type BuildKey struct {
    CollectionID int64
    VChannel     string
    DataVersion  qviews.DataVersion
}

type BuildTask interface {
    Key() BuildKey
    Run()

    Done() <-chan struct{}
    Result() (*GrowingSegmentRuntime, error)
    Cancel()
}
```

The scheduler guarantees only bounded build concurrency. It does not implement
priority, QueryView reference tracking, result storage, cancellation policy, or
DataVersion selection.

`BuildTask` is both the scheduled unit and the build handle. It owns the
cancellable build context, executes `GrowingSegmentRuntimeBuilder.Build`, records
the terminal result, closes `Done`, and exposes `Cancel` to the resource manager.

### 4.2 GrowingSegmentRuntimeBuilder

```go
type GrowingSegmentRuntimeBuilder interface {
    Build(
        ctx context.Context,
        view walview.VChannelWALView,
        live LiveStream,
    ) (*GrowingSegmentRuntime, error)
}
```

The builder constructs one runtime from one WALView. It is not a scheduler and
does not own global concurrency policy.

### 4.3 GrowingSegmentRuntime

```go
type GrowingSegmentRuntime struct {
    // concrete fields hidden by package boundary
}

func (r *GrowingSegmentRuntime) DataVersion() qviews.DataVersion
func (r *GrowingSegmentRuntime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent)
func (r *GrowingSegmentRuntime) Truncate(minDataVersion qviews.DataVersion)
func (r *GrowingSegmentRuntime) Close()
```

The runtime exposes resource lifecycle and truncation operations. Query-facing
APIs are intentionally out of scope for this document.

`ApplyLiveEvent` does not return a recoverable error. Failure to apply a valid
live event means the write path, WALView input, or runtime state is corrupted.
That is a critical StreamingNode failure, not a resource-manager condition that
can be repaired locally.

### 4.4 GrowingSegment

```go
type GrowingSegment interface {
    ID() int64
    PartitionID() int64
    SealedAtDataVersion() *viewpb.DataVersion

    LoadPersisted(ctx context.Context, storage *streamingpb.L1SegmentPersistedStorage) error
    ApplyInsert(ctx context.Context, msg message.ImmutableMessage) error
    ApplyDelete(ctx context.Context, entry *streamingpb.TransformLogEntry) error
    MarkSealed(sealedAt qviews.DataVersion)

    Close()
}
```

Live resource events are not applied directly to a single segment. They first enter
`GrowingSegmentRuntime`, which performs vchannel-level dispatch and then calls
segment-scoped methods.

`MarkSealed` records the `DataVersion` assigned after the segment's flush commit
is acknowledged. WAL `Flush` closes the segment for writes, but it does not carry
this `DataVersion`.

## 5. Actual Behavior

### 5.1 Build Job Creation

Only `StreamingNodeResourceManager` creates runtime build jobs.

Normal load:

```text
RecoveryStorage observes AlterLoadConfig
  -> builds VChannelWALView
  -> StreamingNodeResourceManager.OnAlterLoadConfig(view)
  -> create initRef
  -> create BuildTask
  -> scheduler.Submit(task)
```

Crash recovery:

```text
PChannelRuntime restores load intent and persisted QueryView meta
  -> QueryViewStateMachine provides the oldest recovered Up QueryView DataVersion
  -> RecoveryStorage selects recovery base DataVersion
  -> builds VChannelWALView(recoveryBaseDataVersion)
  -> StreamingNodeResourceManager.OnAlterLoadConfig(view)
  -> create BuildTask
  -> scheduler.Submit(task)
```

`Acquire` never submits build jobs. It can only observe an existing `BuildTask`
or completed runtime.

### 5.2 Scheduler Behavior

The scheduler runs build jobs under a fixed concurrency limit:

```text
queued BuildTask
  -> wait for concurrency slot
  -> run task.Run()
  -> task publishes runtime or error to its own result state
```

The scheduler does not know whether the job is an initialization load or a
recovery load. Priority and promotion are not part of this design.

When all references for an in-flight job are removed, `StreamingNodeResourceManager`
may call `BuildTask.Cancel()`. When all references for a completed runtime are
removed, `StreamingNodeResourceManager` closes the runtime.

### 5.3 Runtime Build Flow

`GrowingSegmentRuntimeBuilder.Build` executes the resource preparation flow:

```text
1. Create an empty GrowingSegmentRuntime.
2. Load every segment from view.SegmentSnapshot.Segments.
3. Drain and apply view.DeleteReplay.
4. Initialize applied frontiers from the WALView base watermarks.
5. Attach the live stream and start the runtime live apply task.
6. Return ready runtime.
```

The builder trusts the no-gap WAL input contract of `VChannelWALView`. It does
not re-check or reconstruct the WALView's snapshot boundaries.

### 5.4 Snapshot Segment Loading

For each `walview.VisibleSegment` in `view.SegmentSnapshot.Segments`:

```text
1. Create GrowingSegment.
2. If SegmentSnapshotData.PersistedStorage exists, call LoadPersisted.
3. Replay SegmentSnapshotData.InsertMessages through ApplyInsert.
4. Register the segment in GrowingSegmentRuntime.
```

`InsertMessages` may include Insert or Txn(Insert) messages. Consumers must use
the shared WALView insert parser and select only rows assigned to the target
segment. A WAL message must not be blindly loaded into every segment.

### 5.5 Historical Delete Replay

After snapshot segment loading completes, the builder drains
`view.DeleteReplay` to completion.

Each returned transform entry is applied through `GrowingSegmentRuntime`, which
dispatches it to relevant `GrowingSegment` instances:

```text
DeleteReplay entry
  -> GrowingSegmentRuntime.ApplyHistoricalDelete
  -> matching GrowingSegment.ApplyDelete
```

If the scanner returns an error, build fails. A failed build closes the scanner,
the live stream, and every partially built segment resource.

### 5.6 Frontier Initialization

After snapshot inserts and historical deletes are applied, the runtime initializes
its base frontiers:

```text
appliedGrowingTimeTick = view.BaseGrowingTimeTick
appliedTransformTimeTick = view.BaseTransformTimeTick
```

These frontiers describe resource preparation progress. Query behavior is out of
scope.

### 5.7 Live Stream Attachment

The live stream is attached before the runtime is reported ready.

```text
live stream
  -> GrowingSegmentRuntime live apply task
  -> GrowingSegmentRuntime.ApplyLiveEvent
  -> GrowingSegment methods as needed
```

The live apply task consumes later resource events in the order delivered by
`VChannelLiveObserver`. Runtime live apply has no recoverable error path. If a
ready runtime cannot apply valid live input, the StreamingNode must treat it as a
critical corruption.

### 5.8 Live Resource Event Dispatch

`GrowingSegmentRuntime` owns vchannel-level dispatch for live resource events:

| Event | Runtime behavior |
|---|---|
| `CreateSegment` | Create and register a new `GrowingSegment` if the message belongs to the runtime's vchannel. |
| `Insert` / `Txn(Insert)` | Dispatch assigned rows to the target `GrowingSegment`. |
| `Delete` / `Txn(Delete)` | Dispatch delete entries to relevant segments. |
| `Flush` | Mark the segment as flushed and stop accepting later inserts for that segment. |
| `SegmentSealedEvent` | Record the segment's `sealedAtDataVersion` through `GrowingSegment.MarkSealed`. |
| Collection / partition lifecycle messages | Update or close affected segment resources when required by the WALView resource contract. |

The exact query-visible effects of these messages are outside this document; the
runtime only guarantees that the resource state is advanced consistently.

### 5.9 Sealed DataVersion Synchronization

`GrowingSegment.SealedAtDataVersion` can be present in the WALView snapshot for
segments flushed before the capture point. For segments flushed after the capture
point, the WAL `Flush` message is not enough: it closes the segment but does not
carry the DataCoord-assigned sealed `DataVersion`.

The sealed version is synchronized as a resource event:

```text
SegmentModule commits flushed segment
  -> DataCoord returns sealedAtDataVersion
  -> SegmentModule records sealed_at_data_version
  -> RecoveryStorage emits SegmentSealedEvent
  -> GrowingSegmentRuntime marks the segment sealed
```

`SegmentSealedEvent` is idempotent. If the same segment receives the same sealed
DataVersion more than once, the runtime keeps the existing value. If it receives
a different sealed DataVersion for the same segment, the local state is
corrupted and the StreamingNode must fail critically.

### 5.10 Truncation

`GrowingSegmentRuntime.Truncate(minDataVersion)` releases segment resources that
are no longer required by the resource manager's reference model.

Truncation rules:

1. The truncation watermark is monotonic. Calls with an older DataVersion are
   ignored.
2. A segment with unknown `sealedAtDataVersion` is retained.
3. A segment with `sealedAtDataVersion <= minDataVersion` can be closed and
   removed from the runtime.
4. `Truncate` only releases individual `GrowingSegment` resources; it does not
   close the whole runtime.
5. If a `SegmentSealedEvent` arrives after the watermark has already advanced,
   the runtime immediately applies the same truncation rule to that segment.

`Close` is still the runtime-level lifecycle end. `Truncate` is only an internal
segment-resource trimming operation.

### 5.11 Ready Condition

`GrowingSegmentRuntimeBuilder` returns ready only after:

1. all snapshot visible segments are created;
2. all persisted segment storage is loaded;
3. all snapshot insert messages are replayed;
4. `DeleteReplay` is drained and applied;
5. base frontiers are initialized;
6. the live stream is attached to the runtime.

Ready means resource preparation completed for the WALView handoff. It does not
describe query execution behavior.

### 5.12 Failure And Cleanup

Build failure closes all owned resources:

- `DeleteReplay` scanner;
- live stream / observer handle;
- partially created `GrowingSegment` resources;
- the partially created `GrowingSegmentRuntime`.

Runtime close is idempotent and closes every owned `GrowingSegment`.

After a runtime is ready, live apply failure is not locally recoverable. Valid
live input must be applied successfully. If this invariant is broken, the
StreamingNode must fail critically instead of reporting a recoverable resource
error.

## 6. Summary Invariants

1. `GrowingSegmentRuntime` is the only vchannel-level growing segment collection.
2. There is no separate `GrowingSegmentSet`.
3. `GrowingSegmentRuntimeBuilder` builds one runtime from one `VChannelWALView`.
4. `Scheduler` limits build concurrency only.
5. `Acquire` never schedules runtime builds.
6. `VChannelWALView` provides no-gap WAL input.
7. The builder must attach and apply the WALView input without breaking the
   no-gap handoff.
8. Ready is published only after historical inputs are applied and the live
   stream is attached.
9. `Truncate` releases only segments whose sealed DataVersion is known and not
   newer than the truncation watermark.
10. Runtime live apply has no recoverable error path.
