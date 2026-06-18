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
  -> GrowingSegmentRuntimeBuilder.NewRuntime
  -> GrowingSegmentRuntime(Preparing, VChannelLiveObserver)
  -> Scheduler
  -> GrowingSegmentRuntime.Prepare
  -> GrowingSegmentRuntime(Ready)
```

The purpose of `GrowingSegmentRuntime` is to:

1. own all growing-side segment resources for one vchannel resource base
   `DataVersion`;
2. load historical visible segment data from `VChannelWALView.SegmentSnapshot`;
3. apply historical deletes from `VChannelWALView.DeleteReplay`;
4. implement `VChannelLiveObserver` and buffer live events while preparing;
5. track runtime health and close all segment resources when released.

The runtime does not select DataVersion, compute visible segment membership, read
WAL modules directly, manage QueryView references, or decide final resource
release. Those responsibilities belong to `RecoveryStorage`,
`VChannelWALView`, `SegmentModule`, and `StreamingNodeResourceManager`.

## 2. Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `StreamingNodeResourceManager` | PChannel-local owner of QueryView/init references and build state. Submits runtime build jobs and releases completed runtimes for its PChannel. | It does not build individual growing segments directly and does not serve resources across PChannels. |
| `Scheduler` | Runs runtime prepare jobs with bounded concurrency. | It does not know QueryView references, priority, or resource lifecycle. |
| `GrowingSegmentRuntimeBuilder` | Creates one `GrowingSegmentRuntime` in `Preparing` state from one `VChannelWALView`. | It does not submit jobs, run historical loading, manage references, or choose DataVersion. |
| `GrowingSegmentRuntime` | Owns the vchannel-level segment map, live observer state, pending event buffer, live drain task, applied frontiers, truncation watermark, and close path. | It does not access `SegmentModule` or `TransformLogModule`. |
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
        | Create Preparing runtime and submit prepare job
        v
Scheduler
        |
        | Run with concurrency limit
        v
GrowingSegmentRuntime.Prepare
        |
        | Load historical WALView inputs and mark Ready
        v
GrowingSegmentRuntime
        |
        | Owns many
        v
GrowingSegment
```

The scheduler controls how many prepare jobs run concurrently. The builder
creates the runtime before scheduling. The runtime owns the resulting resources.

### 3.2 Runtime State

```text
GrowingSegmentRuntime
  collectionID
  vchannel
  baseDataVersion
  schema
  state Preparing | Ready | Closed
  segments map[segmentID]GrowingSegment
  pendingEvents []VChannelResourceEvent
  liveDrainTask
  pendingDrained
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

1. `GrowingSegmentRuntimeBuilder` only consumes `VChannelWALView` and creates a
   `GrowingSegmentRuntime` that implements `VChannelLiveObserver`.
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
9. The runtime is returned as the live observer before the prepare task runs.
   Events observed in `Preparing` are stored in the runtime pending buffer.
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
    Result() (*ViewRuntime, error)
    Cancel()
}
```

The scheduler guarantees only bounded build concurrency. It does not implement
priority, QueryView reference tracking, result storage, cancellation policy, or
DataVersion selection.

`BuildTask` is both the scheduled unit and the build handle. It owns the
cancellable build context, executes the runtime prepare function, records the
terminal result, closes `Done`, and exposes `Cancel` to the resource manager.

### 4.2 GrowingSegmentRuntimeBuilder

```go
type GrowingSegmentRuntimeBuilder interface {
    NewRuntime(desc LoadResourceDescriptor) (*GrowingSegmentRuntime, error)
}
```

The builder constructs one `Preparing` runtime from one WALView. It is not a
scheduler, does not execute historical loading, and does not own global
concurrency policy.

### 4.3 GrowingSegmentRuntime

```go
type GrowingSegmentRuntime struct {
    // concrete fields hidden by package boundary
}

func (r *GrowingSegmentRuntime) DataVersion() qviews.DataVersion
func (r *GrowingSegmentRuntime) ObserveEvent(ctx context.Context, event walview.VChannelResourceEvent) bool
func (r *GrowingSegmentRuntime) Prepare(ctx context.Context) error
func (r *GrowingSegmentRuntime) PendingDrained() <-chan struct{}
func (r *GrowingSegmentRuntime) Truncate(minDataVersion qviews.DataVersion)
func (r *GrowingSegmentRuntime) Close()
```

The runtime exposes resource lifecycle and truncation operations. Query-facing
APIs are intentionally out of scope for this document.

`ObserveEvent` is the WAL live observer entrypoint. In `Preparing`, it buffers
events. In `Ready`, it appends events and starts asynchronous drain when needed.

`Prepare` performs historical loading and changes the runtime to `Ready`.
`PendingDrained` closes after the runtime has entered `Ready` and the pending
buffer captured around the initial WALView handoff has been drained.

Runtime live apply does not return a recoverable error. Failure to apply a valid
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
  -> create GrowingSegmentRuntime in Preparing state
  -> return GrowingSegmentRuntime as VChannelLiveObserver
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
  -> create GrowingSegmentRuntime in Preparing state
  -> return GrowingSegmentRuntime as VChannelLiveObserver
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

### 5.3 Runtime Prepare Flow

`GrowingSegmentRuntime.Prepare` executes the resource preparation flow:

```text
1. Create the underlying collection resource.
2. Load every segment from view.SegmentSnapshot.Segments.
3. Drain and apply view.DeleteReplay.
4. Initialize applied frontiers from the WALView base watermarks.
5. Mark the runtime Ready.
6. If the pending buffer is non-empty, start asynchronous drain.
```

The runtime trusts the no-gap WAL input contract of `VChannelWALView`. It does
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

After snapshot segment loading completes, `Prepare` drains
`view.DeleteReplay` to completion.

Each returned transform entry is applied through `GrowingSegmentRuntime`, which
dispatches it to relevant `GrowingSegment` instances:

```text
DeleteReplay entry
  -> GrowingSegmentRuntime.ApplyHistoricalDelete
  -> matching GrowingSegment.ApplyDelete
```

If the scanner returns an error, prepare fails. A failed prepare closes the
scanner and every partially built segment resource.

### 5.6 Frontier Initialization

After snapshot inserts and historical deletes are applied, the runtime initializes
its base frontiers:

```text
appliedGrowingTimeTick = view.BaseGrowingTimeTick
appliedTransformTimeTick = view.BaseTransformTimeTick
```

These frontiers describe resource preparation progress. Query behavior is out of
scope.

### 5.7 Live Observer And Pending Drain

The runtime itself is returned as the live observer before the prepare task runs.

```text
RecoveryStorage live resource event
  -> GrowingSegmentRuntime.ObserveEvent
     -> if Preparing: append to pending buffer
     -> if Ready: append to pending buffer and start drain if needed
  -> GrowingSegmentRuntime drain task
  -> GrowingSegment methods as needed
```

When `Prepare` marks the runtime `Ready`, it starts an asynchronous drain if
there are buffered events. `PendingDrained` closes after the runtime is ready and
the pending buffer has reached empty at least once. The IDF oracle initial
`CatchupDone` waits for this signal.

The drain task consumes resource events in the order delivered by
`VChannelLiveObserver`. Runtime live apply has no recoverable error path. If a
ready runtime cannot apply valid live input, the StreamingNode must treat it as
a critical corruption.

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

`GrowingSegmentRuntime.Prepare` marks the runtime ready only after:

1. all snapshot visible segments are created;
2. all persisted segment storage is loaded;
3. all snapshot insert messages are replayed;
4. `DeleteReplay` is drained and applied;
5. base frontiers are initialized;
6. the runtime is marked `Ready`.

Ready means resource preparation completed for the WALView handoff. It does not
describe query execution behavior.

### 5.12 Failure And Cleanup

Build failure closes all owned resources:

- `DeleteReplay` scanner;
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
3. `GrowingSegmentRuntimeBuilder` creates one Preparing runtime from one
   `VChannelWALView`.
4. `Scheduler` limits build concurrency only.
5. `Acquire` never schedules runtime builds.
6. `VChannelWALView` provides no-gap WAL input.
7. The runtime must buffer and apply WALView live input without breaking the
   no-gap handoff.
8. Ready is published only after historical inputs are applied and the runtime
   state changes to `Ready`.
9. `Truncate` releases only segments whose sealed DataVersion is known and not
   newer than the truncation watermark.
10. Runtime live apply has no recoverable error path.
