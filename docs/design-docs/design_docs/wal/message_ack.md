# WAL Message Ack Design

> Reference-counted completion tracking for data moving from WAL into object
> storage and the RecoveryStorage catalog in etcd.

This document is the authoritative design for data-message completion,
RecoveryStorage data checkpoint advancement, and broadcast acknowledgement.
QueryView resource references are a separate, temporary serving concern and are
intentionally out of scope.

## 1. Problem

A persisted WAL message can trigger asynchronous work in several consumers:

- one or more `segment.SegmentView` instances may ensure growing state, flush
  insert chunks, or commit L1 segments;
- one or more `transformlog.TransformLog` instances may flush entries into
  durable transform-log chunks;
- `broadcastAckModule` may acknowledge a broadcast message to StreamingCoord.

The existing timetick Barrier model answers a component-level question:

```text
what timetick has this component published?
```

RecoveryStorage also needs a message-level question:

```text
has every asynchronous consumer of this WAL message finished?
```

The answer must support an arbitrary number of consumers. One message may touch
multiple VChannels, SegmentViews, TransformLogs, and physical tasks, so a fixed
set of module reasons is not the completion model. The completion model is a
sealed reference count.

## 2. Scope And Boundaries

This design covers:

- messages read by the RecoveryStorage data scanner;
- asynchronous persistence from WAL into object storage;
- publication of recovery metadata into the RecoveryStorage catalog in etcd;
- the data checkpoint persisted with recovery metadata;
- broadcast acknowledgement ordering and completion.

This design does not cover:

- QueryView references or QueryRuntime resource lifetime;
- QueryNode consumption of TransformLog subscriptions;
- forcing all work caused by one message into one physical scheduler task;
- L0 compaction after TransformLog materialization;
- durable persistence of every in-flight tracked entry in the first implementation.

A `RefCountedImmutableMessage` is a logical completion group. Every actual
consumer obtains its own retained message handle. Physical task layout remains
module local: one message may create many tasks, and one storage task may batch
work from many messages.

Ack does not schedule those tasks or impose component-local execution order.
SegmentView, TransformLog, and BroadcastAck remain responsible for their own
ordering and dependency rules. Ack only observes whether every retained
operation has reached that operation's success condition.

## 3. Current Ownership

RecoveryStorage registers two top-level recovery modules:

```text
RecoveryStorage
  +-- PChannelRecoveryManager
  |     +-- VChannelRecoveryModule*
  |           +-- SegmentView*
  |           +-- TransformLog
  |           +-- VChannel/DataView recovery state
  +-- broadcastAckModule
```

`PChannelRecoveryManager` routes messages to the affected
`VChannelRecoveryModule` instances. `SegmentView` and `TransformLog` are
VChannel-internal components; there is no independently registered runtime
`SegmentModule` or `TransformLogModule`.

Message Ack is infrastructure, not another recovery module. RecoveryStorage
keeps lightweight tracked entries in WAL order and observes completion.

## 4. Reference-Count Model

### 4.1 Message Wrapper And Retained Handle

The generic wrapper belongs to `pkg/streaming/util/message`. It contains no WAL
checkpoint point and has no dependency on RecoveryStorage:

```go
type RefCountedImmutableMessage interface {
    ImmutableMessage

    Retain() RetainedImmutableMessage
}

type RetainedImmutableMessage interface {
    ImmutableMessage

    Sealed() bool
    IsExclusive() bool
    Release()
}

type RefCountedImmutableMessageController interface {
    RefCountedImmutableMessage

    Seal()
}
```

`NewRefCountedImmutableMessage` takes ownership of the supplied
`ImmutableMessage` without copying its payload or properties. The caller must
drop its original reference after construction. The wrapper, all retained
handles, and all specialized views share one reference-count control block and
one underlying immutable message.

The constructor returns the controller to `messageack.Tracker`. RecoveryStorage
and modules receive only its narrowed `RefCountedImmutableMessage` view, so they
can inspect and retain the message but cannot release the implicit dispatch
reference or seal the message themselves.

Every `Retain()` increments the shared count and returns a distinct retained
message handle. The handle delegates all `ImmutableMessage` methods to the same
underlying message and owns exactly one release. `Release()` is idempotent for
that handle, clears the handle's control-block pointer, and decrements the count
at most once. A released handle is invalid; reading message data through it is a
programming error.

`IsExclusive()` is true only when the message is sealed and this retained handle
is the final remaining reference. BroadcastAck uses this predicate instead of
reading a raw global reference count.

The controller created for a data-scanner message starts with one implicit
dispatch reference:

```text
sealed   = false
refCount = 1
```

There is no reason enum, reason bitset, reason map, or global per-module
sub-count. Recovery correctness depends only on the total count.

### 4.2 Specialization

Reference counting is part of the immutable message wrapper, not a parallel Ack
parameter. Specialization must preserve the same control block:

```go
type RefCountedSpecializedImmutableMessage[H proto.Message, B proto.Message] interface {
    SpecializedImmutableMessage[H, B]
    RefCountedImmutableMessage
}

type RetainedSpecializedImmutableMessage[H proto.Message, B proto.Message] interface {
    SpecializedImmutableMessage[H, B]
    RetainedImmutableMessage
}
```

Generated helpers provide typed conversions such as
`MustAsRetainedImmutableInsertMessageV1`. A retained specialized message exposes
`Header`, `Body`, ordinary immutable-message methods, and `Release` on the same
object. Segment and TransformLog queues therefore store a retained message
handle instead of storing an immutable message and a separate completion token.

Txn retention belongs to the outer retained Txn handle. Messages returned by
`RangeOver` are borrowed views valid only while that outer handle remains live;
they do not own independent releases.

Ordinary `MustAsImmutableXxx` conversion must recognize the wrapper and preserve
its reference-count capability in the returned dynamic object. Meta-only replay
passes a normal `ImmutableMessage`; it supports the same specialization API but
cannot be retained. A handler that attempts to schedule data work from a
Meta-only message is a programming error.

### 4.3 Open And Sealed Phases

The shared reference-count controller has two phases:

```text
Open:
  synchronous observers may Retain

Sealed:
  Retain is forbidden
  existing retained handles may only move toward Release
```

RecoveryStorage calls `Seal()` after every top-level module has returned from
`ObserveMessage`. `Seal()` atomically:

1. marks the message sealed;
2. prevents every later Retain;
3. releases the implicit dispatch reference.

`Retain()` racing with `Seal()` must be serialized. A Retain that loses the race
is a programming error and must fail loudly rather than revive a completed
message.

The dispatch reference guarantees that the count cannot reach zero while
modules are still inspecting the message, even if an asynchronous task finishes
immediately.

The module-facing wrapper is a borrowed view valid only during synchronous
observation. Any code that keeps the message after `ObserveMessage` returns must
first obtain a retained handle. After finalization clears the underlying message,
access through an escaped borrowed view is a programming error.

Consumers that need an independent message copy but must not participate in
RecoveryStorage completion clone the immutable message during synchronous
observation instead of retaining it. QueryRuntime uses this rule for queued live
events: its copy may outlive the RecoveryStorage wrapper, while QueryView
preparation and serving never delay the Message Ack frontier.

### 4.4 Dynamic Child Work

All global retained handles must be created before the owning module returns from
`ObserveMessage`.

If a module cannot know its final child-task count synchronously, it retains one
parent message handle during observation and manages child completion internally:

```text
retained message handle
  -> module-local parent operation
       -> dynamic child task 1
       -> dynamic child task 2
       -> dynamic child task N
  -> parent handle.Release after all children succeed
```

The module-local child counter is not part of the shared message control block.
This keeps the global lifecycle sealed while allowing dynamic scheduling.

Component-local ordering is outside the wrapper. A consumer must satisfy its own
ordering, durability, and continuous-frontier rules before releasing its
retained handle. The count reaching zero proves completion of the retained
operation set; it does not by itself prove any execution order within that set.

### 4.5 Tracker Entry And Message Lifetime

WAL order and checkpoint position belong to `messageack.Tracker`, not to the
message wrapper:

```go
type trackedEntry struct {
    point      utility.WALConsumeCheckpoint
    completed  bool
    controller message.RefCountedImmutableMessageController // nil after completion
}
```

The tracker derives `point` before constructing the wrapper:

```text
point.MessageID = raw.LastConfirmedMessageID()
point.TimeTick  = raw.TimeTick()
```

`point.TimeTick` identifies the logical consumed-through position.
`point.MessageID` is the conservative physical resume anchor used with
`DeliverPolicyStartFrom`. The `LastConfirmedMessageID + StartFrom` pairing may
replay completed messages but must not skip a later TimeTick.

When the sealed message reaches zero references, the wrapper invokes its
finalizer exactly once. The Tracker supplies a finalizer that marks the entry
completed and attempts to advance the continuous completed prefix. After the
finalizer returns, the wrapper clears its underlying `ImmutableMessage`, making
the payload, properties, and Txn children eligible for Go GC even if an earlier
unfinished entry prevents the tracker from removing this lightweight entry.

The tracker therefore retains only `point + completed` for an out-of-order
completed message. Message-object lifetime does not wait for Data checkpoint
advancement.

## 5. Message Lifecycle

```text
data scanner reads message M
  -> tracker creates trackedEntry{point(M)}
  -> construct RefCountedImmutableMessage(M), implicit dispatch refCount = 1
  -> tracker stores the controller in WAL order
  -> dispatch to PChannelRecoveryManager
  -> SegmentViews and TransformLogs retain message handles for actual work
  -> dispatch to broadcastAckModule
  -> BroadcastAck retains a message handle when M has BroadcastHeader
  -> RecoveryStorage calls Seal()
       -> no new Retain allowed
       -> implicit dispatch reference released
  -> asynchronous consumers call handle.Release after success
  -> sealed wrapper reaches refCount == 0
  -> wrapper invokes the Tracker finalizer
  -> tracker marks the entry completed and advances the continuous prefix
  -> wrapper clears the underlying ImmutableMessage
  -> next persist batch may freeze the completed point as its Data checkpoint
```

A module that does not need the message simply does not Retain. No explicit
negative acknowledgement or empty module reference is required.

The tracker advances only a continuous WAL prefix. A completed later entry
cannot pass an earlier entry with outstanding retained handles.

## 6. Consumer Completion Rules

### 6.1 Segment Data

Every affected SegmentView retains its own message handle before asynchronous
data or lifecycle work can run.

Examples include:

- ensure-growing work caused by CreateSegment;
- an Insert retained in an L1 buffer until the containing chunk is durable;
- Txn(Insert) work across multiple SegmentViews;
- flush and commit work required by Flush and flush-style broadcast messages.

One message touching three SegmentViews may therefore own three retained
message handles.
No Segment fanout object is required.

When a storage chunk batches Inserts from several WAL messages, the chunk keeps
one retained handle per contributing message and releases all of them after the
shared write succeeds. Failed writes release none of the contained handles.

Object data may become durable before the corresponding Segment/VChannel dirty
snapshot is persisted to etcd. That is valid: Segment handles express data-side
consumption. RecoveryStorage persists all DirtySnapshots captured for a frozen
batch before it persists that batch's checkpoint.

### 6.2 TransformLog Data

TransformLog retains one message handle for every message whose completion
depends on a chunk flush. This includes:

- Delete and Txn(Delete) entries retained in the open buffer;
- barrier messages that require preceding entries to become durable.

The retained handle is released after the transform-log chunk covering the
message is successfully written and committed into in-memory TransformLog
state. It does not wait for L0 materialization.

```text
TransformLog handle release = required chunk durable
TransformLog materialization = independent downstream output
```

A chunk may release handles belonging to many Delete messages and one or more
barrier messages. If the chunk write fails, every covered handle remains pending.

If the chunk is durable but updated TransformLog metadata has not yet been
persisted to etcd, the data handle may already be released. A persist batch that
includes that completed point still writes the TransformLog DirtySnapshot
before its checkpoint.

### 6.3 Broadcast Ack

For every message carrying a `BroadcastHeader`, `broadcastAckModule` retains one
message handle and keeps broadcast tasks in WAL order.

The queue head may call `Broadcast().Ack` when:

```text
handle.Sealed() && handle.IsExclusive()
```

The exclusive retained handle is the queue head's own BroadcastAck ownership.
Because the message is sealed, no later consumer can appear after this check.

BroadcastAck intentionally does not wait for:

- RecoveryStorage checkpoint publication;
- TransformLog materialization.

`BroadcastHeader.AckSyncUp` does not change this RecoveryStorage precondition.
It tells Coordinator Broadcaster to skip FastAck and wait for the consuming
StreamingNode to call `Broadcast().Ack`. The synchronization is between
Coordinator and RecoveryStorage message consumption; it does not mean that the
RecoveryStorage checkpoint containing the message has already been persisted.

After `Broadcast().Ack` succeeds, the task calls `Release()` on its retained
message handle. Failure keeps the handle and retries the same FIFO queue head.

Broadcast messages that cause Segment or TransformLog work use the same direct
Retain rules as non-broadcast messages. Their handles naturally keep the count
above one until the work is complete.

## 7. Completion Notification And Tracking

The wrapper finalizes only when its internal lifecycle state satisfies:

```text
sealed && refCount == 0
```

The transition to zero notifies the owning tracked entry exactly once. The
tracker must not busy-poll every in-flight message.

The tracker stores entries in WAL order:

```text
[complete, complete, pending, complete, complete]
                     ^
                     completed frontier stops here
```

When the pending message reaches zero, its finalizer marks the entry completed;
the tracker can then remove the whole continuous completed prefix and publish
the newest point as a data checkpoint candidate. Later completed entries keep
only their point and completion bit while waiting for an earlier gap.

## 8. Checkpoint Persist Batch

RecoveryStorage does not maintain per-message metadata barriers or metadata
completion handles. Metadata durability is guaranteed by the ordering of one
frozen persist batch.

### 8.1 Freeze The Batch Boundary

RecoveryStorage serializes observation long enough to freeze:

```text
MetaPoint = latest completely observed WAL point
DataPoint = min(MetaPoint, Ack completed frontier)
```

It then releases the observation lock and consumes every module's stable
DirtySnapshots. Every message at or before `MetaPoint` was fully observed before
the boundary was frozen, so the snapshots necessarily cover its synchronous
metadata mutations.

An asynchronous consumer must follow this order:

```text
perform data-side work
  -> update in-memory recovery metadata
  -> mark the component dirty
  -> retainedMessage.Release()
```

Therefore every message covered by `DataPoint` has completed its asynchronous
metadata mutations before the batch consumes DirtySnapshots.

Messages observed after the boundary is frozen may make a snapshot newer than
`MetaPoint`. Such metadata-ahead persistence is data-safe only when the owning
component satisfies all of these replay conditions:

1. object data referenced by the newer metadata was made durable before the
   metadata state was installed;
2. the component stores a durable data cursor separately from metadata-only
   lifecycle progress where replay needs that distinction;
3. replay from the older Data checkpoint can apply or safely skip each message
   against the recovered future state without skipping unfinished data work;
4. tombstone finalization and catalog deletion wait until the persisted Meta
   and Data checkpoints cover the tombstone;
5. external lifecycle operations accept retry, even when they do not provide
   exactly-once physical side effects.

The current recovery model satisfies these conditions by using Segment data
checkpoints, recovered final-commit work, TransformLog checkpoint metadata,
and checkpoint-bounded tombstone cleanup. Repeated Segment object writes may
leave unreferenced objects, and repeated lifecycle calls may repeat external
events; those require reconciliation or GC but must not lose referenced data.

With those conditions, the batch safety relation is:

```text
metadata newer than checkpoint = safe
checkpoint newer than metadata = unsafe
```

The same batch must never refresh its checkpoint beyond the frozen boundary.

### 8.2 Persist Order

One batch is persisted in this order:

```text
freeze MetaPoint and DataPoint
  -> consume stable DirtySnapshots
  -> persist every DirtySnapshot to etcd
  -> after all succeed, call MarkPersisted()
  -> persist WALCheckpoint{MetaPoint, DataPoint}
```

If any DirtySnapshot write fails, the checkpoint is not written. If all
DirtySnapshots succeed but checkpoint persistence fails, metadata is ahead of
the old checkpoint and WAL replay remains safe.

`MarkPersisted()` updates component-local dirty generations and persisted state
used by cleanup or recovery. It does not unlock a per-message checkpoint
dependency.

Message Ack is the only data-side completion input. The recovery Module API
contains no generic checkpoint barrier, data frontier, or Observe result.
Component-specific timeticks may still exist for storage layout, task ordering,
materialization, subscription, or cleanup, but they are not RecoveryStorage
checkpoint inputs.

## 9. Retry, Close, And Recovery

Tracked entries and ref-count controllers are not persisted as a separate
per-message table in the first implementation. Recovery starts the data scanner
from the persisted data checkpoint and reconstructs entries and wrappers for
replayed messages.

The wrapper has no failed state and `Release()` has no error result. Release
means only that the retained operation reached its success condition. An
operation that has not succeeded, context cancellation, and RecoveryStorage
close all keep the retained handle live. Close does not release outstanding
handles and does not require the count to reach zero. When the process exits,
the in-memory tracker is discarded while the persisted Data checkpoint remains
behind; restart reconstructs fresh entries and wrappers by WAL replay.

| Interruption point | Recovery rule |
|---|---|
| Before RecoveryStorage seals the message | The implicit dispatch reference prevents completion. |
| A module tries to Retain after Seal | Programming error; the message must not be revived. |
| Async object write does not succeed | Its retained handle remains live; any retry remains component local. |
| Object write succeeds, DirtySnapshot persistence fails | Data handles may release, but the batch checkpoint is not written; WAL replay republishes or reconciles metadata. |
| DirtySnapshots persist but checkpoint persistence fails | Metadata is ahead of the old checkpoint; replay from the old checkpoint remains safe. |
| Broadcast RPC succeeds before checkpoint persistence | Replay may send the idempotent broadcast Ack again. |
| Process crashes with in-flight wrappers | Entries and wrappers are rebuilt by WAL replay from the older persisted data checkpoint. |
| Context is canceled or RecoveryStorage closes | Outstanding handles remain retained; no incomplete operation is converted into `Release()`. |
| Later message completes before an earlier message | Tracker keeps the later point and completion bit until the earlier entry completes; its underlying message may already be GC eligible. |

## 10. Observability

The shared message controller stores only the total reference count. Completion
correctness must not depend on owner names or reason categories.

Long-held reference diagnosis can be added independently through:

- module-level gauges for retained handle counts;
- slow-entry logging with WAL point and age;
- debug-only owner labels or Retain call sites;
- task scheduler diagnostics.

Diagnostics must not require a production reason map or change zero-count
semantics.

## 11. Invariants

1. Every data-scanner message has one ref-counted wrapper and one Tracker entry.
2. The wrapper contains no checkpoint point; the Tracker entry owns WAL order,
   point, and completion state.
3. Every new wrapper starts open with one implicit dispatch reference.
4. All global Retains happen synchronously before RecoveryStorage calls Seal.
5. Seal atomically forbids future Retains and releases the dispatch reference.
6. Every Retain returns one distinct retained message handle with idempotent
   Release.
7. A sealed wrapper is complete only when its reference count reaches zero.
8. Finalization clears the underlying `ImmutableMessage` after notifying the
   Tracker entry, independently from continuous checkpoint advancement.
9. The borrowed wrapper is valid only during synchronous observation; every
   asynchronous owner holds a retained handle.
10. Segment completion means all message-induced segment data work is durable.
11. TransformLog completion means the required chunk is durable, not
   materialized.
12. BroadcastAck waits for a sealed message with only its own retained handle
    and remains FIFO.
13. A persist batch writes every captured DirtySnapshot before its frozen
    checkpoint.
14. A batch DataPoint never passes its MetaPoint or the continuous Ack completed
    frontier frozen for that batch.
15. Async consumers update metadata and mark it dirty before releasing their
    retained handles.
16. QueryView references never participate in WAL Ack completion.
17. Ack does not define component-local task order; consumers release handles only
    after satisfying their own ordered success conditions.
18. The wrapper has no failure completion: retry, cancellation, and close retain
    outstanding handles.
19. Checkpoint MessageID uses `LastConfirmedMessageID`, and recovery resumes
    with `DeliverPolicyStartFrom`.
