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
- durable persistence of every in-flight AckRecord in the first implementation.

An AckRecord is a logical completion group. Every actual consumer obtains its
own Ref. Physical task layout remains module local: one message may create many
tasks, and one storage task may batch work from many messages.

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
keeps AckRecords in WAL order and observes completion.

## 4. Reference-Count Model

### 4.1 Record And Ref

Each data-scanner message owns one AckRecord:

```go
type Record interface {
    Point() utility.WALConsumeCheckpoint
    Retain() Ref
    Seal()
    Sealed() bool
    RefCount() int64
    Completed() bool
}

type Ref interface {
    Done()
}
```

`Point()` is the logical consumed-through position of the message:

```text
Point.MessageID = message.LastConfirmedMessageID()
Point.TimeTick  = message.TimeTick()
```

`Point.TimeTick` identifies the last message completed in consumer TimeTick
order. `Point.MessageID` is a conservative physical resume anchor, not
necessarily the physical MessageID of that last consumed message. Recovery
uses `DeliverPolicyStartFrom(Point.MessageID)`. The
`LastConfirmedMessageID + StartFrom` pairing may replay already completed
messages, but it must not skip any later message.

`NewRecord(point)` starts with one implicit dispatch reference:

```text
sealed   = false
refCount = 1
```

Every `Retain()` increments the count and returns a unique Ref token. The token
uses once-only completion, so repeated `Done()` calls are harmless and decrement
the count at most once.

There is no reason enum, reason bitset, reason map, or global per-module
sub-count. Recovery correctness depends only on the total count.

### 4.2 Open And Sealed Phases

AckRecord has two phases:

```text
Open:
  synchronous observers may Retain

Sealed:
  Retain is forbidden
  existing Refs may only move toward Done
```

RecoveryStorage calls `Seal()` after every top-level module has returned from
`ObserveMessage`. `Seal()` atomically:

1. marks the record sealed;
2. prevents every later Retain;
3. releases the implicit dispatch reference.

`Retain()` racing with `Seal()` must be serialized. A Retain that loses the race
is a programming error and must fail loudly rather than revive a completed
record.

The dispatch reference guarantees that the count cannot reach zero while
modules are still inspecting the message, even if an asynchronous task finishes
immediately.

### 4.3 Dynamic Child Work

All global Refs must be retained before the owning module returns from
`ObserveMessage`.

If a module cannot know its final child-task count synchronously, it retains one
parent Ref during observation and manages child completion internally:

```text
AckRecord Ref
  -> module-local parent operation
       -> dynamic child task 1
       -> dynamic child task 2
       -> dynamic child task N
  -> parent Ref.Done after all children succeed
```

The module-local child counter is not part of the global AckRecord. This keeps
the global lifecycle sealed while allowing dynamic scheduling.

Component-local ordering is outside AckRecord. A consumer must satisfy its own
ordering, durability, and continuous-frontier rules before releasing a Ref.
RefCount reaching zero proves completion of the retained operation set; it does
not by itself prove any execution order within that set.

## 5. Message Lifecycle

```text
data scanner reads message M at WAL point P
  -> NewRecord(P), implicit dispatch refCount = 1
  -> tracker records AckRecord in WAL order
  -> wrap M with its AckRecord
  -> dispatch to PChannelRecoveryManager
  -> SegmentViews and TransformLogs Retain for actual work
  -> dispatch to broadcastAckModule
  -> BroadcastAck Retains when M has BroadcastHeader
  -> RecoveryStorage calls Seal()
       -> no new Retain allowed
       -> implicit dispatch ref released
  -> asynchronous consumers call Ref.Done after success
  -> sealed record reaches refCount == 0
  -> tracker advances the continuous completed prefix
  -> next persist batch may freeze the completed point as its Data checkpoint
```

A module that does not need the message simply does not Retain. No explicit
negative acknowledgement or empty module reference is required.

The tracker advances only a continuous WAL prefix. A completed later record
cannot pass an earlier record with outstanding Refs.

## 6. Consumer Completion Rules

### 6.1 Segment Data

Every affected SegmentView retains its own Ref before asynchronous data or
lifecycle work can run.

Examples include:

- ensure-growing work caused by CreateSegment;
- an Insert retained in an L1 buffer until the containing chunk is durable;
- Txn(Insert) work across multiple SegmentViews;
- flush and commit work required by Flush and flush-style broadcast messages.

One message touching three SegmentViews may therefore own three Segment Refs.
No Segment fanout object is required.

When a storage chunk batches Inserts from several WAL messages, the chunk keeps
one Ref per contributing message and releases all of them after the shared write
succeeds. Failed writes release none of the contained Refs.

Object data may become durable before the corresponding Segment/VChannel dirty
snapshot is persisted to etcd. That is valid: Segment Refs express data-side
consumption. RecoveryStorage persists all DirtySnapshots captured for a frozen
batch before it persists that batch's checkpoint.

### 6.2 TransformLog Data

TransformLog retains one Ref for every message whose completion depends on a
chunk flush. This includes:

- Delete and Txn(Delete) entries retained in the open buffer;
- barrier messages that require preceding entries to become durable.

The Ref is released after the transform-log chunk covering the message is
successfully written and committed into in-memory TransformLog state. It does
not wait for L0 materialization.

```text
TransformLog Ref completion = required chunk durable
TransformLog materialization = independent downstream output
```

A chunk may release Refs belonging to many Delete messages and one or more
barrier messages. If the chunk write fails, every covered Ref remains pending.

If the chunk is durable but updated TransformLog metadata has not yet been
persisted to etcd, the data Ref may be complete. A persist batch that includes
that completed point still writes the TransformLog DirtySnapshot before its
checkpoint.

### 6.3 Broadcast Ack

For every message carrying a `BroadcastHeader`, `broadcastAckModule` retains one
Ref and keeps broadcast tasks in WAL order.

The queue head may call `Broadcast().Ack` when:

```text
record.Sealed() && record.RefCount() == 1
```

The remaining Ref is the queue head's own BroadcastAck Ref. Because the record
is sealed, no later consumer can appear after this check.

BroadcastAck intentionally does not wait for:

- RecoveryStorage checkpoint publication;
- TransformLog materialization.

`BroadcastHeader.AckSyncUp` does not change this RecoveryStorage precondition.
It tells Coordinator Broadcaster to skip FastAck and wait for the consuming
StreamingNode to call `Broadcast().Ack`. The synchronization is between
Coordinator and RecoveryStorage message consumption; it does not mean that the
RecoveryStorage checkpoint containing the message has already been persisted.

After `Broadcast().Ack` succeeds, the task calls its Ref's `Done()`. Failure
keeps the Ref and retries the same FIFO queue head.

Broadcast messages that cause Segment or TransformLog work use the same direct
Retain rules as non-broadcast messages. Their Refs naturally keep the count
above one until the work is complete.

## 7. Completion Notification And Tracking

`Completed()` is true only when:

```text
record.Sealed() && record.RefCount() == 0
```

The transition to zero should notify RecoveryStorage or the tracker exactly
once. The implementation may use an on-zero callback or a lightweight notifier;
RecoveryStorage must not busy-poll every in-flight record.

The tracker stores records in WAL order:

```text
[complete, complete, pending, complete, complete]
                     ^
                     completed frontier stops here
```

When the pending record reaches zero, the tracker can remove the whole completed
prefix and publish the newest point as a data checkpoint candidate.

## 8. Checkpoint Persist Batch

RecoveryStorage does not maintain per-message metadata barriers or metadata Ack
Refs. Metadata durability is guaranteed by the ordering of one frozen persist
batch.

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
  -> Ref.Done()
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

AckRecords are not persisted as a separate per-message table in the first
implementation. Recovery starts the data scanner from the persisted data
checkpoint and reconstructs records for replayed messages.

AckRecord has no failed state and Ref has no error result. `Done()` means only
that the retained operation reached its success condition. An operation that
has not succeeded, context cancellation, and RecoveryStorage close all keep the
Ref retained. Close does not release outstanding Refs and does not require them
to reach zero. When the process exits, the in-memory tracker is discarded while
the persisted Data checkpoint remains behind; restart reconstructs fresh
AckRecords by WAL replay.

| Interruption point | Recovery rule |
|---|---|
| Before RecoveryStorage seals the record | The implicit dispatch Ref prevents completion. |
| A module tries to Retain after Seal | Programming error; the record must not be revived. |
| Async object write does not succeed | Its Ref remains pending; any retry remains component local. |
| Object write succeeds, DirtySnapshot persistence fails | Data Refs may complete, but the batch checkpoint is not written; WAL replay republishes or reconciles metadata. |
| DirtySnapshots persist but checkpoint persistence fails | Metadata is ahead of the old checkpoint; replay from the old checkpoint remains safe. |
| Broadcast RPC succeeds before checkpoint persistence | Replay may send the idempotent broadcast Ack again. |
| Process crashes with in-flight AckRecords | Records are rebuilt by WAL replay from the older persisted data checkpoint. |
| Context is canceled or RecoveryStorage closes | Outstanding Refs remain retained; no incomplete operation is converted into `Done()`. |
| Later message completes before an earlier message | Tracker retains the later completion until the earlier record reaches zero. |

## 10. Observability

The core AckRecord stores only the total reference count. Completion correctness
must not depend on owner names or reason categories.

Long-held reference diagnosis can be added independently through:

- module-level gauges for retained Ref counts;
- slow-record logging with WAL point and age;
- debug-only owner labels or Retain call sites;
- task scheduler diagnostics.

Diagnostics must not require a production reason map or change zero-count
semantics.

## 11. Invariants

1. Every data-scanner message has one AckRecord and one WAL checkpoint point.
2. Every new record starts open with one implicit dispatch reference.
3. All global Retains happen synchronously before RecoveryStorage calls Seal.
4. Seal atomically forbids future Retains and releases the dispatch reference.
5. Every Retain returns one idempotent Ref token.
6. A sealed record is complete only when its reference count reaches zero.
7. Segment completion means all message-induced segment data work is durable.
8. TransformLog completion means the required chunk is durable, not
   materialized.
9. BroadcastAck waits for a sealed record with only its own Ref and remains FIFO.
10. A persist batch writes every captured DirtySnapshot before its frozen
    checkpoint.
11. A batch DataPoint never passes its MetaPoint or the continuous Ack completed
    frontier frozen for that batch.
12. Async consumers update metadata and mark it dirty before releasing their
    Ref.
13. QueryView references never participate in WAL Ack completion.
14. Ack does not define component-local task order; consumers release Refs only
    after satisfying their own ordered success conditions.
15. AckRecord has no failure completion: retry, cancellation, and close retain
    outstanding Refs.
16. Checkpoint MessageID uses `LastConfirmedMessageID`, and recovery resumes
    with `DeliverPolicyStartFrom`.
