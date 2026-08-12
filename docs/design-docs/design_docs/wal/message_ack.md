# WAL Message Ack Design

This document defines the message lifetime and completion model used by
RecoveryStorage while persisting WAL data into object storage and recovery
metadata into etcd. QueryView and QueryRuntime resource references are outside
this design.

## 1. Goals And Boundaries

One WAL message may cause work in multiple VChannels, SegmentViews, and
TransformLogs. RecoveryStorage needs to advance its Data checkpoint only after
the continuous WAL prefix has completed all required local work. It also needs
to acknowledge broadcast messages to StreamingCoord after local consumers have
finished.

The model uses ordinary reference counting. It does not use a module reason
map, reason bitset, per-module frontier, DataBarrier, `Seal`, or a special
completion state on the message wrapper. Ack does not define asynchronous task
ordering. Segment and TransformLog retain and release according to their own
ordering and retry rules. BroadcastAck only releases the data-scanner Owner and
tracks the separate Coordinator ACK condition.

RecoveryStorage has these runtime owners:

```text
RecoveryStorage
  +-- messageack.Tracker
  +-- PChannelRecoveryManager
  |     +-- VChannelRecoveryModule*
  |           +-- SegmentView*
  |           +-- TransformLog
  +-- broadcastAck sink
```

SegmentView and TransformLog are VChannel-owned components, not independent
top-level RecoveryStorage modules. BroadcastAck is a dedicated RecoveryStorage
sink rather than a `moduleapi.Module`: it has no Meta-only observer, mode
transition, or dirty snapshot.

## 2. RefCountedImmutableMessage

The generic wrapper is implemented in `pkg/streaming/util/message`. It owns the
underlying `ImmutableMessage` until the final reference is released.

```go
type RefCountedImmutableMessageOwner interface {
    Message() ImmutableMessage
    Clone() RetainedImmutableMessage
    Release()
}

type RetainedImmutableMessage interface {
    Message() ImmutableMessage
    Clone() RetainedImmutableMessage
    Release()
}
```

`NewRefCountedImmutableMessageOwner(msg, finalizer)` takes ownership of `msg`
and creates the unique root reference. The constructor does not copy the
message. The finalizer is invoked exactly once, after the reference count
reaches zero, and is the wrapper's only completion callback.

The Owner is the root reference. A retained handle is one independent
reference. `Owner.Release()` releases the root reference and invalidates that
Owner object immediately. Other retained handles remain valid until they are
released. `RetainedImmutableMessage.Release()` is idempotent for that handle.

The wrapper itself does not promise that one handle can be used concurrently.
Callers that need independent concurrent access call `Clone()` before handing a
handle to another goroutine. The underlying immutable message is shared by all
clones and is safe to access while the caller owns a valid handle. A transaction
is always retained as one complete message; a child returned by `RangeOver` does
not have an independent lifetime.

`Message()` exposes the underlying `ImmutableMessage` through the handle. It is
invalid to call it after that handle has been released. When the final
reference is released, the wrapper clears its pointer to the underlying
message after running the finalizer, allowing the message, payload, properties,
and complete transaction to become eligible for Go GC.

Typed specialization is a view over the same owner or retained handle:

```go
type OwnedMessage[T ImmutableMessage] struct { /* typed message + owner */ }
type RetainedMessage[T ImmutableMessage] struct { /* typed message + handle */ }
```

These helpers carry no second lifecycle and no separate Ack argument. A typed
retained value must release its handle when its asynchronous operation ends.

## 3. Tracker

`messageack.Tracker` owns WAL-order tracking and the message itself. The
message wrapper does not contain a checkpoint point.

Conceptually each entry is:

```go
type trackedEntry struct {
    point                 utility.WALConsumeCheckpoint
    message               message.ImmutableMessage
    broadcastAckRequired  bool
    consumersDone         chan struct{}
    consumersCompleted    bool
    broadcastAckCompleted bool
    completed             bool
}
```

`Tracker.Track(raw)` derives and stores:

```text
point.MessageID = raw.LastConfirmedMessageID()
point.TimeTick  = raw.TimeTick()
```

It then creates the Owner with a finalizer. The finalizer marks the local
consumer phase complete and closes `ConsumersDone`. The Tracker does not remove
the entry until every required completion condition is true.

The returned `TrackedMessage` supplies the internal coordination boundary:

```go
type TrackedMessage struct {
    Message() ImmutableMessage
    ConsumersDone() <-chan struct{}
    RequiresBroadcastAck() bool
    CompleteBroadcastAck()
}
```

`TrackedMessage` is not a second message owner. It is a Tracker handle used by
RecoveryStorage sinks. `RequiresBroadcastAck` is fixed when the entry is
created from the original message and does not change after ACK completion.
`CompleteBroadcastAck` is idempotent and is called only after Coordinator
`Broadcast().Ack` succeeds. For a non-broadcast message the Tracker initializes
the broadcast condition as already complete.

An entry is complete when:

```text
consumersCompleted && broadcastAckCompleted
```

Only then does the Tracker remove a continuous completed prefix and publish a
new Data checkpoint candidate. Out-of-order entries remain in the Tracker with
their message and point until the prefix reaches them. This intentionally keeps
the message available for future message-level Defrag work. Once an entry is
removed from the completed prefix, its message reference is cleared.

## 4. Data Scanner Flow

The data scanner processes a message while RecoveryStorage's observation mutex
is held:

```text
read M
  -> owner, tracked = Tracker.Track(M)
  -> PChannelRecoveryManager.ObserveDataMessage(owner)
       -> affected VChannel modules
       -> actual Segment/TransformLog consumers synchronously Clone(owner)
       -> QueryRuntime receives a normal immutable copy
  -> RecoveryStorage updates Meta checkpoint from M
  -> BroadcastAck accepts owner and tracked
       -> owner.Release() unconditionally
       -> if tracked.RequiresBroadcastAck(), enqueue tracked
```

BroadcastAck is the final top-level Owner-release sink. No module needs to
acknowledge a message just because it observed it. A module that has no work for
a message simply does not clone it.

All clones required by asynchronous work are created before the corresponding
`ObserveDataMessage` call returns. There is no later global retain operation.

For a non-broadcast message, BroadcastAck releases the Owner and does nothing
else. The last actual consumer release invokes the Tracker finalizer, which
completes the entry. For a broadcast message, the same Owner release happens
immediately, while the Tracker's fixed broadcast requirement keeps the entry
alive for the ACK task.

For a broadcast message, BroadcastAck does not read, retain, or clone the
Owner. It stores only the `TrackedMessage` in its FIFO task and immediately
releases the Owner. The task waits for `TrackedMessage.ConsumersDone`, calls
Coordinator ACK with the Tracker-owned message, and calls
`CompleteBroadcastAck` only after ACK succeeds. The sink therefore performs
the top-level Owner release but does not become an Owner or add an ownership
condition of its own.

## 5. Consumer Rules

### Segment

SegmentView clones for concrete asynchronous work:

- CreateSegment ensure-growing work;
- Insert data retained in a pending object-storage chunk;
- a whole Txn retained by every affected SegmentView;
- Flush, ManualFlush, FlushAll, and other lifecycle operations that flush or
  commit data.

The handle is released only after the required object write or lifecycle side
effect succeeds. Before release, the consumer installs the resulting in-memory
metadata and marks the Segment dirty. A failed or canceled task keeps the
handle.

One object-storage chunk can contain messages from many WAL entries. It keeps
one retained handle per entry and releases all of them only after the shared
write succeeds.

### TransformLog

TransformLog clones for Delete, Txn(Delete), and barrier messages that require
preceding Delete entries to be flushed. It releases each handle after the
covering TransformLog chunk is durably written and committed into in-memory
TransformLog state. It marks its dirty metadata before release.

TransformLog does not wait for materialization. L0 materialization is a
separate downstream operation and is not a message-consumer reference.

### QueryRuntime

QueryRuntime receives a synchronous deep copy of the immutable message when it
needs to enqueue a live event. It never retains a RecoveryStorage handle and
never delays the Data checkpoint.

### BroadcastAck

BroadcastAck is a completion sink, not a data consumer. It does not add a
reference. It releases the Owner immediately, then uses `TrackedMessage` to
wait for local consumer completion and perform FIFO Coordinator ACK. Its
Coordinator ACK completion is itself required for a broadcast Tracker entry.

## 6. Checkpoint Persistence

RecoveryStorage keeps separate logical points:

```text
MetaPoint = latest completely observed WAL point
DataPoint = min(MetaPoint, Tracker completed continuous frontier)
```

Before each batch, RecoveryStorage freezes the checkpoint boundary, then
collects stable DirtySnapshots. An asynchronous consumer follows:

```text
object-storage/lifecycle work succeeds
  -> update in-memory recovery metadata
  -> mark metadata dirty
  -> Release retained handle
```

The persist order is:

```text
freeze MetaPoint and DataPoint
  -> persist every captured DirtySnapshot
  -> mark those snapshots persisted
  -> persist the WALCheckpoint
```

The checkpoint uses `LastConfirmedMessageID` and recovery resumes with
`DeliverPolicyStartFrom`. Replaying an already completed message is acceptable;
advancing past an unfinished entry is not.

Meta-only replay does not create message Ack entries. It consumes DirtySnapshot
state and advances the Meta checkpoint as part of bounded metadata recovery.

## 7. Broadcast And AckSyncUp

Coordinator FastAck remains an optimization. If `AckSyncUp` is false, the
Coordinator may self-ack after WAL append. If it is true, the Coordinator skips
FastAck and waits for RecoveryStorage's Coordinator ACK. This does not alter
RecoveryStorage's local ordering or checkpoint persistence.

RecoveryStorage BroadcastAck does not wait for either Meta checkpoint or Data
checkpoint persistence. It waits only for the actual local message consumers,
then performs the idempotent Coordinator ACK. The Tracker entry is not complete
until that ACK succeeds.

## 8. Retry, Close, And Recovery

There is no failure state in the message wrapper or Tracker entry. A retained
handle is released only after its operation succeeds. Coordinator ACK failures
keep the FIFO task pending and leave `broadcastAckCompleted` false. Close does
not convert incomplete work into completion.

After restart, the persisted Data checkpoint is the conservative resume anchor.
Recovery reconstructs the Tracker entries, messages, and all required handles
by replaying the WAL. Coordinator ACK is idempotent, so an ACK that succeeded
before a crash may be repeated safely.

## 9. Invariants

1. Each data-scanner message has one Tracker entry and one Owner.
2. The Tracker entry owns the WAL point and retains the original message until
   the entry leaves the completed prefix.
3. Each actual asynchronous consumer owns one retained message handle.
4. Owner `Release` is the only top-level dispatch release; BroadcastAck does not
   add a reference.
5. The finalizer closes local `ConsumersDone` exactly once.
6. Broadcast ACK success is required before a broadcast entry can complete.
7. Data checkpoint advancement uses only the continuous completed entry prefix,
   bounded by the frozen MetaPoint.
8. TransformLog handle release waits for chunk durability, not materialization.
9. Async consumers mark metadata dirty before releasing their handles.
10. QueryRuntime copies messages and never participates in WAL Ack.
11. Txn messages are retained and completed as one whole message.
12. Ack observes completion but does not define asynchronous task ordering.
