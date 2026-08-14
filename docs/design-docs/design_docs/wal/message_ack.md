# WAL Message Ack Design

This document defines how RecoveryStorage tracks one WAL message until every
required persistence consumer and Coordinator broadcast acknowledgement has
finished. QueryView and QueryRuntime resource lifetimes are outside this model.

## 1. Scope

One WAL message may create asynchronous work in several SegmentViews and
TransformLogs. RecoveryStorage may advance its Data checkpoint only after the
continuous WAL prefix has completed that work. Broadcast messages must also be
acknowledged to StreamingCoord after their local consumers finish.

The design uses ordinary reference counting. It has no reason map or bitset,
module frontier, DataBarrier, `Seal`, explicit failure state, or generic
`moduleapi.Module` consumer API. Ack observes completion; component-local
schedulers and preconditions define asynchronous execution order.

The runtime ownership is:

```text
RecoveryStorage
  +-- messageack.Tracker
  +-- PChannelRecoveryManager
  |     +-- VChannelRecoveryModule*
  |           +-- SegmentView*
  |           +-- TransformLog
  +-- BroadcastAck
```

## 2. Message Handles

The common wrapper lives in `pkg/streaming/util/message`:

```go
type OwnedImmutableMessage interface {
    Message() ImmutableMessage
    Clone() RetainedImmutableMessage
    Exclusive() <-chan struct{}
    Release()
}

type RetainedImmutableMessage interface {
    Message() ImmutableMessage
    Clone() RetainedImmutableMessage
    Release()
}
```

`NewOwnedImmutableMessage(msg, finalizer)` creates the unique Owner root with
reference count one. It does not copy `msg`.

- `Owner.Clone()` and `Retained.Clone()` create independently releasable
  Retained handles.
- `Owner.Release()` releases only the root reference and invalidates that Owner
  handle.
- `Retained.Release()` releases only that handle and is idempotent for the
  handle.
- The finalizer runs exactly once when the total reference count reaches zero.
- A handle must not be used after its own `Release()`.
- One handle is not promised to be concurrently safe. A caller gives each
  independent asynchronous unit its own clone.

`Owner.Exclusive()` returns the current Owner-exclusive event. The returned
channel is closed exactly while the Owner is the only remaining tracked
reference, meaning the reference count is one. A new Retained clone created
from an exclusive Owner installs a new open event; the last Retained release
closes that event when the count returns to one. Exclusive is not completion:
the finalizer still waits for `Owner.Release()` and reference count zero.

The Owner lifecycle is linear at the top level. Once BroadcastAck accepts the
Owner, no caller may clone it again. This makes the Exclusive transition a
stable precondition for the queued broadcast task.

`Message()` exposes the shared underlying `ImmutableMessage`. An ordinary Go
interface value obtained before handle release may outlive the handle. Such a
reference affects Go GC reachability but does not participate in RecoveryStorage
completion. The wrapper clears its own message pointer after finalization; the
object becomes GC-eligible only after all ordinary Go references also vanish.

Transactions are retained as one whole `ImmutableTxnMessage`. Child messages
visited through `RangeOver` never get independently tracked lifetimes.

### Typed Handles

Typed Owner and Retained interfaces are direct peers of
`SpecializedImmutableMessage`; they do not add a nested generic ownership layer.
Retained specializations provide `CloneHandle()` when an asynchronous consumer
needs an untyped Retained clone:

```go
type SpecializedRetainedImmutableMessage[H proto.Message, B proto.Message] interface {
    Message() SpecializedImmutableMessage[H, B]
    Clone() SpecializedRetainedImmutableMessage[H, B]
    CloneHandle() RetainedImmutableMessage
    Release()
}
```

Generated `MustAsRetainedImmutableXxx` helpers bind an existing Retained handle
to its specialized immutable message. This is a view over the same handle, not
a new reference.

## 3. Tracker

`messageack.Tracker` owns WAL-order checkpoint state and retains the raw message
only while its Owner graph is live:

```go
type trackedEntry struct {
    point     utility.WALConsumeCheckpoint
    message   message.ImmutableMessage
    completed bool
}
```

`Tracker.Track(raw)` stores:

```text
point.MessageID = raw.LastConfirmedMessageID()
point.TimeTick  = raw.TimeTick()
```

It appends the entry and returns an Owner whose finalizer:

1. immediately clears this entry's `message` reference;
2. marks this entry completed;
3. removes the continuous completed prefix;
4. advances the completed checkpoint monotonically to the last removed point.

Tracker does not expose a `TrackedMessage`, consumer event, broadcast flag, or
Coordinator Ack method. BroadcastAck interacts only with the Owner.

Completion may be out of order. For example:

```text
M1 incomplete: message != nil, completed=false
M2 complete:   message == nil, completed=true
M3 complete:   message == nil, completed=true
```

M2 and M3 do not retain their payloads merely because M1 blocks checkpoint
advancement. Their entries remain only as lightweight ordered checkpoint
records until M1 completes.

## 4. Data Scanner Flow

RecoveryStorage serializes observation of one data-scanner message:

```text
raw message M
  -> Owner O = Tracker.Track(M)
  -> dispatch Retained D = O.Clone()
  -> PChannelRecoveryManager.ObserveMessage(D)
       -> synchronous routing through affected VChannels
       -> Segment/TransformLog clone D only for actual async work
       -> QueryRuntime receives plain ImmutableMessage
  -> D.Release()
  -> update Meta checkpoint from M
  -> BroadcastAck.Accept(O)
```

For PChannel-wide dispatch, the manager gives each VChannel an independent
Retained clone and releases it after that VChannel's synchronous observation.
Every asynchronous clone must therefore be created before its Observe call
returns.

BroadcastAck becomes the sole Owner holder after `Accept`:

```text
non-broadcast:
  O.Release()

broadcast:
  enqueue O in FIFO order
  wait until <-O.Exclusive()
  Coordinator Ack(O.Message())
  on success: O.Release()
  on failure: keep O and retry the same queue head
```

For a broadcast message, reference count zero therefore proves both that all
local Retained consumers finished and that Coordinator Ack succeeded.

## 5. Meta-Only Flow

Meta-only recovery uses the same Retained observation contract but does not
enter Tracker or BroadcastAck:

```text
raw message M
  -> temporary Owner O = NewOwnedImmutableMessage(M, nil)
  -> dispatch Retained D = O.Clone()
  -> PChannelRecoveryManager.ObserveMessage(D)
  -> D.Release()
  -> O.Release()
  -> advance Meta checkpoint
```

Modules are still in MetaOnly mode, so they rebuild metadata and transaction
state without scheduling data persistence work. The bounded Meta scanner later
persists the resulting DirtySnapshots before its checkpoint.

## 6. Consumer Completion

### Segment

SegmentView clones a Retained handle only for concrete asynchronous work such
as ensure-growing, Insert chunk persistence, and flush/final-commit lifecycle
work. It releases the clone after the required object or lifecycle operation
succeeds, after installing resulting metadata and marking the view dirty.

One chunk may hold handles for several WAL messages. A failed or retrying task
keeps all uncovered handles live.

### TransformLog

TransformLog clones for Delete, Txn(Delete), or a barrier that requires
preceding Delete data to be flushed. It releases covered handles after the
TransformLog chunk is durable and committed into dirty in-memory metadata.

L0 materialization is a separate downstream operation. It does not retain the
source message and does not delay Message Ack or BroadcastAck.

### QueryRuntime

QueryRuntime receives the plain `ImmutableMessage` through its live event. It
uses its own TimeTick filtering and ordering and never receives an Owner or
Retained persistence handle.

## 7. Checkpoint Persistence

RecoveryStorage freezes:

```text
MetaPoint = latest completely observed WAL point
DataPoint = min(MetaPoint, Tracker completed continuous point)
```

Every asynchronous consumer follows:

```text
required work succeeds
  -> install recovery metadata
  -> mark component dirty
  -> Release retained handle
```

The persist batch then runs:

```text
freeze MetaPoint and DataPoint
  -> consume stable DirtySnapshots
  -> persist all DirtySnapshots
  -> MarkPersisted
  -> persist WALCheckpoint last
```

The checkpoint MessageID is `LastConfirmedMessageID`; recovery resumes with
`DeliverPolicyStartFrom`. Replay may conservatively repeat a completed message,
but an unfinished message must never be skipped. DataPoint is monotonic and is
clamped against the already persisted checkpoint.

## 8. AckSyncUp, Retry, And Close

`AckSyncUp` only disables Coordinator FastAck and makes StreamingCoord wait for
the consuming-side Ack. It does not alter RecoveryStorage observation,
checkpoint persistence, TransformLog materialization, or QueryRuntime startup.

There is no explicit message failure state. Incomplete work is represented by
an unreleased handle. Coordinator Ack failure keeps the broadcast Owner at the
FIFO head and retries. Close cancels waiters and retries but does not release an
unfinished Owner or fabricate completion. WAL replay reconstructs all state
from the persisted Data checkpoint; Coordinator Ack is idempotent.

## 9. Invariants

1. Every data-scanner message has one Tracker entry and one Owner.
2. Meta-only messages use a temporary Owner but no Tracker entry.
3. Tracker owns only ordered points and a per-entry message reference until
   finalization; it owns no BroadcastAck state.
4. Each asynchronous Segment or TransformLog unit owns an independent Retained
   clone created during synchronous observation.
5. BroadcastAck is the final Owner holder and waits for Owner exclusivity before
   Coordinator Ack.
6. Finalization occurs only at reference count zero and clears each completed
   message independently of checkpoint-prefix advancement.
7. Async consumers mark metadata dirty before releasing their handles.
8. TransformLog completion requires chunk durability, not materialization.
9. QueryRuntime and Go GC reachability are independent of persistence Ack.
10. Txn messages are retained and completed as one whole message.
11. Data checkpoint advancement uses only the continuous completed Tracker
    prefix and never moves backward.
12. Ack does not define asynchronous task execution order.
