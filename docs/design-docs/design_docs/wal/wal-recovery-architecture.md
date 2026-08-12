# WAL Recovery Architecture

This document describes how StreamingNode RecoveryStorage reconstructs one
PChannel, persists WAL-derived state into object storage and etcd, and publishes
restart checkpoints. Message completion is defined by
[WAL Message Ack Design](message_ack.md).

## 1. Runtime Ownership

```text
RecoveryStorage
  +-- messageack.Tracker
  +-- PChannelRecoveryManager
  |     +-- VChannelRecoveryModule*
  |           +-- VChannelView
  |           +-- SegmentView*
  |           +-- TransformLog
  |           +-- QueryRuntime bridge
  +-- broadcastAck sink
```

Only `PChannelRecoveryManager` is registered as a top-level RecoveryStorage
module. SegmentView and TransformLog are internal VChannel components.
BroadcastAck is a dedicated RecoveryStorage sink, not a `moduleapi.Module`.
Snapshot names such as `ModuleNameSegment` and `ModuleNameTransformLog`
classify catalog records; they do not imply independent runtime modules.

QueryView resource references are temporary serving state. They do not affect
WAL message references, broadcast ACK, or RecoveryStorage checkpoints.

## 2. Persisted State

RecoveryStorage persists:

- `WALCheckpoint`, containing Meta and Data consume points;
- VChannel collection, partition, schema, and lifecycle metadata;
- Segment assignment, durability, sealed-version, and tombstone metadata;
- TransformLog chunk range, durability, materialization, and truncation cursors;
- object-storage Segment and TransformLog payloads;
- replication and WAL migration state stored with the checkpoint.

The checkpoint MessageID is a conservative `LastConfirmedMessageID` resume
anchor. It is used with `DeliverPolicyStartFrom`, so replay may repeat work but
must not skip unfinished work.

## 3. Recovery Flow

RecoveryStorage startup is:

1. Load the persisted `WALCheckpoint` and recovery catalog state.
2. Append a persisted `RecoveryBarrier` to prove write ownership and provide a
   bounded metadata scan endpoint.
3. Construct `PChannelRecoveryManager` and its VChannel-owned components from
   VChannel, Segment, and TransformLog metadata.
4. Run bounded Meta-only replay from the persisted checkpoint to the startup
   `RecoveryBarrier`. This rebuilds synchronous metadata and `TxnBuffer` state.
5. Switch modules into MetaAndData mode.
6. Start the data scanner from the persisted Data checkpoint. Replayed messages
   reconstruct Tracker entries and all required async work.
7. Start QueryView resource recovery from the VChannel WAL view. QueryRuntime
   preparation does not wait for DataScanner to catch the startup barrier.

`RecoveryBarrier` is a writer-fencing proof and bounded Meta-only endpoint. It
is not a data-completion barrier and is not a QueryRuntime readiness fence.

## 4. Meta And Data Lanes

RecoveryStorage maintains two logical consume positions:

```text
MetaPoint = latest completely observed WAL point
DataPoint = latest continuous fully completed Tracker point
```

Meta-only replay passes a normal `ImmutableMessage` to modules. It does not
create reference-counted messages or Tracker entries. The data scanner creates
one Owner and one Tracker entry for every message.

The DataPoint may lag MetaPoint while Segment or TransformLog work is pending or
while a broadcast Coordinator ACK is retrying. A persist batch always bounds
DataPoint by its frozen MetaPoint.

## 5. Data Observation

For one data-scanner message:

```text
owner, tracked = Tracker.Track(raw)
  -> PChannelRecoveryManager.ObserveDataMessage(owner)
       -> route by message scope
       -> concrete Segment/TransformLog consumers Clone(owner)
       -> synchronous metadata mutations are marked dirty
       -> QueryRuntime receives a normal immutable copy
  -> update MetaPoint
  -> BroadcastAck.Accept(owner, tracked)
       -> release owner immediately
       -> if tracked.RequiresBroadcastAck(): enqueue tracked
```

The Tracker entry stores the original message and WAL point. The wrapper
finalizer closes the local `ConsumersDone` event. For a non-broadcast message,
that event is enough to complete the entry. For a broadcast message,
Coordinator ACK success is an additional completion condition.

Ack does not define task order. SegmentView, TransformLog, and BroadcastAck own
their scheduler dependencies, retries, and batching.

## 6. Object Storage Work

SegmentView retains handles for actual data or lifecycle work:

- ensure-growing for CreateSegment;
- pending Insert packs;
- whole-Txn Insert work;
- flush and final segment commit.

TransformLog retains handles for Delete/Txn(Delete) entries and barriers that
must make preceding transform entries durable. The handle releases after chunk
durability and in-memory metadata installation. Materialization is independent.

Async consumers follow:

```text
perform idempotent/retryable work
  -> install resulting in-memory recovery metadata
  -> mark component dirty
  -> Release retained handle
```

Replay is required to be data-safe, not physically exactly once. Repeated
object writes may leave unreferenced objects and repeated lifecycle calls may
repeat external events, but persisted metadata must reference complete durable
data and replay must not skip unfinished work.

## 7. Broadcast ACK

BroadcastAck is the final Owner release sink. It adds no reference and never
reads, clones, or retains the Owner. It releases the Owner immediately for
every data-scanner message. When the Tracker entry's fixed
`RequiresBroadcastAck` property is true, the message is queued in WAL order and
the queue head waits for
`TrackedMessage.ConsumersDone`.

After local consumers finish, BroadcastAck calls Coordinator ACK with the
Tracker-owned immutable message. Success marks the Tracker broadcast condition
complete. Failure keeps the FIFO head pending and prevents the Data checkpoint
from advancing past that message.

`AckSyncUp` disables Coordinator FastAck. It does not require checkpoint
persistence or TransformLog materialization before consuming-side ACK.

## 8. Persist Batch

RecoveryStorage serializes observation long enough to freeze a batch boundary,
then consumes stable module snapshots:

```text
freeze MetaPoint
freeze DataPoint = min(MetaPoint, Tracker.CompletedPoint())
  -> consume VChannel/Segment/TransformLog DirtySnapshots
  -> persist all DirtySnapshots
  -> MarkPersisted for successful snapshots
  -> persist WALCheckpoint{MetaPoint, DataPoint} last
```

If a module snapshot fails, the checkpoint is not written. If metadata succeeds
but checkpoint persistence fails, metadata is ahead of the old checkpoint and
replay from the old point remains safe.

Component-local timeticks still exist for storage layout, cleanup,
materialization, subscriptions, and execution order. They are not generic
RecoveryStorage checkpoint barriers.

## 9. Cleanup

Segment and VChannel tombstones are persisted before physical deletion.
Cleanup uses the last persisted Meta and Data physical timeticks, not the
current in-memory checkpoint. TransformLog object deletion follows persisted
truncation metadata. Message Ack does not replace these component-specific
retention rules.

The Tracker retains each original message until its entry is fully complete and
leaves the continuous prefix. This is the ownership required by future
message-level checkpoint compaction or Defrag; Defrag itself is outside the
current implementation.

## 10. Close And Crash Recovery

There is no failed completion state. A task releases a handle only after its
success condition. Coordinator ACK failure leaves the broadcast condition
incomplete. Close does not synthesize releases or ACK completion.

Graceful close waits for scheduled tasks and persists dirty snapshots when
possible. A process crash discards in-memory Owners, retained handles, and
Tracker entries. Restart replays from the persisted Data checkpoint and
reconstructs them. Idempotent Coordinator ACK may be repeated.

## 11. Module API

```go
type Module interface {
    ObserveMessage(ctx context.Context, msg message.ImmutableMessage)
    SwitchIntoMetaAndData() ModuleSnapshot
    ConsumeDirtySnapshots() []DirtySnapshot
}

type DataMessageObserver interface {
    ObserveDataMessage(ctx context.Context, owner message.RefCountedImmutableMessageOwner)
}
```

`ObserveMessage` is used for Meta-only replay. `ObserveDataMessage` gives the
unique data-scanner Owner to the top-level `PChannelRecoveryManager` so it can
synchronously clone for real consumers. The final Owner release is
RecoveryStorage-owned and is routed through the dedicated BroadcastAck sink.

## 12. Invariants

1. Meta-only replay never claims data completion.
2. Every data-scanner message has one Tracker entry and one Owner.
3. Actual asynchronous data work owns retained handles; observation alone does
   not create a handle.
4. BroadcastAck is outside the module lifecycle, adds no reference, reads no
   Owner state, and releases the Owner immediately.
5. Broadcast Tracker entries require both local consumer completion and
   Coordinator ACK success.
6. DataPoint never exceeds the frozen MetaPoint or the continuous completed
   Tracker frontier.
7. DirtySnapshots are persisted before the checkpoint that covers them.
8. TransformLog message completion does not wait for materialization.
9. QueryRuntime copies live messages and never participates in Message Ack.
10. Recovery uses `LastConfirmedMessageID + DeliverPolicyStartFrom`.
