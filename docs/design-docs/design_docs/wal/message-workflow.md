# Message Workflow

This document describes how RecoveryStorage consumes persisted WAL messages and
routes the resulting work into VChannel-owned components. The lifecycle and
checkpoint contract is defined by [WAL Message Ack Design](message_ack.md).

## 1. Common Flow

Every message from the data scanner follows this sequence while observation is
serialized:

```text
raw message M
  -> Tracker.Track(M) returns Owner O and TrackedMessage T
  -> PChannelRecoveryManager dispatches O synchronously
       -> actual Segment/TransformLog consumers call O.Clone()
       -> QueryRuntime receives an ordinary immutable copy
  -> RecoveryStorage updates MetaPoint from M
  -> dedicated BroadcastAck sink accepts O and T
       -> O.Release() unconditionally
       -> if T.RequiresBroadcastAck(): enqueue T
  -> final retained handle Release closes T.ConsumersDone
  -> broadcast task performs Coordinator Ack and calls T.CompleteBroadcastAck
  -> Tracker advances the continuous completed prefix
```

The Tracker point is computed from the original message:

```text
Point.MessageID = M.LastConfirmedMessageID()
Point.TimeTick  = M.TimeTick()
```

Recovery uses `Point.MessageID` with `DeliverPolicyStartFrom`. Completed
messages may be replayed; an unfinished message must never be skipped.

Meta-only replay uses the ordinary immutable message interface and does not
create a Tracker entry. It rebuilds synchronous metadata and the transaction
buffer, then advances the Meta checkpoint.

There is no explicit negative ACK. A module that has no work for a message
simply does not clone the Owner. BroadcastAck still receives every data-scanner
Owner as the final release sink, even when the Tracker says no Coordinator ACK
is required. There is no DataBarrier or module frontier in the RecoveryStorage
checkpoint contract.

## 2. Ownership Table

| Holder | Creation | Release/completion condition |
|---|---|---|
| Owner root | `Tracker.Track` | Dedicated BroadcastAck sink receives it and unconditionally calls `Release` |
| Segment handle | Segment sees concrete async work | Required object/lifecycle work succeeds, metadata is dirty |
| TransformLog handle | Delete or barrier requires chunk flush | Covering chunk is durable and committed, metadata is dirty |
| Tracker entry message | `Tracker.Track` | Both local consumers and required broadcast ACK complete, then entry leaves continuous prefix |
| QueryRuntime copy | Live event is queued | QueryRuntime owns and releases it through its normal queue lifecycle |

BroadcastAck is not a recovery module and does not create or retain a message
handle. It does not inspect the Owner. The Tracker entry keeps the original
immutable message available while BroadcastAck retries and records whether a
Coordinator ACK is required.

## 3. Typical Messages

### TimeTick

TimeTick changes no object-storage state and no recovery metadata owned by a
VChannel. No module clones the Owner. BroadcastAck releases the Owner, the
Tracker entry completes immediately, and the continuous Data checkpoint can
advance.

### CreateCollection

The target VChannel module updates collection metadata, partitions, schema
history, and its dirty snapshot. It normally creates no Segment or TransformLog
handle. BroadcastAck releases the Owner immediately. Its FIFO ACK task observes
`ConsumersDone` (already closed once the root is released), ACKs the message,
and completes the Tracker broadcast condition.

### Insert

The VChannel routes the Insert to each affected SegmentView. Each affected view
clones the Owner and stores its retained handle with the pending L1 data pack:

```text
Insert
  -> append rows and retain handle
  -> flush pack to object storage
  -> install persisted segment metadata and mark dirty
  -> release handle
```

RecoveryStorage releases the Owner after synchronous dispatch. The Tracker
cannot complete until every affected SegmentView has released its handle.

### Delete

TransformLog converts Delete into one vchannel transform entry and retains a
handle if the entry depends on a future chunk flush:

```text
Delete
  -> append TransformLog entry and retain handle
  -> write and commit containing chunk
  -> update TransformLog metadata and mark dirty
  -> release handle
```

Materialization into L0 output is independent and does not delay this handle.
Delete does not require a Coordinator broadcast ACK, so the sink only releases
the Owner for this message.

### Flush And ManualFlush

Flush, ManualFlush, and FlushAll update lifecycle metadata synchronously and
may schedule Segment chunk flush/commit work. Each affected SegmentView clones
the same message before task submission. TransformLog clones the message only
when preceding Delete entries require a chunk flush. Handles release after the
required object-storage work succeeds, not after materialization.

For a broadcast Flush-style message, the dedicated BroadcastAck sink releases
the Owner first. Its FIFO ACK task waits for all actual Segment and TransformLog
handles through `ConsumersDone`, performs the Coordinator ACK, and completes
the Tracker entry.

### Txn

A committed transaction is one immutable WAL message. SegmentViews and
TransformLog retain the outer Txn message as needed. Messages returned by
`RangeOver` are borrowed children and do not receive independent references.
The Tracker point and completion also belong to the whole Txn.

### Broadcast Metadata Messages

CreatePartition, DropCollection, DropPartition, AlterCollection, replication
configuration changes, RBAC changes, and other broadcast metadata messages
usually affect only synchronous VChannel metadata. They therefore create no
data handles. BroadcastAck still performs FIFO Coordinator ACK so that
`AckSyncUp` and broadcast callback ordering remain correct.

## 4. Persist Batch

RecoveryStorage freezes a batch before consuming dirty snapshots:

```text
MetaPoint = latest completely observed WAL point
DataPoint = continuous Tracker completed point bounded by MetaPoint
```

The batch order is:

```text
freeze MetaPoint/DataPoint
  -> consume stable DirtySnapshots
  -> persist every DirtySnapshot to etcd/object catalog
  -> MarkPersisted on successful snapshots
  -> persist WALCheckpoint last
```

Every asynchronous consumer must perform metadata mutation and mark the module
dirty before releasing its retained handle. This ensures a Data checkpoint does
not pass data whose recovery metadata has not been captured by the batch.

## 5. Invariants

1. One data-scanner message has one Tracker entry and one Owner.
2. Owner and retained handles are independent objects; each handle is released
   at most once.
3. All asynchronous consumer clones are created synchronously during dispatch.
4. BroadcastAck is outside the module lifecycle, never adds a reference, and
   always releases the Owner.
5. Coordinator ACK occurs only after all local retained handles are released.
6. A broadcast Tracker entry completes only after Coordinator ACK succeeds.
7. TransformLog completion requires chunk durability, not materialization.
8. Checkpoint MessageID is `LastConfirmedMessageID` and resume uses
   `DeliverPolicyStartFrom`.
9. Ack observes completion but does not impose task execution order.
10. QueryRuntime receives copies and never participates in RecoveryStorage Ack.
