# Broadcast Ack Module

`broadcastAckModule` owns StreamingNode-local acknowledgement for persisted
broadcast WAL messages. It is a RecoveryStorage module because the coordinator
Ack is a replayable data-side effect.

Message completion and checkpoint semantics are defined by
[WAL Message Ack Design](message_ack.md).

## 1. Ownership

`broadcastAckModule` owns:

- detecting persisted messages carrying a `BroadcastHeader`;
- retaining one message handle for each such message;
- preserving acknowledgement order within one PChannel;
- submitting `streaming.WAL().Broadcast().Ack(ctx, msg)`;
- retrying failed coordinator Ack calls;
- releasing its handle only after the Ack call succeeds.

It does not own:

- VChannel, Segment, TransformLog, or DataView metadata;
- object-storage flush decisions;
- TransformLog materialization;
- module dirty snapshots;
- Meta or Data checkpoint advancement.

## 2. Ack As Message Work

For every persisted message carrying a `BroadcastHeader`, the module calls
`Retain()` during `ObserveMessage` and stores the returned retained immutable
message directly in the queued broadcast task.

The handle shares the same reference-count control block used by SegmentView and
TransformLog consumers. It blocks completion of that WAL message until the
external broadcast Ack has succeeded.

The module expresses its data-side completion only through its retained handle.
RecoveryStorage derives its data checkpoint from the continuous completed Ack
frontier frozen for a persist batch.

Broadcast Ack is replayable and idempotent. If StreamingNode crashes after the
coordinator call succeeds but before the recovery checkpoint is persisted, WAL
replay may send the same Ack again.

For a message with `BroadcastHeader.AckSyncUp`, Coordinator Broadcaster skips
its FastAck path and waits for this consuming-side Ack. `AckSyncUp` does not add
another RecoveryStorage readiness condition and does not mean the
RecoveryStorage checkpoint has already been persisted.

## 3. Preconditions

The queue head may acknowledge its message when:

```text
handle.Sealed() && handle.IsExclusive()
```

The exclusive ownership is the queue head's own BroadcastAck handle. `Sealed()`
proves that all observers have returned and no new consumer may retain the
message. `IsExclusive()` proves that every other asynchronous consumer has
released its handle.

No message-type-specific readiness table is required.

The task does not wait for:

- a Segment or TransformLog timetick frontier;
- RecoveryStorage Data checkpoint progress;
- RecoveryStorage checkpoint publication;
- TransformLog materialization.

If a broadcast message causes Segment or TransformLog data work, those
consumers must retain their own handles before dispatch is sealed. The
BroadcastAck precondition then observes their real completion directly.

## 4. Ordering

Broadcast acknowledgements remain FIFO within one PChannel:

```text
message A queue head
  -> wait until A is sealed and BroadcastAck's handle is exclusive
  -> Ack(A) succeeds
  -> A's BroadcastAck handle.Release()
  -> submit message B queue head
```

A later broadcast is not acknowledged before an earlier one, even if the later
message's other handles have already released. StreamingCoord relies on this
per-PChannel WAL order.

## 5. Retry And Close

If another message consumer is still pending, the task returns the scheduler's
delay signal and keeps its handle.

If `Broadcast().Ack` fails, the task is delayed and retried. It does not call
`Release()` and does not advance the FIFO queue.

BroadcastAck has no failed completion state. Context cancellation and
RecoveryStorage close also keep the handle retained. Shutdown may discard the
in-memory queue only because the persisted Data checkpoint remains behind the
message and WAL replay will rebuild the BroadcastAck handle.

After `Broadcast().Ack` succeeds:

1. call `Release()` on the task's retained message handle;
2. remove the task from the queue head;
3. submit the next queued broadcast task;
4. let the shared message zero transition notify its Tracker entry when this was
   the final handle.

## 6. ModuleAPI

`broadcastAckModule` implements `moduleapi.Module`:

- `Name()` returns `ModuleNameAck`;
- `ObserveMessage()` retains one message handle and enqueues a task only for a broadcast
  message in MetaAndData mode;
- `SwitchIntoMetaAndData()` enables broadcast acknowledgement and returns no
  recovery snapshot;
- `ConsumeDirtySnapshots()` returns nil because Ack state is replayed from WAL,
  not persisted as module metadata.

## 7. Invariants

1. Every observed broadcast message in MetaAndData mode owns exactly one
   BroadcastAck retained handle.
2. BroadcastAck waits for a sealed message with an exclusive handle, not inferred
   module frontiers.
3. The coordinator call remains FIFO per PChannel.
4. A failed coordinator call keeps the handle live and remains retryable.
5. The handle is released only after the coordinator call succeeds.
6. BroadcastAck does not wait for checkpoint publication or
   TransformLog materialization.
7. `AckSyncUp` disables Coordinator FastAck but does not change the
   RecoveryStorage Ack precondition.
8. Cancellation and close never release an incomplete BroadcastAck handle.
