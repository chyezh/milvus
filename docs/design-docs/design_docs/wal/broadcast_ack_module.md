# Broadcast Ack Module

`broadcastAckModule` sends consuming-side acknowledgements for broadcast WAL
messages to StreamingCoord. It is a dedicated RecoveryStorage sink, not an
additional message consumer and not a `moduleapi.Module`.

The common ownership and checkpoint rules are defined in
[WAL Message Ack Design](message_ack.md).

## 1. Ownership

BroadcastAck receives the Owner and the Tracker's `TrackedMessage` for the
same data-scanner message. It does not clone the Owner and does not retain a
separate handle. `Accept` is called only from the data-scanner path. Its only
operation on the Owner is an unconditional `Release`; it uses the Tracker
handle, rather than the Owner, to decide whether a Coordinator ACK is needed:

```text
owner.Release()
if tracked.RequiresBroadcastAck():
    queue TrackedMessage
```

The queued task retains the original immutable message indirectly through the
Tracker entry. The Tracker keeps that entry until both local consumers and
Coordinator ACK have completed.

## 2. Task Preconditions

The FIFO queue head is eligible when:

```text
TrackedMessage.ConsumersDone is closed
```

This event means that the Owner and every Segment/TransformLog retained handle
have been released. BroadcastAck does not inspect the Owner, a raw refcount, or
an exclusive-handle predicate, and does not wait for a checkpoint frontier.
The sink itself has no retained handle; the Tracker event is the only local
consumer completion signal it observes.

When eligible, the task calls:

```text
streaming.WAL().Broadcast().Ack(ctx, tracked.Message())
```

After the call succeeds it invokes `tracked.CompleteBroadcastAck()` and removes
itself from the FIFO queue. The Tracker can then advance the continuous
completed prefix if this message is next in WAL order.

## 3. Ordering And Retry

Broadcast ACK remains FIFO within one PChannel:

```text
release A Owner -> enqueue A
  -> wait A ConsumersDone
  -> Coordinator Ack(A)
  -> CompleteBroadcastAck(A)
  -> enqueue/submit B
```

The queue head is submitted only after `ConsumersDone` is closed, so an
unfinished local consumer does not create scheduler polling. If Coordinator
ACK fails, the task remains the queue head and schedules one delayed retry
outside the scheduler's delay/requeue path. It does not complete the Tracker
entry or advance the queue.

Coordinator ACK is idempotent. If the process crashes after Coordinator has
accepted the ACK but before the Tracker entry or checkpoint is persisted, WAL
replay may issue the same ACK again.

## 4. AckSyncUp

`BroadcastHeader.AckSyncUp` only changes Coordinator behavior: it disables
FastAck and makes Coordinator wait for the consuming-side ACK. It does not make
BroadcastAck wait for Meta checkpoint persistence, Data checkpoint persistence,
TransformLog materialization, or QueryRuntime readiness.

## 5. Close And Recovery

An unfinished BroadcastAck task retains no Owner handle, but its Tracker entry
remains live through the Tracker's original message reference. Close cancels
the wait and retry timers without marking the task complete. On restart, WAL
replay reconstructs the task and Coordinator ACK may be repeated safely.

## 6. Invariants

1. BroadcastAck is not a `moduleapi.Module` and is absent from the module
   dirty-snapshot and Meta-only replay paths.
2. BroadcastAck never calls `Message()` or `Clone()` on the Owner.
3. BroadcastAck releases the Owner exactly once for every data-scanner message.
4. Only actual Segment and TransformLog work creates retained handles.
5. Coordinator ACK happens after `ConsumersDone` and before
   `CompleteBroadcastAck`.
6. Broadcast ACK order is FIFO per PChannel.
7. BroadcastAck does not wait for checkpoint publication or materialization.
