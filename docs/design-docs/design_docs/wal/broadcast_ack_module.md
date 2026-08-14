# Broadcast Ack Module

`broadcastAckModule` sends consuming-side acknowledgements for broadcast WAL
messages to StreamingCoord. It is a dedicated RecoveryStorage sink, not a
`moduleapi.Module` and not a data persistence consumer.

The common lifetime contract is defined in
[WAL Message Ack Design](message_ack.md).

## 1. Accept Ownership

```go
func (m *broadcastAckModule) Accept(owner message.OwnedImmutableMessage)
```

`Accept` takes exclusive top-level ownership of the Owner. The caller must not
use or clone it afterward.

- If `owner.Message().BroadcastHeader() == nil`, BroadcastAck immediately calls
  `owner.Release()` and creates no task.
- Otherwise, BroadcastAck enqueues the Owner in its per-PChannel FIFO queue and
  keeps it until Coordinator Ack succeeds.

Tracker is not passed to BroadcastAck. The Owner finalizer established by
Tracker is sufficient to mark the message complete after BroadcastAck finally
releases the root reference.

## 2. Exclusive Precondition

The queue head becomes schedulable when:

```text
<-owner.Exclusive()
```

This means reference count is one: BroadcastAck's Owner is the only remaining
tracked reference and every Segment/TransformLog Retained handle has been
released. It does not mean the message is complete, because BroadcastAck still
owns the root reference.

Once the Owner is accepted, no new Retained clones may be created. Therefore a
closed Exclusive event remains a stable precondition for the Ack task.

## 3. Ack, FIFO, And Retry

```text
enqueue A Owner
  -> wait A Exclusive
  -> Coordinator Ack(A.Message())
  -> A.Release()
  -> submit next FIFO owner
```

Only the queue head can run. If Coordinator Ack fails, the task retains the same
Owner and schedules a delayed retry; it does not release the Owner or advance
the queue. Ack is idempotent, so replay may repeat an Ack accepted before a
crash but not covered by a persisted Data checkpoint.

## 4. AckSyncUp

`BroadcastHeader.AckSyncUp` affects only StreamingCoord: it skips FastAck and
waits for this consuming-side Ack. BroadcastAck still waits only for local
Retained consumers. It does not wait for Meta/Data checkpoint publication,
DirtySnapshot persistence, TransformLog materialization, or QueryRuntime
readiness.

## 5. Close

Close cancels Exclusive waiters and retry timers. It does not release queued
Owners or mark unfinished work successful. Restart reconstructs the queue by
replaying from the persisted Data checkpoint.

## 6. Invariants

1. BroadcastAck is outside the recovery component dirty-snapshot lifecycle.
2. `Accept` consumes the Owner exactly once.
3. Non-broadcast Owners are released immediately.
4. Broadcast Owners remain live through every failed Ack attempt.
5. Coordinator Ack runs only after Owner exclusivity.
6. Successful Ack releases the Owner before the next FIFO task is submitted.
7. BroadcastAck has no Tracker handle, checkpoint frontier, or materialization
   dependency.
