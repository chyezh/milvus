# WAL Recovery Architecture

This document describes how StreamingNode reconstructs PChannel state from WAL,
persists data into object storage and recovery metadata into etcd, and advances
restart checkpoints safely.

Detailed data-message completion semantics are defined by
[WAL Message Ack Design](message_ack.md).

## 1. Goals

RecoveryStorage must:

1. rebuild VChannel, Segment, TransformLog, and DataView recovery state;
2. consume new WAL messages after recovery;
3. persist data payloads into object storage;
4. persist recovery metadata and tombstones into etcd;
5. publish Meta and Data checkpoints that are safe restart positions;
6. acknowledge broadcast messages after their local consumers finish;
7. bound asynchronous recovery work and close without falsely completing it;
8. retain enough metadata for data-safe replay and delayed cleanup.

QueryView and QueryRuntime resource lifetime is not part of checkpoint
publication. QueryView may consume recovered state through
`PChannelRecoveryManager`, but its references never participate in Message Ack.

## 2. Runtime Ownership

RecoveryStorage registers two top-level `moduleapi.Module` implementations:

```text
RecoveryStorage
  +-- PChannelRecoveryManager              ModuleNameVChannel
  |     +-- VChannelRecoveryModule*
  |           +-- VChannel metadata and schema history
  |           +-- SegmentView*
  |           +-- TransformLog
  |           +-- DataView recovery state
  |           +-- QueryRuntime bridge
  +-- broadcastAckModule                   ModuleNameAck
```

`PChannelRecoveryManager` owns the VChannel index and routes each message to the
affected `VChannelRecoveryModule`. PChannel-wide broadcast messages may be
observed by all relevant VChannels through the same Message Ack envelope.

`SegmentView` and `TransformLog` are VChannel-internal components. They are not
independent top-level recovery modules, even though snapshot payloads retain
segment and transform-log module names for catalog organization.

## 3. Recovery State

RecoveryStorage loads the latest persisted state for one PChannel:

- WAL checkpoint;
- VChannel metadata and tombstones;
- Segment assignment metadata and tombstones;
- TransformLog metadata;
- DataView recovery metadata;
- any salvage checkpoint required after partial recovery.

It constructs `PChannelRecoveryManager`, which creates one
`VChannelRecoveryModule` for each recovered VChannel and reconnects Segment and
TransformLog ownership under that VChannel.

## 4. Two Scanner Lanes

RecoveryStorage maintains two logical consume positions:

- Meta checkpoint: latest completely observed WAL point whose synchronous
  recovery metadata was captured and persisted by the same batch;
- Data checkpoint: WAL data work already complete, with all resulting metadata
  captured by the same or an earlier batch, and safe to skip during data replay.

Recovery begins with metadata reconstruction and then switches modules into
MetaAndData mode. A data scanner starts from the persisted Data checkpoint and
replays any data-side work that may not have completed before the previous
process stopped.

Meta-only observation uses a no-op Ack envelope. Data-scanner observation
creates a real Ack record for every message.

## 5. Message Observation

For one data-scanner message at WAL point `P`:

```text
create AckRecord(P) with one implicit dispatch Ref
  -> append record to ordered Ack tracker
  -> dispatch envelope to PChannelRecoveryManager
  -> dispatch envelope to broadcastAckModule
  -> Seal the record
       -> forbid future Retain
       -> release implicit dispatch Ref
  -> advance latest observed WAL point
  -> mark RecoveryStorage dirty
  -> reevaluate continuous Ack completed frontier
```

The point is:

```text
P.MessageID = message.LastConfirmedMessageID()
P.TimeTick  = message.TimeTick()
```

The TimeTick is the logical consumed-through boundary. The MessageID is a safe
physical resume anchor used with `DeliverPolicyStartFrom`; recovery may replay
completed messages but must not skip a later TimeTick.

Module observation performs synchronous in-memory state transitions and may
submit asynchronous tasks. Every actual data consumer must retain a direct Ref
before the asynchronous task can observe completion. A module that discovers
dynamic children later retains one parent Ref before returning and joins those
children internally.

The Module API exposes no generic data barrier or data frontier. Message Ack is
the only contract for data-side work that must block the RecoveryStorage Data
checkpoint.

Message Ack does not provide task ordering. SegmentView, TransformLog, and
BroadcastAck own their scheduler dependencies and component-local progress.
They call `Ref.Done()` only after their own ordered success condition is
satisfied.

## 6. Persistence Model

### 6.1 Object Storage

Segment and TransformLog data are written asynchronously:

- Segment insert data is sealed into chunks or committed L1 output;
- TransformLog entries are sealed into deterministic chunk objects;
- failed writes remain retryable and keep their Message Ack refs.

Object writes may finish before the recovery metadata that references them is
persisted. Such objects are not enough by themselves to publish a restart
checkpoint.

Replay is required to be data-safe, not physically exactly once. Segment pack
replay may create unreferenced object files, and retried lifecycle RPCs may
repeat external events. Component metadata must reference only completed data;
orphan cleanup and external deduplication remain owned by the corresponding
storage or coordinator component.

### 6.2 Recovery Catalog In etcd

VChannel, Segment, TransformLog, DataView, and tombstone changes are exposed as
stable `DirtySnapshot` values. RecoveryStorage captures dirty snapshots under
module synchronization, writes them to their catalog keys, and calls
`MarkPersisted()` after the catalog operation succeeds.

`MarkPersisted()` updates module-local dirty generations and persisted state
used by recovery, cleanup, or GC. It does not advance a per-message checkpoint
barrier.

Snapshot ownership remains component local:

- VChannel snapshots are owned by `VChannelRecoveryModule`;
- Segment snapshots are owned by their `SegmentView` and aggregated by the
  VChannel/PChannel manager;
- TransformLog snapshots are owned by the VChannel's `TransformLog`;
- cleanup snapshots are emitted only after component-specific retention rules
  are satisfied.

## 7. Checkpoint Persist Batch

RecoveryStorage persists metadata and checkpoint state as one ordered batch. It
first freezes a boundary while message observation is serialized:

```text
MetaPoint = latest completely observed WAL point
DataPoint = min(MetaPoint, Ack completed frontier)
```

After freezing the boundary, RecoveryStorage consumes stable DirtySnapshots
from every module. New observations may make those snapshots newer than
`MetaPoint`, but the batch checkpoint remains fixed at the frozen boundary.

The persist order is:

```text
freeze MetaPoint and DataPoint
  -> consume module DirtySnapshots
  -> persist all DirtySnapshots
  -> MarkPersisted after every snapshot succeeds
  -> persist WALCheckpoint{MetaPoint, DataPoint}
```

The batch never refreshes its checkpoint to a point observed after the freeze.
If new work arrives, it is captured by the next batch.

An asynchronous consumer that changes recovery metadata must mutate the state
and mark it dirty before calling `Ref.Done()`. Consequently, every message in
the frozen Ack completed frontier has exposed all metadata that the subsequent
DirtySnapshot capture must persist.

There is no per-message metadata checkpoint queue or generic data checkpoint
barrier queue. RecoveryStorage directly owns the latest observed point, the
ordered Ack tracker, and the immutable in-flight persist batch. TransformLog
materialization is outside the message Ack completion boundary.

Snapshots newer than the frozen checkpoint are permitted because current
components retain the recovery information needed to reconcile them:

- Segment metadata separates metadata progress from durable data progress,
  replays Insert work against recovered Growing or Flushed state, and recreates
  unfinished final-commit work;
- TransformLog advances its checkpoint only after the corresponding chunk is
  durable and skips replay only through that durable checkpoint;
- tombstone finalization and catalog cleanup wait for persisted Meta and Data
  checkpoints to cover the tombstone;
- recovery constructs VChannel ownership from the union of VChannel, Segment,
  TransformLog, and DataView records, allowing independently persisted keys.

These are replay preconditions owned by the components, not guarantees supplied
by AckRecord itself.

## 8. Async Scheduler

RecoveryStorage wraps the node scheduler with a PChannel-scoped scheduler. It:

- limits the number of concurrently running recovery tasks;
- queues excess work;
- requeues delayed tasks without consuming a running slot;
- tracks pending and running tasks for `WaitIdle`;
- propagates cancellation to queued and running tasks;
- drains or cancels tasks during close.

Message Ack is independent from physical task shape. One message may fan out to
multiple tasks, and one storage task may batch work from multiple messages.
Every actual consumer retains a direct Ref. When children are discovered after
observation, module-local fan-in maps those child completions to a parent Ref
that was retained before Seal.

Cancellation and close do not release Message Ack Refs. AckRecord has no failed
state: only successful component work calls `Done()`. Process shutdown may
discard the in-memory tracker because the persisted Data checkpoint remains
behind every outstanding Ref and restart reconstructs the records from WAL.

## 9. Broadcast Acknowledgement

`broadcastAckModule` retains one Ref for messages carrying a `BroadcastHeader`.
Its FIFO queue head waits until the record is sealed and its own Ref is the only
remaining reference, then calls the coordinator Ack API and releases that Ref.

It does not infer readiness from message type, module progress, checkpoint
publication, or TransformLog materialization. See
[Broadcast Ack Module](broadcast_ack_module.md).

`AckSyncUp` only disables Coordinator FastAck and forces the broadcaster to wait
for this consuming-side Ack. It does not require the RecoveryStorage checkpoint
to be persisted before `Broadcast().Ack`.

## 10. Tombstones And Cleanup

Logical drop and physical cleanup are separate:

1. observation records a tombstone in component-owned recovery metadata;
2. the tombstone is persisted before checkpoints can publish the change;
3. cleanup waits for the component's own retention rules and physical
   checkpoints;
4. cleanup emits a delete snapshot for the component's catalog key;
5. successful catalog deletion releases the retained metadata.

VChannel, Segment, and TransformLog cleanup remain independently owned. One
component must not inspect another component's private state to decide whether
its own tombstone is safe to remove.

Message-tracking cleanup is separate from metadata tombstones. RecoveryStorage
removes the continuous completed AckRecord prefix from its live tracker after
the records are sealed and all consumer Refs have completed.

## 11. Retry And Recovery

| Interruption point | Recovery behavior |
|---|---|
| Before object data is durable | Consumer Ref remains pending; persisted Data checkpoint stays behind and WAL replay retries the work. |
| Object data durable but dirty metadata not persisted | Ack may complete, but the persist batch cannot write its checkpoint; WAL replay reconciles or republishes the metadata. |
| Dirty metadata persisted but a consumer Ref remains | MetaPoint may advance, but DataPoint stays behind the Ack completed frontier. |
| Dirty metadata persists but checkpoint persistence fails | Metadata is ahead of the old checkpoint; replay from the old checkpoint remains safe. |
| Broadcast Ack succeeds before checkpoint persistence | WAL replay may repeat the idempotent coordinator Ack. |
| Process exits with in-flight Ack records | Data scanner rebuilds records by replaying from the persisted Data checkpoint. |
| Task context is canceled or RecoveryStorage closes | Outstanding Refs remain retained; close never reports incomplete work as completed. |
| Catalog contains a tombstone after restart | Component reconstructs the tombstone and resumes cleanup under the same retention rule. |

## 12. ModuleAPI Boundary

The core recovery module contract is:

```go
type Module interface {
    Name() ModuleName
    ObserveMessage(ctx context.Context, msg messageack.Message)
    SwitchIntoMetaAndData() ModuleSnapshot
    ConsumeDirtySnapshots() []DirtySnapshot
}
```

A module must retain a message Ref for every operation that must block that
message's completion. `ObserveMessage` returns nothing because metadata
durability is guaranteed by the frozen persist batch and data completion is
represented by Message Ack. Domain-specific progress values remain private to
their owning components or are exposed through purpose-specific APIs.

`CleanupModule` and `PendingCleanupModule` expose catalog cleanup work.
`ModuleNotifier` wakes RecoveryStorage after new dirty metadata or Ack completion
so a new persist batch can be scheduled promptly.

## 13. Detailed Documents

- [Message Workflow](message-workflow.md)
- [WAL Message Ack Design](message_ack.md)
- [Broadcast Ack Module](broadcast_ack_module.md)
- [Segment View Module](segment_view_module.md)
- [TransformLog Design](transformlog/transform_log.md)
- [VChannel View Module](vchannel_view_module.md)
- [StreamingNode VChannel WAL Input View](streamingnode_vchannel_wal_view.md)

## 14. Invariants

1. Every persist batch freezes its checkpoint boundary before consuming stable
   DirtySnapshots.
2. Data checkpoint advancement is driven by the continuous Message Ack
   completed frontier.
3. A batch DataPoint never passes its MetaPoint.
4. Segment and TransformLog data refs release only after their data-side success
   conditions.
5. Async consumers update metadata and mark it dirty before releasing their
   Ref.
6. Every DirtySnapshot in a batch persists before that batch's checkpoint.
7. BroadcastAck waits for other refs on the same message and remains FIFO.
8. QueryView references do not affect RecoveryStorage checkpoints.
9. Physical task layout is module local and is not encoded in AckRecord owner
   categories.
10. WAL replay is sufficient to rebuild non-persisted Ack state.
11. Ack does not define component-local execution order or failure states.
12. Checkpoint recovery uses `LastConfirmedMessageID` with
    `DeliverPolicyStartFrom`.
