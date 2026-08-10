# Message Workflow

This document describes how RecoveryStorage handles consumer-observable
persisted WAL messages. Transaction control records consumed by transaction
assembly and deprecated message types are out of scope.

Completion and checkpoint rules are defined by
[WAL Message Ack Design](message_ack.md).

## 1. Common Dispatch Rules

RecoveryStorage dispatches every persisted data-scanner message through one
explicit Message Ack envelope:

```text
NewRecord with implicit dispatch Ref
  -> PChannelRecoveryManager observes the message
  -> affected VChannelRecoveryModule instances observe the message
  -> each actual SegmentView and TransformLog consumer calls Retain()
  -> broadcastAckModule calls Retain() for BroadcastHeader messages
  -> RecoveryStorage calls Seal()
       -> forbid later Retain()
       -> release implicit dispatch Ref
```

The AckRecord point is the logical consumed-through boundary:

```text
Point.MessageID = message.LastConfirmedMessageID()
Point.TimeTick  = message.TimeTick()
```

Recovery resumes with `DeliverPolicyStartFrom(Point.MessageID)`. This may
replay completed messages, but the LastConfirmed anchor prevents a later
TimeTick from being skipped.

Metadata mutations are captured as dirty snapshots. RecoveryStorage freezes a
persist-batch boundary, writes every captured DirtySnapshot, and writes that
batch's checkpoint last.

Data checkpoint advancement is driven by the continuous Message Ack completed
frontier frozen for the same persist batch.

For a broadcast message, BroadcastAck waits until the sealed AckRecord has only
its own Ref, calls the coordinator Ack API, and releases that Ref. It does not
use message-type-specific readiness or materialization rules.

`AckSyncUp` only disables Coordinator FastAck and requires Coordinator to wait
for this RecoveryStorage-side Ack. It does not add checkpoint persistence to
the BroadcastAck readiness rule.

Ack does not define Segment or TransformLog task order. Each consumer owns its
execution dependencies and calls `Ref.Done()` only after its own success
condition is satisfied. Work that is retried, canceled, or abandoned during
close retains its Ref; restart reconstructs a new AckRecord from WAL.

## 2. Ack Reference Summary

| Reference | Retained when | Released when |
|---|---|---|
| Implicit dispatch Ref | `NewRecord` for every data-scanner message | `Seal()` after all top-level observers return |
| Segment consumer Ref | One per actual SegmentView or module-local parent operation | Its data or lifecycle work succeeds |
| TransformLog consumer Ref | A message appends transform payload or requires pending transform entries to flush | The containing TransformLog chunk is durable and committed in memory |
| BroadcastAck Ref | Message has a `BroadcastHeader` | The sealed record has no other Refs and coordinator Ack succeeds |

TransformLog materialization is independent from TransformLog Ref completion.

## 3. Message Workflows

### TimeTick

TimeTick does not mutate VChannel, Segment, or TransformLog recovery state and
does not retain a consumer Ref. It has no `BroadcastHeader` and is immediately
complete after `Seal()` releases the implicit dispatch Ref.

### CreateCollection

The target `VChannelRecoveryModule` creates or updates VChannel metadata,
partition state, schema history, and the VChannel dirty snapshot. No Segment or
TransformLog Ref is required when there is no pending transform payload.

The broadcast module retains its own Ref, which can run after the record is
sealed and any actual VChannel-internal data Refs finish. VChannel metadata
is included in a DirtySnapshot before a later persist-batch checkpoint is
written.

### CreatePartition

The target VChannel updates partition metadata and emits a dirty snapshot. It
normally retains no Segment or TransformLog Ref. BroadcastAck follows the
common last-reference rule.

### DropCollection

The target VChannel records collection-drop metadata and tombstones. In
MetaAndData mode:

- every SegmentView that flushes or commits pending data retains its own Ref;
- TransformLog retains a Ref when preceding Delete entries need a chunk flush;
- any separately scheduled L0 materialization does not extend the TransformLog
  Ref;
- BroadcastAck waits for the actual Segment and TransformLog refs, then calls
  the coordinator Ack API.

### TruncateCollection

The target VChannel advances truncation metadata. SegmentViews and TransformLog
retain Refs only for concrete data work caused by the message. BroadcastAck
uses the common last-reference rule and does not inspect VChannel-local
timetick progress.

### DropPartition

The VChannel records the partition tombstone. SegmentViews in the affected
partition may retain direct Refs while flushing pending data. TransformLog may
retain a Ref to flush preceding Delete entries. The
BroadcastAck task waits for those refs, not for partition/vchannel frontier
objects.

### Import, CommitImport, RollbackImport, BatchUpdateManifest

These messages do not create local Segment or TransformLog persistence work in
RecoveryStorage unless a current VChannel handler explicitly adds such work.
Their broadcast acknowledgement normally waits only for the record to be
sealed, previous broadcast FIFO order, and any Ref retained by another
observer.

### CreateSegment

`VChannelRecoveryModule` creates the target SegmentView using the schema valid
at the message timetick and updates Segment metadata.

In MetaAndData mode, ensure-growing work retains a direct Ref before task
submission. The Ref releases after the lifecycle side effect succeeds.
Segment metadata is marked dirty for the next persist batch.

### Insert

The target SegmentView updates in-memory statistics and appends the payload plus
a direct message Ref to its pending L1 buffer.

```text
Insert@T
  -> pending Segment buffer retains Ref
  -> flush policy seals a chunk containing Insert@T
  -> object write succeeds and chunk is installed
  -> Insert@T Ref.Done()
```

A chunk may contain several WAL messages. Failure keeps every contained Ref
pending. The task updates Segment metadata and marks it dirty before releasing
the contained Refs.

### Delete

TransformLog converts Delete into a vchannel-level transform entry and appends
it with a direct message Ref. The Ref remains pending until the chunk
containing the entry is durably written and committed into TransformLog state.

Delete legality is guaranteed by WAL write-time checks and replay is
idempotent against TransformLog metadata. TransformLog does not need to inspect
Segment private state.

Delete is not a broadcast message, so BroadcastAck does not retain a Ref.

### Flush

The target SegmentView records the sealed transition and may retain direct Refs
for pending chunk flush and commit-L1 work.

TransformLog treats Flush as a sync-up barrier. If preceding Delete entries
need to be flushed, it retains a Ref until their containing chunks are durable.
It does not retain a Ref for materialization.

### ManualFlush

All retained SegmentViews in the target VChannel may retain direct Refs
for required flush/commit work. TransformLog flushes preceding Delete entries
and retains a Ref until chunk durability.

ManualFlush may also trigger L0 materialization as an independent downstream
task. Neither the TransformLog Ref, BroadcastAck, nor RecoveryStorage Data
checkpoint waits for materialization.

BroadcastAck runs after the Segment and TransformLog refs have completed.

### FlushAll

Each local VChannel applies the same rules as ManualFlush to its SegmentViews
and TransformLog. Every actual SegmentView and TransformLog consumer directly
retains its own Ref on the same message AckRecord.

BroadcastAck remains FIFO and waits for all actual refs on the PChannel-wide
message. It does not wait for an all-local materialized frontier.

### Txn

A committed transaction is observed as one atomic WAL message:

- every affected SegmentView retains its own Ref for Insert work;
- Delete bodies produce one TransformLog entry at the transaction timetick and
  retain a TransformLog Ref until that entry's chunk is durable;
- all metadata changes are captured by the next persist batch.

The transaction is not split into independent external recovery messages.

### AlterCollection

The VChannel records collection metadata changes. For a schema change, retained
SegmentViews may flush pending data before moving to the new schema boundary,
and TransformLog may flush preceding Delete entries. Those operations retain
direct Segment and TransformLog Refs.

Non-schema alterations normally require metadata publication only.
BroadcastAck waits for Refs actually retained by the message and does not
query composed module progress.

### AlterLoadConfig And DropLoadConfig

QueryView metadata is the query-resource load trigger. These messages do not
create VChannel-local QueryRuntime references in RecoveryStorage. Any
RecoveryStorage metadata observation is independent from QueryView resource
lifetime.

BroadcastAck follows the common last-reference rule.

### AlterReplicateConfig

RecoveryStorage records replication progress in WAL checkpoint-related state.
It does not require Segment or TransformLog data work unless another observer
retains a Ref. BroadcastAck follows FIFO ordering.

### Database, Alias, RBAC, Resource Group, And Index Broadcasts

This category includes:

- CreateDatabase, AlterDatabase, DropDatabase;
- AlterAlias, DropAlias;
- AlterUser, DropUser, AlterRole, DropRole;
- AlterUserRole, DropUserRole;
- AlterPrivilege, DropPrivilege;
- AlterPrivilegeGroup, DropPrivilegeGroup, RestoreRBAC;
- AlterResourceGroup, DropResourceGroup;
- CreateIndex, AlterIndex, DropIndex.

These messages normally do not mutate local VChannel, Segment, or TransformLog
recovery state. `broadcastAckModule` retains its Ref and acknowledges them after
the record is sealed, prior broadcast ordering, and any Ref retained by another
observer.

### AlterWAL

AlterWAL is PChannel scoped. Local VChannels may flush eligible Segment pending
data and TransformLog entries up to the message timetick. The message-level
AckRecord receives one direct Ref from every actual consumer across all affected
VChannels.

BroadcastAck waits for those refs and FIFO order. It does not wait for a
composed all-local progress value or TransformLog materialization.

### RecoveryBarrier

RecoveryBarrier advances TransformLog's volatile sync-up frontier. If preceding
Delete entries need a chunk flush, TransformLog retains a Ref until durability.
It is not a coordinator broadcast acknowledgement.

## 4. Persist Batch Publication

For every message that mutates recovery metadata:

```text
observe and mutate in-memory state
  -> mark the component dirty
```

RecoveryStorage later freezes one batch boundary, consumes stable
DirtySnapshots covering that boundary, persists all of them, and persists the
frozen checkpoint last. An asynchronous consumer must mutate metadata and mark
it dirty before `Ref.Done()`, so every message in the frozen Ack completed
frontier is represented by the batch snapshots.

## 5. Invariants

1. One data-scanner message owns one Ack record.
2. The implicit dispatch Ref remains retained until RecoveryStorage seals the
   record after all observers return.
3. Every actual Segment and TransformLog consumer retains a direct Ref before
   exposing asynchronous work.
4. Broadcast messages use the same Segment and TransformLog retain rules as
   non-broadcast messages.
5. BroadcastAck waits for actual message refs, not inferred timetick frontiers.
6. TransformLog Ack completion does not wait for materialization.
7. Every persist batch writes all captured DirtySnapshots before its frozen
   checkpoint.
8. Async consumers mutate metadata and mark it dirty before `Ref.Done()`.
9. Checkpoint recovery pairs `LastConfirmedMessageID` with
   `DeliverPolicyStartFrom`.
10. Ack does not provide task ordering or failed completion states.
