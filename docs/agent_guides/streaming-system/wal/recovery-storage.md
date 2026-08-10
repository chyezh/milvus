# RecoveryStorage

Persists WAL consumer state to the catalog (etcd) and object storage. **Core invariant**: from any WAL position + the corresponding persisted state, RecoveryStorage can replay the WAL forward and recover a fully consistent in-memory state.

## Persisted State

- **WALCheckpoint** (etcd): `TimeTick` is the logical consumed-through
  boundary; `MessageID` is that message's `LastConfirmedMessageID`, used as a
  conservative resume anchor with `DeliverPolicyStartFrom`. The checkpoint also
  contains `ReplicateCheckpoint` for secondary clusters and `AlterWalState` for
  WAL backend migration.
- **VChannel metadata** (etcd): Per-VChannel collection info, partition list,
  schema history, lifecycle state, and the greatest sealed data version retained
  after obsolete segment assignments are cleaned.
- **Segment assignments** (etcd): Per-segment growing/flushed status with row
  count and binary size stats. Tombstoned assignments are deleted only after
  their sealed data version is covered by a persisted VChannel summary.
- **Segment data** (object storage): Sealed segment binlog, indexes, and stats files.
- **TransformLog metadata** (etcd): Per-VChannel durable chunk range,
  checkpoint, materialization, and truncation cursors.
- **TransformLog chunks** (object storage): Durable Delete/transform payloads
  stored in deterministic per-VChannel chunk paths.

## Recovery Flow

1. **Persist recovery** (`recoverRecoveryInfoFromMeta`): Load checkpoint, VChannel metadata, and segment assignments from catalog in parallel.
2. **Recovery barrier append**: Append a persisted
   [RecoveryBarrier](../message/message-semantic-recovery-barrier.md) message as
   the first recovery WAL write for this PChannel. The append proves that the
   recovering node can write this WAL; on backends with writer fencing, currently
   Woodpecker, it also prevents old owners from appending later entries. If the
   append fails because the writer is fenced, recovery must stop and the node
   must not serve the PChannel.
3. **Stream recovery** (`recoverFromStream`): Build a `RecoveryStream` from the
   checkpoint's MessageID through the `RecoveryBarrier` message. Replay all
   messages to reconstruct in-memory state. Extract uncommitted `TxnBuffer`.
   Applying the empty `RecoveryBarrier` initializes or advances per-VChannel
   query MVCC for every VChannel that is live after replay reaches the barrier
   and makes the corresponding growing and transforming resources visible at the
   barrier TimeTick. Runtime-specific handling is defined by
   [StreamingNode Growing Segment Runtime Design](../../../design-docs/design_docs/qviews/snview/growing_segment_runtime.md)
   and
   [QueryNode QueryView Resource Preparation Design](../../../design-docs/design_docs/qviews/qnview/querynode_queryview_resource_preparation.md).

`RecoveryBarrier` avoids persisting per-VChannel query MVCC snapshots in the
checkpoint. The checkpoint remains focused on recovery position and durable WAL
state; the barrier establishes the query-resource baseline as part of recovery
replay.

## Checkpoint Persistence

RecoveryStorage freezes a persist-batch boundary before consuming module dirty
snapshots:

- Meta checkpoint is the latest completely observed WAL point frozen for the
  batch.
- Data checkpoint is the minimum of that Meta point and the frozen continuous
  Message Ack completed frontier.
- All captured DirtySnapshots are persisted before the batch checkpoint.
- Every actual Segment and TransformLog data consumer retains a direct Ref until
  its object-storage work succeeds and its metadata changes are marked dirty.
- Broadcast acknowledgement retains its own Ref and waits for a sealed record
  with no other Refs, independently from checkpoint progress.
- `AckSyncUp` disables Coordinator FastAck and waits for the RecoveryStorage
  consumer Ack; it does not require checkpoint persistence before that Ack.
- Retry, cancellation, and close keep incomplete Refs retained. Restart rebuilds
  AckRecords from the persisted Data checkpoint.

See
[WAL Message Ack Design](../../../design-docs/design_docs/wal/message_ack.md).

## Key Packages

- `internal/streamingnode/server/wal/recovery/` — `RecoveryStorage`, `RecoverySnapshot`, WAL replay orchestration, meta recovery and background persist task
- `internal/streamingnode/server/wal/moduleapi/` — common RecoveryStorage module contracts and dirty snapshots
- `internal/streamingnode/server/wal/messageack/` — sealed reference-count
  Message Ack records and continuous completion tracking
- `internal/streamingnode/server/wal/vchannel/` — VChannel metadata, schema history, partition lifecycle, and VChannel tombstones
- `internal/streamingnode/server/wal/vchannel/segment/` — growing segment assignment metadata, Insert/L1 persistence, segment lifecycle, and segment tombstones
- `internal/streamingnode/server/wal/vchannel/transformlog/` — Delete TransformLog storage, recovery, chunk replay, scanners, and truncation
- `internal/streamingnode/server/wal/checkpoint/` — WAL checkpoint state, frozen persist-batch points, and advancement rules
