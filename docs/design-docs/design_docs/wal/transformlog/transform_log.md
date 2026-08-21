# TransformLog Design

TransformLog is the VChannel-level transform **consumer**: it turns the
transform records of the pchannel-scoped WALSummary into DataCoord-managed L0
segments. Delete is the initial transform payload. QueryNode and StreamingNode
query resources consume the L0 output to advance transform visibility.

Per-message ownership is defined by
[WAL Message Ack Design](../message_ack.md).

## 1. Ownership

```text
RecoveryStorage (pchannel)
  -> PChannelRecoveryManager
       +-- VChannelRecoveryModule A
       |     +-- summaryView (walsummary.SummaryView of vchannel A)
       |     +-- TransformLog A   (materialize-only consumer)
       +-- VChannelRecoveryModule B
       |     +-- summaryView (walsummary.SummaryView of vchannel B)
       |     +-- TransformLog B
       +-- walsummary.Manager (pchannel-scoped summary store, owns persistence)
             +-- views per vchannel
             +-- flushListeners per vchannel  -> TransformLog.OnSummaryFlushed
```

Persistence lives exclusively in the [WALSummary](../summary.md) of the
pchannel; the TransformLog owns **no** buffer, no chunk objects, and no catalog
metadata.

TransformLog owns:

- the in-memory materialization window (`pending`): durable records of its
  vchannel after the committed frontier, replaced by flush events;
- the committed materialization frontier `materialized_time_tick`, carried by
  `VChannelMeta.transform_materialized_time_tick`;
- the L1 upper bound derived from uncommitted L0 segments;
- L0 materialization (batching, ordering, retry).

## 2. Persistence Model

The transform records are persisted by the WALSummary as pchannel chunks with
a per-vchannel section, plus a manifest. The summary decides **entirely on its
own** when records become durable:

- autonomous flush at the binary size threshold;
- forced persistence through `RequestPersistThrough` (tracker stall / under
  pressure);
- barrier `SyncUp` (flush / flush-all / manual-flush / drop / truncate ...).

The TransformLog never reads the summary store and never triggers persistence.
It observes the outcome: after a chunk and its manifest are durable, the
summary delivers a `FlushedBatch` per vchannel through `FlushListener`, and
only then releases the message handles. The handle lifecycle therefore
guarantees: **WAL checkpoint <= durable summary frontier**, and the consumer's
window only ever contains durable records, so

```text
materialized_time_tick <= durable frontier <= WAL checkpoint
```

## 3. Message Classification

| Kind | WAL messages | Effect |
|---|---|---|
| Payload | Delete, committed Txn containing Delete | `summaryView.ObserveMessage` appends one ordered Delete record to the summary staging. |
| Barrier | RecoveryBarrier, Flush, ManualFlush, FlushAll, DropPartition, DropCollection, TruncateCollection, CreateCollection, schema-changing AlterCollection, AlterWAL | `RequestMaterializeThrough(tt)` records the frontier and `summaryView.SyncUp(tt)` asks the summary to make everything through the barrier durable. |
| None | Insert and other messages | No transform effect. |

A committed Txn creates one record at the outer Txn TimeTick and stores Delete
blocks for all Delete children.

## 4. Observe And Materialization Trigger

There is one Observe path for recovery and live messages:

1. classify the message;
2. return for `None`;
3. `summaryView.ObserveMessage`: build the transform record (a standalone
   proto), append it to the view staging, retain a message handle, and let the
   summary decide about flushing (size threshold);
4. for Barrier messages, additionally `RequestMaterializeThrough` +
   `summaryView.SyncUp`.

The TransformLog itself schedules materialization only when two conditions
hold:

- a request exists: `RequestMaterializeThrough` recorded a frontier, or the L1
  upper bound advanced via `SetMaterializeUpperBound`;
- the durable frontier covers the target: a flush event moved the window
  (`DurableTimeTick() >= target`).

## 5. Flush Event And The Window

A successful summary flush delivers to the TransformLog:

```go
type FlushedBatch struct {
    RecordsByVChannel map[string][]*TransformLogEntry // ascending timetick
    CoveredTimeTick   uint64
}
```

`OnSummaryFlushed`:

1. appends the vchannel's entries after the committed frontier to `pending`;
2. advances `durableTimeTick`;
3. schedules a materialize task for the retained request when the window now
   covers part of it (deduplicated against a pending task).

The entries are standalone protos that survive the handle release: the
consumer may retain them as long as needed.

## 6. L0 Materialization

Materialization converts the windowed Delete entries into DataCoord-managed L0
deltalogs. It may be triggered by explicit barrier requests or size pressure.

Materialization:

- consumes only **durable** records (never the summary staging or store);
- does not retain source WAL messages;
- does not delay BroadcastAck;
- does not gate the global recovery checkpoint;
- does not pass the earliest uncommitted L1 Segment's creation TimeTick;
- commits `materialized_time_tick` into `VChannelMeta`, marking the vchannel
  snapshot dirty for the next RecoveryStorage checkpoint;
- may be retried idempotently at the logical level.

`VChannelRecoveryModule` derives one inclusive materialization upper bound from
its SegmentViews:

```text
upper_bound = min(create_segment_time_tick of every Segment with l1_commit_done = false)
```

When there is no such Segment, the bound is unbounded. The creation TimeTick is
safe to include because rows assigned to that Segment have later TimeTicks.
This guarantees that an L0 Segment never covers a transform range whose L1
data has not completed its final commit.

The target of one batch is `min(requested, upper_bound, durableTimeTick)`.
TransformLog keeps the requested materialization TimeTick separately from the
currently executable TimeTick, schedules
`min(requested, upper_bound, durable)` and retains the original request. Every
completed L1 final commit makes the owning VChannel recompute the bound, which
retries the retained request without requiring another WAL trigger. Batches
are capped by rows/bytes; a capped batch schedules a continuation task whose
predecessor is the current one, keeping batches strictly sequential. A task
whose data has not been flushed yet stays delayed in the scheduler until a
flush event covers its target.

Physical duplicate L0 output after a crash is outside the WAL checkpoint
protocol and requires lifecycle idempotency or reconciliation.

## 7. Recovery

1. the summary recovers its manifest (and fences the term via the catalog
   meta);
2. legacy per-vchannel transform logs are migrated into the summary (see
   Recovery Storage);
3. for every vchannel, recovery loads the initial materialization window once:
   `summaryManager.ReadTransformEntries(vchannel, materializedTimeTick, +inf)`
   — the only read of the summary store in the whole consumer path;
4. the module restores `materialized_time_tick` from
   `VChannelMeta.transform_materialized_time_tick` and seeds the window;
5. live operation continues from the restored frontier; runtime flushes
   replace the window through flush events.

## 8. GC

The summary releases chunk objects by retention budget, bounded below by the
per-vchannel materialization frontiers mirrored via
`Manager.SetMaterializedTimeTick`. Because the consumer only ever consumes
durable records, a chunk fully covered by the materialization frontier is
guaranteed to have been consumed, so releasing it cannot lose transform data.

## 9. Invariants

1. TransformLog is VChannel-owned; persistence is pchannel-owned (WALSummary).
2. All entry positions use source WAL TimeTick.
3. `materialized_time_tick <= durable frontier <= WAL checkpoint`.
4. A Delete handle releases only after the chunk and manifest are durable
   (summary-owned), never before the flush event was delivered.
5. Barrier visibility advances only after the summary made the records
   durable.
6. L0 materialization does not gate source-message Ack.
7. The transform consumer never triggers persistence and never reads the
   summary store at runtime.
