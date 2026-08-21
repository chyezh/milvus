# WALSummary Design

WALSummary is the **pchannel-scoped** summary of the transform records (Delete
and Txn-Delete) observed on a physical WAL channel. It is the single owner of
transform-record persistence and plays the role of a pchannel-level
"SegmentView" for transform data: records are staged in memory, persisted to
object storage under a summary store, retained within a budget, and released
through the same MessageRef lifecycle as every other recovery component.

## 1. Scope And Dependencies

```text
recovery (RecoveryStorage)  -> walsummary
vchannel (VChannelRecoveryModule) -> walsummary
walsummary                  -> (no dependency on vchannel / transformlog)
```

The transform consumer of each vchannel (see
[TransformLog](transformlog/transform_log.md)) registers as a `FlushListener`
and receives durable records through flush events; it never reads the summary
store at runtime.

## 2. Object Model

### 2.1 Objects (object storage)

Chunk: one flush of one or more vchannels.

```text
<root>/streamingnode/summary-store/<pchannel>/chunks/chunk.<gen>.term<term>.psc
```

The key carries the term, so a fenced owner can never collide with the
successor's chunks.

Manifest: the chunk index of the current term.

```text
<root>/streammingnode/.../<pchannel>.manifest.<term>
```

### 2.2 Catalog meta (etcd)

```text
streamingnode-meta/wal/<pchannel>/summary-store/pchannel-summary-meta
```

`PChannelSummaryMeta{term}` fences the summary store across term changes.

### 2.3 Protos

```proto
message PChannelSummaryMeta { int64 term = 1; }
message PChannelSummaryManifest { repeated PChannelSummaryChunkIndexEntry chunks = 1; }
message PChannelSummaryChunkIndexEntry {
    uint64 generation = 1;
    uint64 start_timetick = 2;
    uint64 end_timetick = 3;
    uint64 object_size = 4;
    repeated VChannelSummaryChunkIndex vchannels = 5; // per-vchannel section offsets
}
message PChannelSummaryChunkFooter { int64 term = 1; ... }
message VChannelSummaryTransformRecord { uint64 time_tick = 1; TransformDeleteEntry delete = 2; }
```

Legacy per-vchannel formats (`VChannelTransformLogMeta`,
`TransformLogChunk`) are deprecated and kept only for reading old data during
migration.

## 3. Lifecycle And Persistence

```text
ObserveMessage(retained)
  -> build TransformLogEntry (standalone proto)
  -> append to view staging (retained.Clone)
  -> if stagingBytes >= FlushMaxBytes: requestFlush
flush task (summary-owned decision)
  -> collectStaging of every view
  -> write chunk (generation)
  -> publish manifest
  -> deliver FlushedBatch to each vchannel's FlushListener
  -> release handles            (WAL checkpoint may now advance)
```

The summary alone decides when persistence happens:

| Trigger | Path |
|---|---|
| size threshold | `FlushMaxBytes` (staging binary size, configured as `FlushL0MaxSize`) |
| forced persist | `SummaryView.RequestPersistThrough(tt)` (tracker stall / pressure) |
| barrier | `SummaryView.SyncUp(tt)` (flush / flush-all / manual-flush / drop / truncate) |

Handle release happens only after the chunk object AND the manifest record are
durable, so the global WAL checkpoint — which advances past a message only
when every retained handle is released — can never outrun the summary:

```text
materialized frontier <= durable frontier <= WAL checkpoint
```

## 4. Flush Events

```go
type FlushedBatch struct {
    RecordsByVChannel map[string][]*streamingpb.TransformLogEntry
    CoveredTimeTick   uint64
}

type FlushListener interface {
    OnSummaryFlushed(batch *FlushedBatch)
}
```

`Manager.SetFlushListener(vchannel, listener)` registers one listener per
vchannel; the vchannel module unregisters it (`RemoveFlushListener`) together
with `RemoveView` on cleanup. The delivered entries are standalone protos that
survive the handle release. Precondition: a listener must be non-blocking and
must not call back into the manager.

## 5. Recovery And Fencing

`Manager.Recover`:

1. read the catalog meta; if `meta.term > own term`, the store is fenced
   (error); otherwise save the own term;
2. read the manifest; if absent, probe forward for chunks of the own term and
   seal them into a fresh manifest;
3. the manifest is the durable chunk index for the live flush path.

Write arbitration across terms: an `Exist -> Write` of a chunk key is not
atomic; on a byte mismatch the footer is decoded — a footer term greater than
the own term fences the writer, a smaller term is overwritten, an equal term
with identical content is an idempotent retry, and an equal term with
different content is corruption.

## 6. Retention GC

`Manager.GCOnce` releases chunks above `RetentionMaxBytes`
(`streaming.summary.maxBytesPerPChannel`, default 4 GB), bounded below by the
per-vchannel materialization frontiers (`SetMaterializedTimeTick`): records
not yet materialized must never be released. Because the consumer only
consumes durable records, the materialization frontier is a hard lower bound
of the retention.

## 7. Migration

Legacy per-vchannel transform logs (etcd `tl` metas + `<root>/transform-log/...`
chunk objects) are migrated once: the recovery path reads every legacy chunk
of each vchannel, validates strict ascending order, writes them into one
summary chunk (all vchannels), publishes the manifest — the commit point — and
removes the legacy metas. The migration is idempotent: a manifest that
already owns chunks is a no-op.
