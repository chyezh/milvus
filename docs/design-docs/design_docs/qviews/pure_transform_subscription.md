# Pure Transform Subscription Design

> This document describes the final design of the pure transform subscription
> path used by QueryViews. It focuses on module relationships, interface
> boundaries, and workflows.
> Reference: [Distributed Query View Design](README.md), [QueryView State Machine](query_view_state_machine.md), [Syncer Design](syncer.md), [streaming.proto](../../../../pkg/proto/streaming.proto), [view.proto](../../../../pkg/proto/view.proto).

## 1. Overview

QueryViews move sealed-segment ownership to QueryNode. StreamingNode no longer
applies incremental deletes to sealed segments and no longer forwards
per-segment delete traffic. Instead, QueryNode subscribes to a pure transform
stream from the StreamingNode that owns the vchannel, caches transform effects
locally, and applies them to local sealed segments.

The first transform type is Delete. The protocol is extensible for future
transform effects, but this design only defines Delete.

### Architecture Position

```
                            QueryView / DataView
                    delete_apply_start_after_timetick
                                      │
                                      ▼
                         RetentionHintTracker
                                      │
                                      ▼
Delete WAL ──► RecoveryStorage / GrowingModule ──► TransformLogStore
                                      │                 │
                                      │                 ├──► TransformLogManager
                                      │                 │        │
                                      │                 │        ▼
                                      │                 │  SubscribeTransform
                                      │                 │        │
                                      │                 │        ▼
                                      │                 │  QN TransformLogClient
                                      │                 │        │
                                      │                 │        ▼
                                      │                 │  QN client-side cache
                                      │                 │        │
                                      │                 │        ▼
                                      │                 │  QN SegmentManager/DeleteBatch
                                      │                 │
                                      │                 └──► TransformLogMaterializer
                                      │                          │
                                      ▼                          ▼
                         WAL data checkpoint              L0 Segment / DataCoord
```

### Design Principles

- **SN-owned source of truth**: StreamingNode owns the durable per-vchannel
  transform log. L0 segments are asynchronous materialized outputs, not the
  subscription replay source.
- **Effect protocol**: SubscribeTransform emits transform effects, not WAL
  records. WAL message id, last-confirmed id, transaction context, and WAL
  properties are not exposed.
- **VChannel-level sharing**: The QN-side TransformLog client maintains one
  transform cache and one upstream subscription client per vchannel. Segments do
  not create independent upstream subscriptions.
- **No server-side ack**: QN tracks local apply progress, retained cache range,
  and replay barriers. SN retention is driven by QueryView/DataView hints, not
  by per-subscription progress acks.
- **Forward-only recovery**: If QN cache or SN retained history cannot cover a
  required start point, the QueryView becomes Unrecoverable. There is no gap
  filling or rewind path in the first implementation.

## 2. Component Model

### 2.1 Component Responsibilities

| Component | Responsibility | Interface Boundary |
| --- | --- | --- |
| `GrowingModule` | Converts Delete WAL messages into durable transform log entries and installs the transform-log data barrier for WAL data checkpoint advancement. | Depends on `TransformLogStore.Append` and transform-log data barrier; does not depend on SubscribeTransform stream code. |
| `TransformLogStore` | Durable per-vchannel transform log recovered and owned by RecoveryStorage. Provides append, snapshot, range read, watch, watermark, data durability barrier, and GC operations. | Depends on SN catalog metadata, object storage, and RecoveryStorage lifecycle/data-barrier notification; does not depend on QN state or DataCoord L0 as the normal replay source. |
| `TransformLogManager` | Serves subscriptions by validating vchannel ownership, sampling durable position, replaying retained entries, and watching future entries. | Depends on `TransformLogStore`; must not open WAL scanners. |
| `TransformLogMaterializer` | Materializes accumulated transform log ranges into L0 delete segments and commits them to DataCoord. | Depends on `TransformLogStore` and DataCoord L0 commit API; does not depend on QN cache progress. |
| `RetentionHintTracker` | Applies QueryView/DataView retention hints to SN transform log retention. | Depends on `delete_apply_start_after_timetick`; does not use subscription acks. |
| `QN TransformLogClient` | Owns the shared QN-to-SN stream, multiplexes vchannel subscriptions, and owns the vchannel client-side cache. | Depends on SubscribeTransform; does not create per-segment streams. |
| `VChannelTransformCache` | Client-side cache owned by `QN TransformLogClient`. Stores ordered transform effects and provides local replay/watch for all QN sealed segments in one vchannel. | Depends on stream responses and local retention hints; does not replay WAL/DataCoord directly. |
| `SegmentManager/DeleteApplier` | Loads sealed segments, requests replay/watch from the TransformLog client cache, applies cached/future deletes, and reports Ready/Unrecoverable. | Depends on TransformLog client cache interface and local `DeleteBatch` apply path; does not own upstream subscription buffer. |

### 2.2 Ownership

| Resource | Owner | Notes |
| --- | --- | --- |
| Durable transform log | RecoveryStorage on StreamingNode | Recovered with WAL checkpoint and vchannel metadata; primary replay source for SubscribeTransform. |
| L0 segment lifecycle | DataCoord | Receives materialized output from SN. |
| QN transform cache | QN TransformLog client | Shared by all local sealed segments of one vchannel. |
| Retention watermark | QueryView/DataView hints | Applied to SN transform log and QN cache. |
| Segment apply state | QueryNode | Delete application is local and idempotent. |

## 3. Data Model And Positions

### 3.1 Transform Log Entry

```go
type TransformLogEntry struct {
    VChannel     string
    FromTimeTick uint64
    ToTimeTick   uint64
    DataTimeTick uint64
    Blocks       []*streamingpb.TransformDeleteBlock
}
```

`TransformLogEntry` is the durable unit stored by StreamingNode. Entries are
ordered by transform timetick within one vchannel. `FromTimeTick` and
`ToTimeTick` describe the retained Delete effect block range. `DataTimeTick` is
an internal durability position used by RecoveryStorage data checkpoint barrier;
it is not emitted in `TransformMessageBatch` and is not used by QN replay
filtering. Multiple entries may have the same Delete effect range when one
logical Delete operation is split into multiple WAL messages; `DataTimeTick`
keeps those entries distinct while preserving idempotent WAL replay.

### 3.2 Transform Log Metadata

```go
type VChannelTransformLogMeta struct {
    VChannel                    string
    RetentionStartAfterTimeTick uint64
    MaterializedTimeTick        uint64
    MaterializedDataTimeTick    uint64
    Chunks                      []TransformLogChunkMeta
}
```

Fields:

- `RetentionStartAfterTimeTick`: oldest exclusive start point from which SN can
  replay all Delete effects without loss. A subscription is valid only when its
  `start_after_time_tick >= RetentionStartAfterTimeTick`.
- `MaterializedTimeTick`: highest Delete effect timetick included in L0
  materialization output.
- `MaterializedDataTimeTick`: highest `TransformLogEntry.DataTimeTick`
  consumed by L0 materialization. This is the materializer read cursor and is
  distinct from `MaterializedTimeTick` because multiple WAL Delete entries may
  carry the same Delete effect timetick.
- `Chunks`: storage index for durable transform log chunks.

### 3.3 Delete Block

`TransformDeleteBlock` contains:

- `partition_id`
- `primary_keys`
- `time_tick`

`time_tick` is the actual delete timestamp used by QN `DeleteBatch`, not merely
the WAL message timetick. Runtime append and recovery must generate the same
ordered block layout from the same delete data.

`TransformDeleteBatch.blocks` are ordered by `time_tick` in non-decreasing
order. Blocks with the same `time_tick` keep the stable order produced by SN
transform log reader.

### 3.4 TimeTick Timeline

Transform subscription uses TimeTick as a vchannel-ordered Delete effect
position, not as an idle time value. The key question is not "what is the latest
TimeTick seen by every segment", but "from which exclusive start point can SN
replay all Delete effects without loss".

```
vchannel timeline:

R = RetentionStartAfterTimeTick
S = start_after_time_tick requested by QN
M = MaterializedTimeTick
D = MaterializedDataTimeTick

        not replayable             retained no-loss replay window
        ┌───────────────┐          ┌───────────────────────────────────
... ─── R ───────────── S ─────── d1 ───── d2 ───────────────► future
                            │      │       │
                            │      │       └─ Delete effect block
                            │      └───────── Delete effect block
                            └──────────────── subscription starts after S

M records the highest Delete effect timestamp included in L0 output. D records
the WAL data position through which transform log entries have been consumed by
the materializer. L0 output effect range is computed from the materialized
Delete blocks themselves, not from the TransformLog entry read order.
```

The meanings are:

- `TransformDeleteBlock.time_tick`: the actual Delete effect timestamp used by
  QN `DeleteBatch`. It is the position used to filter `(start_after_time_tick,
  +future)`.
- `TransformLogEntry.DataTimeTick`: the WAL data checkpoint position covered by
  the durable transform append. It lets RecoveryStorage keep WAL checkpoint
  advancement separate from effect replay filtering when a Delete message
  contains effect timestamps different from the message timetick.
- `RetentionStartAfterTimeTick`: the lower bound of the no-loss replay window.
  If `S >= RetentionStartAfterTimeTick`, SN can serve all retained and future
  Delete effects with `time_tick > S`. If `S` is older, the QueryView is
  Unrecoverable.
- `start_after_time_tick`: an exclusive local requirement from QN. SN sends
  Delete effects with `time_tick > start_after_time_tick`.
- `caught_up`: a request-level barrier, not a TimeTick watermark. It means SN
  has replayed all currently retained Delete effects after the requested start
  point and has installed the live watch for future Delete effects.
- `materialized_time_tick`: the highest Delete effect timestamp already
  included in L0 output. It does not prove subscription replay completeness and
  does not affect retention.
- `materialized_data_time_tick`: the materializer read cursor over
  `TransformLogEntry.DataTimeTick`. L0 materialization resumes from this value
  so later WAL Delete entries with the same Delete effect timetick are not
  skipped.

Therefore absence of `TransformDeleteBlock` after `S` does not require exposing
a latest TimeTick to QN. It is meaningful because the subscription was created
from a start point inside the no-loss replay window, and `caught_up` proves the
current retained suffix after that start point has been drained.

### 3.5 Subscription Positions

| Position | Meaning |
| --- | --- |
| `start_after_time_tick` | Exclusive subscription start point. SN sends effects with `time_tick > start_after_time_tick`. |
| `caught_up` | Barrier proving the current retained suffix after the requested start point has been replayed and future effects are watched. |
| `retention_start_after_time_tick` | Oldest exclusive start point currently replayable without Delete loss. |

SN does not send idle progress messages. TimeTick in this protocol is bound to
real Delete effects. `caught_up` is a stream barrier, not a TimeTick value.

## 4. Interfaces

### 4.1 TransformLogStore

Expected semantic interface:

```go
type TransformLogStore interface {
    Append(ctx context.Context, entry TransformLogEntry) error
    DataBarrier(vchannel string) DataBarrier
    Snapshot(ctx context.Context, vchannel string) (VChannelTransformLogMeta, error)
    OpenSubscriptionCursor(ctx context.Context, vchannel string, startAfter uint64) (TransformLogCursor, error)
    ReadRange(ctx context.Context, vchannel string, startAfter uint64, through uint64) (TransformLogReader, error)
    ReadRangeByDataTimeTick(ctx context.Context, vchannel string, startAfter uint64, through uint64) (TransformLogReader, error)
    SetRetentionStartAfter(ctx context.Context, vchannel string, startAfter uint64) error
    AdvanceMaterialized(ctx context.Context, vchannel string, through uint64, dataThrough uint64, materialization L0TransformMaterialization) error
}
```

Implementation may split data into object-storage chunks and persist metadata in
SN catalog. Append must be idempotent by `(vchannel, from_time_tick,
to_time_tick, data_time_tick)`. `Append` is invoked only for actual Delete
effects. Ranges without Delete effects do not need placeholder transform
entries.

`DataBarrier` is the RecoveryStorage checkpoint barrier exposed by
`TransformLogStore`. It reports the highest `DataTimeTick` whose transform log
append is durable for the vchannel. RecoveryStorage owns checkpoint advancement;
`TransformLogStore` only contributes this barrier and notifies RecoveryStorage
when the barrier may advance.

`OpenSubscriptionCursor` is the subscription-facing read API. It creates a
linearized cursor that contains:

- a retained reader for currently stored Delete entries with
  `time_tick > startAfter`;
- a live watcher for future Delete entries after that retained suffix.

The linearization point is an internal store cursor, not a TimeTick exposed to
QN. `ReadRange` is an effect-time range reader. L0 materialization uses
`ReadRangeByDataTimeTick` because its no-skip cursor is
`MaterializedDataTimeTick`, not the Delete effect timestamp.

### 4.2 SubscribeTransform RPC

The protocol lives in `pkg/proto/streaming.proto` under
`StreamingNodeHandlerService`.

```proto
service StreamingNodeHandlerService {
    rpc SubscribeTransform(stream TransformRequest)
        returns (stream TransformResponse) {}
}
```

`TransformRequest` is a oneof:

| Request | Fields | Meaning |
| --- | --- | --- |
| `create` | `subscription_id`, `vchannel`, `start_after_time_tick` | Creates one vchannel subscription on the shared stream. |
| `refresh` | `subscription_id`, `vchannel`, `start_after_time_tick` | Requests a fresh replay barrier for an existing vchannel subscription and replays any retained effects needed for that start point. |
| `close_subscription` | `subscription_id` | Closes one subscription without closing the stream. |
| `close_stream` | none | Gracefully closes the whole stream. |

`TransformResponse` is a oneof:

| Response | Fields | Meaning |
| --- | --- | --- |
| `create` | `subscription_id`, `vchannel`, `start_after_time_tick`, `retention_start_after_time_tick` | Acknowledges subscription creation and reports the no-loss replay lower bound. |
| `message_batch` | `subscription_id`, `vchannel`, payload | Carries real transform effects. |
| `caught_up` | `subscription_id`, `vchannel` | Marks that current retained replay after the requested start point is drained and future effects are watched. |
| `subscription_error` | `subscription_id`, `vchannel`, `StreamingError` | Reports an error for one subscription on the shared stream. |
| `close_stream` | none | Acknowledges graceful stream close. |

`TransformMessageBatch.payload` is a oneof. The initial payload is
`TransformDeleteBatch delete`, containing ordered `TransformDeleteBlock` values.
No separate transform type field is needed.

`refresh` does not send a separate response type. It reuses
`TransformMessageBatch` for any effects in the refreshed range and
`TransformSubscriptionCaughtUp` for the replay barrier.

`TransformProgressAck` is intentionally omitted.

### 4.3 QN TransformLog Client Interface

`QN TransformLogClient` owns both the upstream `SubscribeTransform` stream and
the per-vchannel client-side cache. QueryNode components consume it through a
cache/replay interface; they do not maintain their own subscription buffer.

Expected semantic cache interface:

```go
type VChannelTransformCache interface {
    ApplyCreate(resp CreateTransformSubscriptionResponse) error
    AppendBatch(batch TransformMessageBatch) error
    MarkCaughtUp(caughtUp TransformSubscriptionCaughtUp) error
    MarkUnrecoverable(err StreamingError)
    Replay(startAfter uint64) ([]*TransformDeleteBlock, error)
    Watch(startAfter uint64) (Watcher, []*TransformDeleteBlock, error)
    ExpireThrough(timeTick uint64)
}
```

The cache retains a no-loss start point. A replay request with
`start_after_time_tick` older than the retained start fails and makes the view
Unrecoverable. Readiness for a newly loaded segment also requires a caught-up
barrier for its requested start point.

The cache must not treat the highest cached Delete effect timetick as a
deduplication watermark. Transform entries are consumed in WAL data order, and a
later entry may contain a smaller `TransformDeleteBlock.time_tick` than an
earlier entry. QN cache keeps every non-expired, non-duplicate block whose
`time_tick > RetentionStartAfterTimeTick`, stores replay data ordered by
`time_tick`, and filters watcher notifications by each watcher's own
`start_after_time_tick`.

## 5. Workflows

### 5.1 WAL Scan, Delete Append, And Checkpoint

```
RecoveryStorage scanner
  -> GrowingModule observes ordered vchannel messages
  -> for Delete: build TransformDeleteBlock list and TransformLogStore.Append
  -> compose DataBarrier with TransformLogStore.DataBarrier(vchannel)
  -> RecoveryStorage data checkpoint advances only after the composed barrier reaches the Delete timetick
```

Failure rules:

- If transform log append has not completed, the transform-log data barrier must
  stay behind the Delete timetick, so WAL data checkpoint cannot pass the Delete.
- If append completed but checkpoint persistence did not, recovery may replay
  the Delete and must deduplicate through append idempotency.

### 5.2 Subscription Create And Replay

For `CreateTransformSubscriptionRequest(start_after_time_tick = S)`:

1. `TransformLogManager` validates vchannel ownership.
2. It loads `VChannelTransformLogMeta`.
3. If `S < RetentionStartAfterTimeTick`, it returns replay-unavailable.
4. It creates a linearized retained reader plus live watcher from `S`.
5. It sends `CreateTransformSubscriptionResponse`.
6. It reads currently retained transform entries with `time_tick > S` and sends
   `TransformMessageBatch`.
7. It sends `TransformSubscriptionCaughtUp`.
8. It continues forwarding future entries from the watcher.

If there are no currently retained Delete effects after `S`, SN only sends the
create response and caught-up barrier. This is safe because `S` is inside the
no-loss replay window.

### 5.3 Replay Barrier Refresh Without Idle Progress

SN does not push periodic idle progress on the transform stream. QN asks for a
fresh replay barrier only when one of these events happens:

1. A vchannel subscription is created.
2. The stream reconnects and active vchannel subscriptions are recreated.
3. QN needs to load a segment whose `delete_apply_start_after_timetick` is newer
   than an already caught-up replay point, so the TransformLog client sends a
   refresh for that vchannel on the existing shared stream.

Refresh uses the same replay rule as create:

```
cache retained start is R
new segment requires start_after_time_tick = S
if S >= R but the client needs a fresh replay barrier:
  QN sends refresh(start_after_time_tick = S)
  SN sends currently retained Delete effects with time_tick > S
  SN sends caught_up
```

If the refreshed suffix contains no Delete blocks, the caught-up barrier still
proves the current retained suffix after `S` has been drained. This is not idle
TimeTick synchronization; it is a demand-driven replay barrier.

### 5.4 QueryNode Segment Load

When QN receives a Preparing QueryView:

1. SegmentManager loads the assigned sealed segments.
2. It registers segments through the TransformLog client cache using
   `QueryViewMeta.delete_apply_start_after_timetick`.
3. If the TransformLog client needs a fresh replay barrier for that start point,
   it refreshes on the shared stream.
4. The TransformLog client cache replays local transform blocks after that start
   point.
5. DeleteApplier applies those blocks to loaded sealed segments through local
   `DeleteBatch`.
6. If the local retained start can cover the required start point and the
   caught-up barrier has been received, the segment can report Ready.
7. Future transform batches are applied to all registered local segments of the
   vchannel.

If the local retained start is newer than the required start point, QN reports
Unrecoverable for the QueryView.

### 5.5 Stream Reconnect

When the QN-to-SN stream breaks:

1. `QN TransformLogClient` opens a new stream to the owner SN.
2. For each active vchannel, it reads the local retained start position `R`.
3. It recreates the subscription with `start_after_time_tick = R`.
4. New batches are appended to the same client-side cache and applied to
   registered segments.

Duplicate delivery is allowed because Delete apply is idempotent by
`(PK, time_tick)`.

If SN returns replay-unavailable during reconnect, QN marks affected QueryViews
Unrecoverable. No rewind or gap filling is attempted.

### 5.6 Retention And GC

For one vchannel:

```
retention_start_after = min(
    active QueryView.delete_apply_start_after_timetick,
    active DataView.delete_apply_start_after_timetick,
)
```

Retention flow:

1. Retention hints arrive from QueryView/DataView.
2. `RetentionHintTracker` advances `RetentionStartAfterTimeTick`.
3. `TransformLogStore` drops chunks whose `ToTimeTick <= retention_start_after`.
4. QN cache expires local blocks through the same watermark.
5. Subscription requests older than the retained range return replay-unavailable.

L0 materialization progress does not affect retention. Only QueryView/DataView
hints decide transform log GC.

### 5.7 L0 Materialization

```
TransformLogStore unread materialized range
  -> TransformLogMaterializer
  -> write L0 deltalog blob
  -> CommitL0Segment to DataCoord
  -> AdvanceMaterialized
```

Materialization is triggered by row count, bytes, time, or explicit
flush/barrier thresholds.

```go
type L0TransformMaterialization struct {
    VChannel         string
    FromTimeTick     uint64
    ToTimeTick       uint64
    DataFromTimeTick uint64
    DataToTimeTick   uint64
    SegmentID        int64
    Deltalogs        []*datapb.FieldBinlog
}
```

Materialization retry must be idempotent. Subscription does not wait for L0
materialization and does not use materialized L0 as the normal replay index.

Before committing an L0 segment to DataCoord, `TransformLogMaterializer` records
the generated L0 batch in a deterministic TransformLogStore object path keyed by
`(vchannel, from_time_tick, to_time_tick, data_from_time_tick,
data_to_time_tick)`. The record contains the segment id, deltalogs,
start/checkpoint positions, transform blocks, and data range. If SN crashes
after writing the L0 batch or after `CommitL0Segment` but before advancing the
materialized frontier, recovery retries the same data range, reloads the record,
and submits the same segment id and deltalogs again. This makes the retry
idempotent at the logical L0 segment level and avoids producing a second L0
segment for the same transform data range.

### 5.8 StreamingNode Recovery

On StreamingNode recovery:

1. RecoveryStorage loads WAL checkpoint, vchannel meta, segment assignment meta,
   and `VChannelTransformLogMeta`.
2. RecoveryStorage constructs `TransformLogStore`, reopens transform log chunks,
   and restores retention, materialized, and data-barrier state.
3. RecoveryStorage injects the recovered `TransformLogStore` into GrowingModule,
   TransformLogManager, RetentionHintTracker, and TransformLogMaterializer.
4. RecoveryStorage replays WAL from the recovery data checkpoint.
5. For each replayed Delete, GrowingModule appends its Delete effects to the
   recovered `TransformLogStore` if they are not already present, and the
   transform-log data barrier must reach the Delete timetick before the data
   checkpoint can pass it. Non-Delete messages do not create transform log
   entries.
6. TransformLogMaterializer resumes from `MaterializedDataTimeTick` while
   keeping `MaterializedTimeTick` as the effect range recorded in L0 output.
7. TransformLogManager serves subscriptions from the recovered transform log
   after vchannel ownership is active.

Crash handling:

| Crash Point | Recovery Behavior |
| --- | --- |
| Before transform log append | WAL data checkpoint cannot pass the Delete; recovery appends it again. |
| After transform log append but before checkpoint persist | WAL may replay the Delete; append idempotency removes duplicates. |
| Before L0 materialization | Subscription is unaffected; materializer resumes from materialized watermark. |
| During or after `CommitL0Segment` | Materialization retry uses deterministic range metadata or idempotent materialization record to avoid duplicate logical output. |

## 6. Errors

Transform subscription reuses `StreamingError`.

| Scenario | Error code |
| --- | --- |
| VChannel is not served by this SN | `STREAMING_CODE_CHANNEL_NOT_EXIST` or `STREAMING_CODE_CHANNEL_FENCED` |
| SN is shutting down | `STREAMING_CODE_ON_SHUTDOWN` |
| Invalid subscription request | `STREAMING_CODE_INVAILD_ARGUMENT` |
| Transform log reader internal failure | `STREAMING_CODE_INNER` |
| Requested start is older than retained transform log | `STREAMING_CODE_UNRECOVERABLE` |

Replay-unavailable is treated as an unrecoverable view preparation failure. QN
marks affected QueryViews Unrecoverable instead of retrying locally or filling
the gap from another source.

## 7. Invariants

1. Subscription start point is exclusive.
2. One QN-to-SN stream multiplexes all vchannel transform subscriptions between
   that QN and SN.
3. Transform protocol emits effects, not WAL records.
4. SN transform log is the normal replay source for subscriptions.
5. L0 segments are asynchronous materialized outputs and do not drive
   subscription replay.
6. WAL data checkpoint cannot advance past a Delete until the transform-log
   data barrier for that vchannel has reached the Delete timetick.
7. RecoveryStorage owns DataCheckpoint advancement; `TransformLogStore`
   contributes a data barrier and barrier-updated notifications only.
8. `TransformLogStore` is recovered by RecoveryStorage before GrowingModule,
   TransformLogManager, RetentionHintTracker, or TransformLogMaterializer use it.
9. `RetentionStartAfterTimeTick` is the lower bound of the no-loss replay
   window. SN can serve all Delete effects with `time_tick > S` for any
   `S >= RetentionStartAfterTimeTick`.
10. Transform log append is idempotent by vchannel, transform timetick range,
    and `DataTimeTick`, so split Delete messages with the same effect timetick
    are retained without duplicating WAL replay of the same message.
11. QN vchannel cache is owned by the QN-side TransformLog client and shared by
   all local sealed segments of that vchannel.
12. QN SegmentManager consumes the TransformLog client cache interface; it does
    not own the upstream subscription buffer.
13. QN segment readiness depends on replaying cached/current Delete effects and
    receiving a caught-up barrier for the required start point.
14. QN may refresh a vchannel replay barrier on demand, but it must still reuse
    the shared QN-to-SN stream and shared vchannel cache.
15. Cache miss or replay-unavailable makes the QueryView Unrecoverable.
16. Delete delivery is at-least-once and idempotent by `(PK, time_tick)`.
17. Retention is driven by QueryView/DataView hints, not by subscription ack or
    L0 materialization progress.
18. L0 materialization advances by `TransformLogEntry.DataTimeTick`; effect
    timetick is recorded for L0 output range but is not sufficient as the
    materializer replay cursor.
19. L0 materialization output range is the min/max of materialized
    `TransformDeleteBlock.time_tick` values, independent from the
    `DataTimeTick` order used to read entries.
20. QN cache deduplication is based on exact Delete block identity, not on a
    highest-seen effect timetick watermark.

## 8. Implementation Stages

### Stage 1: Protocol And In-Memory Flow

- Add `SubscribeTransform` RPC and transform messages to `streaming.proto`.
- Implement SN stream handler with in-memory `TransformLogManager`.
- Implement QN TransformLog client and its vchannel client-side cache.
- Implement QN reconnect by recreating active vchannel subscriptions from local
  retained start.
- Implement demand-driven replay barrier refresh on the shared transform stream.
- Wire QN SegmentManager readiness through the TransformLog client replay and
  caught-up interface.

### Stage 2: Durable SN Transform Log

- Add durable `TransformLogStore` and `VChannelTransformLogMeta`.
- Recover `TransformLogStore` inside RecoveryStorage before wiring GrowingModule
  and subscription services.
- Append Delete transform blocks through RecoveryStorage/GrowingModule.
- Expose transform-log data barrier and compose it into the RecoveryStorage data
  barrier for Delete messages.
- Recover transform log metadata and chunks on SN startup.
- Route historical replay and live tail through `TransformLogStore`.

### Stage 3: L0 Materialization

- Implement `TransformLogMaterializer`.
- Write L0 delete segments when thresholds are reached.
- Commit materialized L0 segments to DataCoord.
- Advance `MaterializedTimeTick` and `MaterializedDataTimeTick`.
- Make materialization retry idempotent.

### Stage 4: Retention, Recovery, And Metrics

- Apply QueryView/DataView retention hints to SN transform log retention.
- Return `STREAMING_CODE_UNRECOVERABLE` when the requested start point is no
  longer retained.
- Garbage-collect transform log chunks covered by the retention watermark.
- Add metrics for replay unavailable, caught-up latency, retained cache range, and
  stream lag.

## 9. Open Follow-Ups

1. Define the exact SN catalog key layout for `VChannelTransformLogMeta`,
   transform log chunks, and L0 materialization records.
2. Define transform log chunk sizing and materialization thresholds.
3. Decide whether DataCoord should expose an L0 materialization query for SN
   validation or repair.
4. Define QN TransformLog client cache memory limits and eviction policy.
