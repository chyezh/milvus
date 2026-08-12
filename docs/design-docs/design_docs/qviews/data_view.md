# DataView Design

> This document describes the DataView design for distributed query views.
> Reference: [Distributed Query View Design](README.md), [Balancer & CollectionLoadManager Design](balancer_design.md), [view.proto](../../../../pkg/proto/view.proto).

## 1. Purpose

DataView is the data-side snapshot that separates **what data exists** from
**who serves the data**.

```
DataView(DataVersion)
    "which segments are currently allowed to be loaded, organized by shard and partition"

QueryView(DataVersion, QueryVersion)
    "on top of a DataView, which nodes serve which segments"
```

DataCoord owns and persists DataView. QueryCoord consumes DataView as immutable
input for QueryView generation.

## 2. Concepts

### 2.1 DataView

DataView is the complete, non-duplicated loadable segment membership snapshot
of a collection.

It answers only:

```
At DataVersion D, which segments are allowed to be loaded in each vchannel and partition?
```

It does not contain QueryNode placement, resource groups, load state, index
state, segment content version, manifest version, or QueryView lifecycle state.

Once a segment is already in DataView, later changes to that segment's content
or load workflow are outside DataView. Those changes may still require QueryNode
reopen/reload through other mechanisms, but they do not change DataView
membership or DataVersion.

### 2.2 Loadable Segment

A segment joins a QueryCoord-visible DataView only when DataCoord considers it
allowed to be loaded by QueryNode.

The loadable predicate is:

```
state == Flushed
&& !is_invisible
&& !is_importing
&& level != L0
&& !dropped
```

Segments outside this predicate do not join a QueryCoord-visible DataView.
Examples:

- Growing, Sealed, Flushing, Importing, Dropped segments are not in DataView.
- Invisible flushed segments are not in DataView until they become visible.
- L0 delete segments are not in DataView membership.

After a segment has joined a persisted DataView, DataView membership is the
loadability authority for that version. Later segment-meta state changes such
as `Dropped` or `Compacted` do not by themselves make the segment unloadable for
retained DataViews that still reference it. Physical GC must wait until no
retained DataView or QueryView can reference the segment.

The flush plus sort-compaction path does not publish an intermediate DataView.
The flushed segment receives and retains its `sealed_at_data_version`, while
QueryCoord continues to consume the previous published head. Once sort
compaction produces the final loadable segment, DataCoord publishes the
inherited streaming epoch for the completed flush.

### 2.3 DataVersion

DataVersion is a collection-level composite version:

```
(streaming_version, compact_version)
```

Versions are ordered lexicographically.

`streaming_version` advances when new loadable segments join the view from the
streaming/write side. `compact_version` advances when the loadable membership
of the current streaming epoch is rewritten or trimmed.

Version rules:

| Membership Change | Version Transition | Meaning |
|---|---|---|
| Segment joins DataView from write/import/copy-segment-complete path | `(S, C) -> (S+1, 0)` | New loadable data joins the view. |
| Segment leaves DataView, or membership is replaced/trimmed | `(S, C) -> (S, C+1)` | Existing loadable membership changes without a new streaming epoch. |
| Segment content changes without membership change | unchanged | DataView does not track content mutation. |
| Delete frontier changes without membership change | unchanged | `transform_start_after_timetick` is derived metadata, not a version source. |
| DropCollection | delete DataView | The whole collection view is removed. |

`compact_version` resets to `0` whenever `streaming_version` advances.

The first published DataView version of a collection starts from `(1, 0)`.

DataView is advanced by explicit DataCoord business adapters, not by QueryCoord
or by a generic recalculation loop. Each adapter submits the final loadable
membership mutation after its business completion gates pass.

Flush, import, and copy-segment completion advance DataVersion only when they
introduce new loadable membership. Compaction, drop partition, and truncate
advance DataVersion only when they remove, replace, or trim loadable
membership.

### 2.4 QueryViewVersion

QueryViewVersion is:

```
(DataVersion, QueryVersion)
```

DataVersion changes when DataView changes. QueryVersion changes when the same
DataView is served by a different placement plan, for example balance, recovery,
or resource-group adjustment.

When DataVersion advances, QueryVersion starts from `1` for the new QueryView.
When only placement changes, DataVersion stays unchanged and QueryVersion
increments.

## 3. Data Structures

DataView uses the proto definitions in `view.proto` for persistence and
transport:

```proto
message DataViewOfCollection {
    int64 collection_id = 1;
    repeated DataViewOfShard shards = 2;
    DataVersion data_version = 3;
}

message DataViewOfShard {
    string vchannel = 1;
    repeated DataViewOfPartition partitions = 2;
    uint64 transform_start_after_timetick = 3;
}

message DataViewOfPartition {
    int64 partition_id = 1;
    repeated int64 segment_ids = 2;
}
```

The durable DataView record stores membership and DataVersion only. Segment
metadata such as row count, memory size, binlogs, manifest path, schema version,
storage version, segment-level data version, and segment-level
`delete_apply_start_after_timetick` remain in DataCoord segment metadata.
QueryCoord may fetch that metadata separately for balancer scoring, but it must
not recompute DataView membership from it.

`DataViewOfShard.transform_start_after_timetick` is a snapshot/transport
field derived from the current loadable membership. It does not need to be
stored durably with DataView; DataCoord may recompute it from segment metadata
when publishing snapshots or syncing StreamingNode. If the serialized proto
contains this field in a persisted DataView value, DataCoord still treats it as
derived cache and recomputes it from segment metadata on load/recovery. The
source value is a separate persisted field on each segment metadata record, not
`segment.dml_position.Timestamp`.

## 4. Ownership

### 4.1 DataCoord

DataCoord is the source of truth for DataView.

Responsibilities:

1. Recover DataView from the metastore on startup.
2. Keep one in-memory published DataView per collection.
3. Persist every loadable-membership mutation.
4. Advance DataVersion according to the rules above.
5. Expose DataView snapshots to QueryCoord.
6. Derive and sync `transform_start_after_timetick` to StreamingNode when it changes.

### 4.2 QueryCoord

QueryCoord consumes DataView.

Responsibilities:

1. Maintain a `DataViewProvider` cache for Balancer.
2. Publish immutable `DataViewSnapshot` objects.
3. Trigger Balancer when a collection's DataVersion advances.
4. Build QueryViews from `DataView membership + assignments`.
5. Refresh DataView-derived metadata such as delete timetick without changing
   QueryViewVersion.

QueryCoord must not modify DataView or assign DataVersion.

### 4.3 DataViewManager State Model

DataCoord keeps one collection-scoped published state. It is the in-memory copy
of the snapshot identified by the durable `published_data_version`.

```go
type collectionDataViewState struct {
    collectionID int64
    published    *viewpb.DataViewOfCollection
}
```

Definitions:

- **published DataView**: the immutable snapshot identified by the collection's
  durable `published_data_version`.
- **orphan snapshot**: a snapshot key that is not reachable from the durable
  published head, for example because its head update did not complete.

The published head is the sole QueryCoord membership authority. A flush whose
output is not yet loadable receives and durably retains its
`sealed_at_data_version`, but does not create or expose another DataView state.
The later loadable completion publishes the inherited streaming epoch.

## 5. Persistence

The first implementation stores one full collection snapshot per DataVersion.
It does not shard the persisted DataView payload.

Suggested keys:

```
datacoord-meta/dataview/{collectionID}/versions/{streamingVersion}/{compactVersion}
```

`versions/{S}/{C}` stores a serialized `DataViewOfCollection` membership
snapshot. The durable record may omit
`transform_start_after_timetick`; that value can be recomputed when
DataCoord publishes a snapshot or syncs StreamingNode.

Persistence and visibility semantics:

- A DataView version is durably complete if and only if its full snapshot key
  exists.
- `published_data_version` selects the sole snapshot that new QueryCoord work
  may consume. The maximum snapshot key is not an authority.
- A delayed flush persists only its segment-level assigned version until final
  loadable membership can be published; it does not persist an unavailable
  DataView snapshot.
- QueryCoord and DataViewProvider must obtain DataView from DataCoord or the
  DataViewProvider cache; they must not derive membership from segment metadata.
- Snapshot persistence and published-head advancement are atomic. A snapshot
  not selected by `published_data_version` is an orphan and is never exposed as
  the latest published DataView.

### 5.1 SegmentMeta-First Publication

DataView and segment metadata are managed by separate components in the first
implementation. DataCoord updates segment metadata first, then persists and
publishes DataView.

Normal flow:

```
1. Apply and persist segment metadata mutation.
2. After business completion gates pass, build the next full snapshot from the
   previous published DataView plus an explicit membership mutation.
3. Atomically persist `versions/{D'}` and advance `published_data_version`.
4. Notify QueryCoord after publication succeeds.
```

This is not a strict atomic transaction between segment metadata and DataView.
It is a segmentMeta-first, DataView-lagging publication model. The important
ordering guarantee is:

```
DataView may lag segment metadata.
DataView must not publish membership that segment metadata cannot describe.
```

If DataView persistence fails after segment metadata has already advanced,
the published head is unchanged and QueryCoord is not notified. The business
adapter reports the failure and explicitly retries the same assigned epoch or
membership rewrite. Recovery does not infer a replacement mutation or version
from SegmentMeta.

DropPartition and truncate also follow SegmentMeta-first ordering, but their
metadata mutation has a narrower meaning: it is a durable, collection-scoped
logical-removal fence. DataCoord validates assigned-epoch ordering, persists
the matching SegmentMeta records as `Dropped`, then publishes the DataView
trim. QueryCoord-visible membership remains authoritative until that trim is
durable; an older retained DataView may therefore continue to reference and
load a fenced segment while publication is retried. DataView and QueryView
references continue to protect the segment's physical lifetime.

### 5.2 Recovery Authority

Recovery loads the collection version-state record and then loads exactly the
snapshot identified by `published_data_version`. It does not compare the
snapshot with SegmentMeta, reconstruct expected membership, classify a diff,
or generate a new DataVersion.

SegmentMeta may legitimately be ahead of the published head. In particular,
`sealed_at_data_version` can identify assigned but unfinished Streaming epochs,
and DDL removal fences can be durable before their DataView trim. The
corresponding business adapter re-drives publication with an explicit mutation.
Recovery preserves both sides without inventing the missing business decision.

A readable snapshot newer than the published head is an orphan. It is not
adopted by recovery and GC removes it once no exact live reference protects it.

## 6. Mutation Semantics

DataViewManager is the DataCoord component that applies explicit DataCoord
membership commits to the persisted loadable membership. Business adapters use
`CommitPublishedView`, `CommitStreamingView`, `CommitRewrite`, and
`CommitSegmentTrim` with explicit mutations.

Current interface shape:

```go
func RecoverManager(ctx context.Context, catalog RecoveryCatalog, segments SegmentStore) (Manager, error)

type DataViewManager interface {
    AssignFlushVersion(ctx context.Context, collectionID, segmentID int64) (*viewpb.DataVersion, error)
    CommitPublishedView(ctx context.Context, collectionID int64, assigned *viewpb.DataVersion, mutation PublishedMutation) (*viewpb.DataVersion, error)
    RetryAssignedFlushPublication(ctx context.Context, collectionID, segmentID int64, assigned *viewpb.DataVersion, removeOnly bool) (*viewpb.DataVersion, error)
    CommitStreamingView(ctx context.Context, collectionID int64, mutation PublishedMutation) (*viewpb.DataVersion, error)
    CommitRewrite(ctx context.Context, collectionID int64, mutation PublishedMutation) (*viewpb.DataVersion, error)
    CommitSegmentTrim(ctx context.Context, collectionID int64, resolve SegmentTrimTargetResolver, finalize SegmentTrimFinalize) (*viewpb.DataVersion, error)
    InitializeCollection(ctx context.Context, initialization CollectionInitialization) (*viewpb.DataVersion, error)
    MarkCollectionTerminal(ctx context.Context, collectionID int64) error

    Get(ctx context.Context, collectionID int64, version qviews.DataVersion) (DataViewRef, error)
    LatestPublished(ctx context.Context, collectionID int64) (DataViewRef, error)
    DataViewSnapshotRefForCollections(ctx context.Context, collectionIDs map[int64]struct{}) (dataview.SnapshotRef, error)
    SegmentSnapshot(ctx context.Context, segmentIDs []int64) dataview.SegmentSnapshot
    ShardTimeTicks(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewShardTimeTick, error)
    IsSegmentReferenced(ctx context.Context, collectionID, segmentID int64) (bool, error)
    GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error
}
```

Collection-drop finalization is owned by the lifecycle wrapper around the core
manager. It durably marks the collection terminal, calls
`MarkCollectionTerminal`, waits for durable QueryView cleanup and live refs to
drain, then removes the collection snapshots and version state.

`RecoverManager` restores the durable published head directly from metastore.
There is no generic post-recovery SegmentMeta repair phase. If an explicit
business publication failed, that business workflow retries its `Commit*`
operation after recovery.

Internal helpers keep publication state-oriented and explicit:

```go
buildNextView(base, mutation) -> next
deriveDeleteTimetick(view, segmentMeta) -> per-shard timetick
persistSnapshotAndPublishedHead(view) error
```

Business adapters carry the affected collection, vchannel, partition, and
segment IDs from the completed DataCoord mutation. DataViewManager must not
discover the next view by scanning all segment metadata on the normal path or
during recovery.

Business adapters own completion and loadability gates before calling the
manager. They submit the final affected IDs as `SegmentMembership` values and
explicit removals. The manager validates that every added membership is
loadable and belongs to the target collection, but it never scans SegmentMeta
to discover membership, classify the transition, or wait for a later business
event. Metadata-only changes do not call a membership commit. Delayed outputs
remain outside DataView until their owning adapter submits the final loadable
membership at the already-assigned epoch.

A single DataCoord mutation batch advances DataVersion at most once.

Commit adapters must be idempotent. Repeated requests and mutations whose
membership is already reflected in the published head are normal no-op success
cases after durable verification.

### 6.1 Flush Publication Flow

`SaveBinlogPaths` is the Streaming flush adapter. It first invokes
`AssignFlushVersion`, then persists the SegmentMeta changes. For an immediately
loadable output it invokes `CommitPublishedView` with the exact assigned epoch
and explicit membership before returning success. A retry whose segment has
already become Dropped uses `RetryAssignedFlushPublication` to verify a durable
prior publication or explicitly complete an empty/remove-only epoch; the
manager does not reconstruct the original mutation from current SegmentMeta.

StreamingNode flush can first produce a temporary sealed segment that still
needs sort processing. Such a segment receives its deterministic
`sealed_at_data_version`, but never enters a published DataView.

If the flushed segment is immediately usable, it joins DataView and advances:

```
(S, C) -> (S+1, 0)
```

If the flush output must pass through sort compaction first, QueryCoord
publication waits. The final sorted segment inherits the original assigned
`(S+1, 0)` and is published at that exact streaming epoch. The handoff does not
expose an intermediate DataView snapshot to QueryCoord.

Dropped flush output, partial checkpoint updates, and binlog-only updates do
not change DataView.

### 6.2 Import Publication

The import adapter runs after import completion.

Import preallocation and import progress do not affect DataView. Imported
segments join only after the import path has completed all gates required for
QueryNode loading. In the current flow this can be delayed until compaction and
index build finish, and 2PC import must also clear `is_importing`.

When imported segments become loadable, they join DataView and advance:

```
(S, C) -> (S+1, 0)
```

L0 import output follows the L0 rules and does not join membership.

### 6.3 Compaction Rewrite

The compaction adapter runs after compaction completion.

For non-L0 compaction, DataView changes only when loadable membership changes:

- loadable input segments leave the view;
- loadable output segments join the view;
- invisible output segments wait until the required visibility gate, such as
  stats/index build, completes.

When the compaction rewrite becomes visible to QueryNode loading, DataVersion
advances:

```
(S, C) -> (S, C+1)
```

Schema-bump compaction that updates the same segment's manifest/schema/storage
metadata without changing membership does not advance DataView. If a schema-bump
task performs a full replacement with a different loadable segment ID, it is a
compact rewrite and advances `compact_version`.

### 6.4 L0 Compaction Projection

The L0 compaction adapter runs after Level-0 delete compaction.

L0 segments are delete-log carriers, not loadable sealed segments. L0 compaction
can update manifests, append deltalogs to target segments, and drop L0 input
segments, but it does not add or remove `DataViewOfPartition.segment_ids`.

Therefore the L0 adapter only refreshes the derived
`delete_apply_start_after_timetick`. It does not advance DataVersion unless the
same mutation also changes non-L0 loadable membership.

### 6.5 DDL / Trim Commits

Some membership changes are not caused by segment production:

- DropPartition removes loadable segments in the dropped partition.
- Truncate removes loadable segments before the truncate fence.
- DropCollection first makes the collection terminal, then deletes DataView
  snapshots only after durable QueryView cleanup and live-reference drainage.

DropPartition and truncate advance `compact_version` if they remove loadable
membership:

```
(S, C) -> (S, C+1)
```

DropCollection does not advance DataVersion because the collection view no
longer exists. The durable terminal marker rejects new exact-version and
Balancer snapshot references while existing refs remain readable.

DropPartition and truncate first persist a collection-scoped logical-removal
fence by marking every matching SegmentMeta record `Dropped`, then publish the
DataView trim while holding the same collection serialization lock. The target
scope and assigned epochs are resolved again after metadata finalization so
concurrent replacement outputs cannot escape the trim.

The durable DataView head remains the sole QueryCoord membership authority.
Physical segment GC consults manager-owned retained-membership accounting. The
published head, configured retention window, and every live DataViewRef are
protected; callers cannot provide an independent protected-version list.
Balancer planning acquires refs for the exact snapshots it reads and releases
them after planning and application. On process recovery, QueryView state
machines reacquire their exact refs or enter cleanup before DataCoord starts GC.
Until the trim publication succeeds, the prior head may still contain a fenced
segment and existing QueryViews may continue to use it. The `Dropped` state
prevents that segment from joining future repaired DataViews; it does not revoke
membership from an already-published or retained DataView. A publication
failure is retryable, and retry completes the assigned remove-only epoch or
compact removal from the same authoritative head.

There is no separate DropChannel commit in the current DataCoord behavior.
Channel-level effects should be represented by the actual DDL/trim operation
that changes membership, or by collection drop.

### 6.6 Delayed Visibility

Some flows write segment metadata before the segment is allowed to be loaded.

Examples:

- Sort-enabled flush may mark a flushed segment invisible while stats/index
  preparation continues.
- Clustering compaction writes invisible output segments first, then marks them
  visible only after the required preparation succeeds.

Invisible segments are not in DataView. DataVersion advances only when the
segment crosses into or out of the loadable predicate.

If an invisible output segment is later dropped without becoming loadable,
DataView does not change.

### 6.7 Non-Membership Updates

Changes to an already-loadable segment do not change DataView when membership is
unchanged.

Examples:

- `BatchUpdateManifest` updates a manifest pointer or column groups.
- Segment-level `data_version` changes for QueryNode reopen.
- Binlog, manifest, schema, storage-version, or stats metadata changes.
- Index metadata changes.
- L0 deltalogs are appended to an existing loadable segment's manifest.

These changes may affect QueryNode load/reopen behavior through other metadata
paths, but DataView does not advance DataVersion for them.

After a segment index result is durably committed, DataCoord refreshes the
already-loaded QueryNode segment through the segment-load-info watch path. This
wakeup is independent from DataView: it reloads the index held by an existing
QueryView and does not create a replacement QueryView.

### 6.8 L0 Delete Segments

L0 segments are delete-log carriers, not loadable sealed segments.

Rules:

- L0 create does not add a segment to `DataViewOfPartition.segment_ids`.
- L0 compaction/drop does not advance DataVersion unless loadable membership
  also changes.
- L0 state can affect the derived `delete_apply_start_after_timetick`.

### 6.9 Copy Segment Completion

The copy-segment adapter runs after a copy segment task has persisted the target
segment result into DataCoord metadata.

Snapshot restore is the upstream business flow that creates copy segment jobs,
but the restore request itself does not make data loadable. A copied target
segment joins DataView only after its binlog/manifest/index metadata has been
written and it satisfies the loadable predicate, for example:

```
state == Flushed
&& !is_importing
&& !is_invisible
&& level != L0
```

When target segments become loadable, the join advances:

```
(S, C) -> (S+1, 0)
```

Partial copy progress, restore job creation, copied index metadata before the
target segment is loadable, and task state updates that do not change loadable
membership do not change DataView.

### 6.10 External Collection Refresh Commit

The external-refresh adapter runs after an external collection refresh applies
its segment patch.

External refresh can add new loadable segments, patch existing segment content,
and remove stale loadable segments. DataView handles only membership changes:

- pure addition of new loadable segments advances `(S, C) -> (S+1, 0)`;
- removal or replacement of existing loadable membership advances
  `(S, C) -> (S, C+1)`;
- patching an existing segment without changing membership does not advance
  DataVersion.

If one refresh batch both adds and removes membership, it is a rewrite of the
external source snapshot and should advance `compact_version` once.

### 6.11 DropCollection

DropCollection does not advance DataVersion because the collection view no
longer exists. The lifecycle owner first persists a terminal marker and invokes
`MarkCollectionTerminal`, which rejects new exact-version and Balancer snapshot
refs while preserving existing refs. QueryCoord releases QueryViews through the
load-config release path. Only after durable QueryView cleanup and ref drainage
does lifecycle-owned finalization remove the collection snapshots and version
state and clear the terminal marker.

## 7. QueryCoord Consumption

QueryCoord implements Balancer's `DataViewProvider` by reading DataCoord's
published DataView snapshot.

```
DataCoord DataViewManager
        |
        | ref-owned in-process snapshot / watch / refresh
        v
QueryCoord DataViewProvider
        |
        v
BalancerSnapshot.DataViewSnapshot
        |
        v
BalancePolicy.Plan(...)
        |
        v
QueryViewAtCoordBuilder(DataView, assignments)
```

The immutable snapshot value, reference, and segment lookup types are owned by
`internal/dataview`; the Balancer is only a consumer and must not define the
DataView manager's output model. The Balancer acquires
`DataViewSnapshotRefForCollections` and treats its DataViews as immutable during
one reconcile cycle. It holds the snapshot ref through planning and
application, then releases it. If DataVersion advances while a plan is being
built, that new DataView is consumed by the next reconcile cycle.

QueryCoord must acquire a manager-owned reference, for example through
`LatestPublished(collectionID)` or `Get(collectionID, version)`. It must not
select the maximum persisted DataVersion by itself; only the durable published
head is authoritative.

Phase 1 uses DataVersion comparison:

```
current Up QueryView DataVersion < latest DataView DataVersion -> Must prepare new QueryView
```

Phase 2 uses the DataView shard membership as the segment set to allocate.

QueryCoord must communicate with segment metadata through DataView semantics.
It must not derive loadable membership from raw segment metadata and must not
repair DataView holes locally.

LoadInfo is outside DataView management. QueryNode obtains load metadata through
its own additional RPC path. DataView only decides which segment IDs belong to a
view; it does not own binlog/manifest/index lookup or QueryNode load metadata
delivery.

## 8. Delete Data Eviction

`DataViewOfShard.transform_start_after_timetick` is DataView-derived
metadata because it depends on the data that must remain queryable, not on query
placement.

It is derived from the segment IDs contained in the current DataView.
Conceptually, for one shard:

```
transform_start_after_timetick =
    min(segment.delete_apply_start_after_timetick for every segment in the current DataView shard)
```

The calculation does not re-check whether those segments are currently
loadable, and it does not include historical retained DataViews. It only uses
the segment IDs already present in the current DataView membership.

`segment.dml_position.Timestamp` must not be used as this source. Its existing
meaning is overloaded:

- normal flush updates it as the segment checkpoint / end position;
- import sets it from imported row timestamp range;
- compaction recalculates it from output binlog timestamp range or input
  fallback positions;
- GC and truncate already apply their own effective timestamp rules.

DataCoord therefore owns a separate persisted segment metadata field:

```proto
message SegmentInfo {
    // Exclusive lower bound for delete data that must be retained/applied when
    // this segment is loaded. DataView derives shard-level
    // delete_apply_start_after_timetick from this field.
    uint64 delete_apply_start_after_timetick = 37;
}
```

New segment-producing paths must populate this field explicitly:

- **Flush / StreamingNode flush**: use the segment start position or create
  segment timetick. For StreamingNode-managed L1 segments this is the segment
  assignment's create-segment timetick, not the data checkpoint timetick.
- **Import**: import segments join DataView only after commit. Use the import
  commit timestamp, matching the QueryNode rule that an import segment becomes
  visible at its commit fence.
- **Copy / snapshot restore**: copy the source segment's
  `delete_apply_start_after_timetick`.
- **Non-L0 compaction**: inherit the minimum
  `delete_apply_start_after_timetick` from all input segments. Compaction
  replaces membership and must not shorten the delete retention window by using
  output binlog timestamp ranges.
- **Sort compaction**: inherit the input segment's
  `delete_apply_start_after_timetick`, even if row timestamps or positions are
  rewritten in the output segment.
- **L0 compaction**: L0 segments do not join DataView membership. L0 compaction
  can refresh the derived shard timetick, but it does not write membership for
  a loadable segment and does not advance DataVersion by itself.

For existing segment metadata that predates this field, DataCoord derives a
compatible value when building or recovering DataView:

```go
func segmentDeleteApplyStartAfterTimetick(segment) uint64 {
    if segment.delete_apply_start_after_timetick != 0 {
        return segment.delete_apply_start_after_timetick
    }
    if segment.commit_timestamp != 0 {
        return segment.commit_timestamp
    }
    if segment.start_position != nil {
        return segment.start_position.Timestamp
    }
    return 0
}
```

This fallback is intentionally conservative. Old normal flushed segments fall
back to their start position, old committed import segments fall back to their
commit timestamp, and very old segments without start position fall back to
`0`, which may retain more delete data but will not evict required deletes.

Snapshot/restore metadata must persist the new segment field. If an older
snapshot manifest does not contain it, restored segments use the same fallback
rules above. The DataView record itself still does not need to persist
`delete_apply_start_after_timetick`; it is derived from segment metadata when
DataView is returned or synced.

L0 delete state can affect this derived frontier, but L0 segments still do not
join DataView membership.

StreamingNode delete eviction is safe without depending on the latest DataView
alone because StreamingNode applies its own minimum across the DataView
timeticks it has received and still needs to retain.

Delete frontier changes do not advance DataVersion. They also do not require a
new QueryView because segment membership is unchanged.

DataCoord syncs the lightweight timetick through `SyncDataView`:

```proto
message DataViewShardTimeTick {
    string vchannel = 1;
    uint64 transform_start_after_timetick = 2;
}
```

This path is valid for loaded and unloaded collections. QueryView may carry the
same field as part of a normal membership update, but timetick-only changes are
metadata refreshes and must not drive QueryView state-machine transitions.

## 9. Snapshot Semantics

DataView is a loadable segment membership snapshot at a DataVersion.

When a QueryView is built from DataVersion `D`, workers execute that QueryView
against exactly the loadable membership in `DataView(D)`. Later membership
changes produce a newer DataVersion and do not change existing QueryViews in
place.

Segment content changes after a segment has joined DataView do not change
`DataView(D)`. They are handled by segment metadata, QueryNode reopen/reload, or
other content-version mechanisms.

Delete frontier changes after a segment has joined DataView also do not change
`DataView(D)` or QueryViewVersion. They are propagated as DataView-derived
metadata refreshes.

This provides:

1. Stable query inputs during two-phase query execution.
2. Clear generation of replacement QueryViews when loadable membership changes.
3. Independent QueryVersion evolution for placement-only changes.

## 10. GC And Retention

Segment state and physical GC are separated from DataView membership.
Compaction, DropPartition, and truncate may mark segment metadata as
`Dropped`/`Compacted` before old DataViews are gone. That state means the
segment should not join future DataViews; it does not mean the segment can no
longer be loaded by an already-retained DataView.

For DropPartition and truncate, this separation also covers the interval after
the logical-removal fence is durable but before DataView trim publication
succeeds. The old DataView remains valid and keeps the segment live; retrying
publication must not allow physical cleanup to race ahead of retained DataView
or QueryView references.

Physical cleanup of segment files, binlogs, manifests, and indexes is allowed
only after all of the following are true:

1. No retained DataView references the segment.
2. No QueryCoord-side QueryView can still reference the segment.
3. Existing DataCoord GC safety checks for object storage and metadata cleanup
   also pass.

Therefore GC must use DataView retention as one of its inputs. DataView
versions can be removed only after QueryCoord no longer has QueryViews that
reference them.

Because DataCoord and QueryCoord run in the same coordinator process for this
design, DataView GC does not need a separate cross-service retention protocol in
the first implementation:

- The durable published head is always retained.
- The configured retention window is counted backward from the published head,
  not from the maximum snapshot key.
- Snapshots newer than the published head are unpublished orphans and are
  collected unless an exact live ref protects them.
- If a collection is loaded, any DataView referenced by a live QueryView cannot
  be GCed.
- Segment physical GC runs after DataView GC. A segment can be deleted only
  after no retained DataView and no QueryCoord-side QueryView can still
  reference it.

Published DataView history is linear. GC protects the durable head, retained
history, and every manager-owned exact-version ref. The resulting retained
membership index protects physical segments independently of current
SegmentMeta lifecycle state.

## 11. Recovery

On DataCoord startup:

1. Load each collection's durable version-state record.
2. Load exactly the DataView snapshot identified by
   `published_data_version`.
3. Reject recovery if that authoritative snapshot is missing or malformed.
4. Restore the in-memory published state from that snapshot.
5. Restore the allocated Streaming watermark and exact assigned epochs from
   durable version state and `SegmentMeta.sealed_at_data_version`.
6. Rebuild retained-membership accounting from retained snapshots at or before
   the published head.
7. Ignore newer orphan snapshots for publication and let GC remove them.

Recovery never creates a DataView snapshot or advances DataVersion. SegmentMeta
may be ahead because an assigned flush, compaction, or DDL workflow has not yet
completed publication. The owning business adapter explicitly retries the
corresponding `CommitPublishedView`, `CommitRewrite`, or `CommitSegmentTrim`
operation. Recovery does not infer membership, operation type, or version from
that difference.

The only compatibility exception is migration of legacy collections that have
snapshot records but no `published_data_version` field. That one-time migration
may choose the newest already-persisted snapshot whose newly added membership
is loadable, backfill the durable head to that existing snapshot, and never
creates a reconstructed snapshot. Once the head exists, all later recovery is
head-only.

Because each DataView version is persisted as a single full snapshot value,
DataCoord treats every readable version key as a complete persisted snapshot.
Completeness does not imply publication: only `published_data_version` makes a
snapshot authoritative. There is no temporary unavailable DataView state.

DataView's shard-level `delete_apply_start_after_timetick` is recomputed during
recovery from segment metadata. The per-segment source field is durable segment
metadata; if it is absent on old segment records or old snapshot manifests,
DataCoord uses the compatibility fallback from Section 8.

On QueryCoord startup:

1. Recover QueryView state from its own catalog.
2. Refresh DataView snapshots from DataCoord.
3. Trigger a full Balancer reconcile.

If recovered QueryViews are older than DataView, Balancer creates replacement
QueryViews. If load config is absent, Balancer releases residual QueryViews.

## 12. Test Matrix

The first implementation should cover at least:

1. SegmentMeta-first mutation succeeds but DataView publication fails; restart
   restores the old published head and the business retry publishes the exact
   explicit mutation.
2. Delayed flush persists `sealed_at_data_version` without creating a DataView;
   the final sort output inherits and publishes that exact epoch.
3. A later ready Streaming epoch cannot overtake an earlier assigned but
   unfinished epoch, including across restart.
4. Durable head S1 plus newer orphan S2 recovers S1; GC keeps S1, removes S2,
   and the next restart still succeeds.
5. Recovery with SegmentMeta ahead of the durable head does not add membership,
   classify a diff, or allocate a new version.
6. DropPartition or truncate persists the scoped SegmentMeta removal fence,
   crashes before DataView trim publication, and explicit retry completes the
   trim while retained older DataViews keep physical data live.
7. L0 compact refreshes `delete_apply_start_after_timetick` without advancing
   DataVersion.
8. Delete-timetick projection refreshes metadata without creating a new
   DataVersion.
9. Duplicate or stale explicit commits are idempotent after durable
   verification.

## 13. Invariants

1. DataView contains no duplicate segment IDs.
2. QueryCoord-visible DataView membership contains only segments that are
   allowed to participate in load for that DataVersion.
3. DataVersion never rolls back for an existing collection DataView.
4. A DataVersion observed by QueryCoord corresponds to a complete persisted
   loadable segment membership.
5. QueryCoord never mutates DataView or assigns DataVersion.
6. QueryView generation uses DataView as immutable input.
7. DataView membership changes are represented by DataVersion changes.
8. Placement changes are represented by QueryVersion changes.
9. Delete frontier changes are represented by DataView-derived metadata refresh,
   not by DataVersion or QueryVersion changes.
10. Segment content changes after joining DataView do not change DataView
    membership or DataVersion.
11. QueryCoord derives loadable membership only from DataView, not from raw
    segment metadata.
12. Physical segment GC must wait until no retained DataView or QueryCoord-side
    QueryView can reference the segment.
13. DataView may lag segment metadata, but DataView must not publish membership
    that segment metadata cannot describe.
14. QueryCoord only consumes the snapshot selected by the durable published
    head.
15. DropPartition and truncate persist a collection-scoped SegmentMeta removal
    fence before DataView trim publication; recovery does not infer a trim from
    the fence, while retained DataView and QueryView references remain the
    authority for membership and physical lifetime until explicit retry
    succeeds.
16. Recovery never constructs membership or assigns a DataVersion from a
    SegmentMeta diff.
17. GC always retains the durable published head and does not count newer
    orphan snapshots in its retention window.

## 14. Open Implementation Choices

1. **Notification path**: QueryCoord can initially refresh DataView through
   polling or explicit trigger, then later switch to watch/event delivery.
2. **Delete frontier notification**: implementation should decide whether
   timetick-only refreshes are pushed directly by DataCoord to StreamingNode or
   routed through a QueryCoord cache before `SyncDataView`.
