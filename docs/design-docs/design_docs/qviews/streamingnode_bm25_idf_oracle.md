# StreamingNode BM25 and IDFOracle Design

> DataVersion-level BM25 resource preparation for StreamingNode viewresource.
> References: [StreamingNode View Resource Design](streamingnode_resource_manager.md),
> [StreamingNode Growing Segment Resource Design](streamingnode_growing_segment.md), and
> [StreamingNode VChannel WAL View Design](../wal/streamingnode_vchannel_wal_view.md).

## 1. Scope

This document defines how StreamingNode prepares BM25 resources for one
DataVersion. Query execution and query plan format are out of scope.

BM25 preparation is keyed by `DataVersion`, not by `QueryViewVersion`.
Therefore WAL-observed `AlterLoadConfig` can prepare BM25 resources before any
QueryView is synced to StreamingNode.

## 2. Inputs

For load-config initialization, BM25Provider consumes the same
`VChannelWALView` as the growing runtime:

| Input | Source | Purpose |
|---|---|---|
| latest schema | `view.Schema` | Discover BM25 functions and fields. |
| load settings | `view.LoadConfig.header` | Determine loaded fields and partitions. |
| DataVersion | `view.SegmentSnapshot.DataVersion` | Target BM25 resource version. |
| growing stats | `view.SegmentSnapshot` | BM25 stats for visible growing-side segments. |
| sealed stats | QueryCoord RPC | BM25 resources for sealed segments in DataView(D). |

BM25Provider must use the WALView segment snapshot for growing membership. It
must not ask SegmentModule for a separate visible segment set during the same
load, because that can diverge from the csegment runtime built by the registry.

## 3. QueryCoord RPC

StreamingNode asks QueryCoord for the complete sealed BM25 resource set required
by a DataVersion:

```proto
service QueryCoord {
    rpc GetStreamingNodeQueryViewResources(GetStreamingNodeQueryViewResourcesRequest)
        returns (GetStreamingNodeQueryViewResourcesResponse) {}
}

message GetStreamingNodeQueryViewResourcesRequest {
    common.MsgBase base = 1;

    int64 collection_id = 2;
    string vchannel = 3;
    view.DataVersion data_version = 4;
    view.QueryViewSettings settings = 5;
}

message GetStreamingNodeQueryViewResourcesResponse {
    common.Status status = 1;

    int64 collection_id = 2;
    string vchannel = 3;
    view.DataVersion data_version = 4;

    repeated StreamingNodeBM25Resource bm25_resources = 5;
}

message StreamingNodeBM25Resource {
    int64 segment_id = 1;
    int64 partition_id = 2;

    repeated data.FieldBinlog bm25_binlogs = 3;

    int64 storage_version = 4;
    string manifest_path = 5;
}
```

The response is the full sealed BM25 set for the requested DataVersion, not a
diff from the StreamingNode local cache.

## 4. Provider Placement

The concrete provider lives under `viewresource`:

```text
internal/streamingnode/server/viewresource/
  registry.go
  manager.go

internal/streamingnode/server/viewresource/idf/
  provider.go
  oracle.go
  segment_cache.go
```

`viewresource/idf` must not import `snview`. It depends on DataVersion, BM25
segment resources, growing BM25 stats from WALView segment snapshots, object
storage, and QueryCoord RPC.

## 5. Runtime Model

StreamingNode keeps two BM25 layers:

```text
sealed segment cache:
  segmentID -> local BM25 files and metadata

DataVersion oracle:
  DataVersion -> BM25ViewOracle
    active sealed segments
    active growing segments
    aggregate stats
```

Multiple DataVersion oracles may coexist during QueryView handoff and SN
recovery. They are evicted by the same local required DataVersion watermark as
other view resources.

The oracle is DataVersion-owned, but growing stats inside a not-yet-flushed
segment can still change as WAL live messages arrive. The provider updates every
retained oracle whose active growing set contains that segment. This avoids
building full replacement oracles on every live insert.

Sealed membership changes only when a new DataVersion is loaded. For that case,
the provider may reuse a nearby ready oracle as an optimization, but correctness
does not depend on diff loading.

## 6. Loading Flow

When `ViewResourceRegistry.OnAlterLoadConfig(view)` starts a WALView load task,
BM25Provider prepares:

```text
BM25Runtime(DataVersion = view.SegmentSnapshot.DataVersion)
```

The steps are:

1. derive BM25-enabled loaded fields from schema and settings;
2. return an empty ready BM25 runtime if no loaded field needs BM25;
3. call `GetStreamingNodeQueryViewResources` with the WALView DataVersion;
4. download or reuse sealed BM25 local files;
5. read growing BM25 stats from the WALView segment snapshot;
6. merge sealed and growing stats into the DataVersion oracle;
7. publish readiness through the vchannel registry.

QueryView readiness checks do not start BM25 loading for arbitrary requested
DataVersions. They only wait for a WAL-triggered forward load or report the
view as unrecoverable when the requested DataVersion is no longer locally
available.

## 7. Flush Interaction

For a growing segment flushed at `sealed_at_data_version = D1`:

- DataVersions `< D1` continue to use retained growing BM25 stats;
- DataVersions `>= D1` use sealed BM25 resources returned by QueryCoord;
- retained growing BM25 stats can be released only after the local required
  DataVersion watermark reaches `D1` and SegmentModule has persisted enough
  `SegmentDataVersionSummary` information to recover the observed DataVersion.

`sealed_at_data_version` is assigned by DataCoord through
`SaveBinlogPaths(Flushed=true)`. It is not derived from the WAL `Flush` message.

## 8. Concurrency

- Loading for the same vchannel/DataVersion should be singleflighted by the
  registry before it reaches BM25Provider.
- Published oracles are protected by provider locks.
- Growing updates are applied in WAL live order by the runtime that owns the
  returned observer.
- The live observer must not drop messages. Backpressure is handled by the
  bounded blocking observer buffer described in the WALView design.
- Local sealed BM25 files are retained while any retained oracle references
  them.

## 9. Test Plan

- `AlterLoadConfig` prepares BM25 by WALView Segment snapshot DataVersion.
- BM25Provider calls QueryCoord with DataVersion, not QueryViewVersion.
- QueryCoord response is treated as the full sealed BM25 set.
- Growing BM25 membership matches the WALView segment snapshot used by csegment
  construction.
- QueryView readiness does not trigger arbitrary BM25 loads.
- Old DataVersion oracles and sealed files are evicted only after the local
  required DataVersion watermark advances.
