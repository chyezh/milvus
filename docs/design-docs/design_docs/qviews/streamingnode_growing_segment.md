# StreamingNode Growing Segment Resource Design

> Query-resource view of SegmentModule-owned growing data.
> References: [StreamingNode View Resource Design](streamingnode_resource_manager.md),
> [StreamingNode VChannel WAL View Design](../wal/streamingnode_vchannel_wal_view.md),
> [Segment View Module](../wal/segment_view_module.md), and
> [StreamingNode BM25 and IDFOracle Design](streamingnode_bm25_idf_oracle.md).

## 1. Scope

SegmentModule owns WAL consumer state for current growing segments and retained
flushed segments. `viewresource` must use SegmentModule through the
`VChannelWALView` segment snapshot captured by RecoveryStorage; it must not read
SegmentModule internal maps or request a second segment set during load.

This document only describes the query-visible growing resource contract. The
WAL-side implementation, dirty snapshots, barriers, and catalog persistence are
defined in [Segment View Module](../wal/segment_view_module.md).

## 2. Interface Boundary

SegmentModule provides the Segment part of `VChannelWALView`:

```go
func (m *SegmentModule) VisibleSnapshot(
    vchannel string,
    baseGrowingTimeTick uint64,
) walview.VisibleSegmentSnapshot
```

The returned `walview.VisibleSegmentSnapshot` is the only segment input used by
`viewresource` load preparation. Its concrete DTO shape is defined in
[StreamingNode VChannel WAL View Design](../wal/streamingnode_vchannel_wal_view.md).

Snapshot construction must be lightweight and concurrency-safe:

- metadata may be cloned because it is small and protects against later
  mutation;
- in-memory insert buffers are represented by raw `message.ImmutableMessage`
  values;
- no decoded insert data is duplicated into a second buffer;
- consumers parse Insert and Txn messages through shared utilities and filter
  rows by the surrounding segment assignment.

## 3. Visibility Rule

For a snapshot DataVersion `D`:

```text
visibleGrowing(D) =
  current GROWING segments
  union retained FLUSHED segments whose sealed_at_data_version is unknown
  union retained FLUSHED segments whose sealed_at_data_version > D
```

If a flushed segment has no `sealed_at_data_version`, it remains visible
conservatively. StreamingNode does not yet know the first sealed DataVersion
that covers the segment.

This rule prevents duplicate query:

- `D < sealed_at_data_version`: StreamingNode still queries the segment as
  growing-side data.
- `D >= sealed_at_data_version`: QueryNode covers the sealed segment; SN growing
  runtime must not include it.

## 4. DataVersion Source

`sealed_at_data_version` is assigned by DataCoord when StreamingNode commits a
flush through `SaveBinlogPaths(Flushed=true)`. Retrying the same flushed segment
must return the same DataVersion.

The WAL `Flush` message only closes the local growing segment. It does not
carry the sealed DataVersion.

SegmentModule derives the snapshot DataVersion from its own retained state:

```text
SegmentSnapshotDataVersion(vchannel) =
  max(
    persisted SegmentDataVersionSummary.data_version,
    max(sealed_at_data_version of retained FLUSHED segments in vchannel),
  )
```

If neither source exists, the empty DataVersion is returned. Current growing
segments are still included by the visibility rule.

## 5. Retention and GC

Retained flushed segment state must survive SN crash recovery when a persisted
Up view is older than the latest SegmentModule state. Therefore SegmentModule
may delete flushed segment metadata only after:

```text
segment meta is durable
segment data is durable
sealed_at_data_version is known
requiredDataVersion(vchannel) >= sealed_at_data_version
persistedObservedDataVersion(vchannel) >= sealed_at_data_version
```

`requiredDataVersion(vchannel)` is the local resource watermark computed by
`viewresource` from Up QueryViews and temporary recovery anchors. `AlterLoadConfig`
load tasks and non-Up QueryView states do not advance this watermark.

Before GC deletes flushed segment metadata that would otherwise be the durable
source of the largest observed DataVersion, SegmentModule first persists:

```proto
message SegmentDataVersionSummary {
    view.DataVersion data_version = 1;
}
```

The summary is GC-driven. Flush commit should not synchronously update it.

## 6. Runtime Preparation

`viewresource` converts every visible segment in the WALView snapshot into a
csegment-backed growing runtime:

1. load persisted L1 storage if present;
2. replay snapshot `InsertMessages`;
3. apply historical Deletes from `VChannelWALView.DeleteReplay`;
4. then apply live Insert/Delete/Flush messages received through the returned
   `VChannelLiveObserver`.

Growing BM25 stats are derived from the same visible segment set. BM25Provider
must not compute its own segment membership independently from the growing
runtime.
