# StreamingNode View Resource Design

> StreamingNode-side viewresource lifecycle for loaded collection shards.
> References: [Distributed Query View Design](README.md),
> [QueryViewHandler Design](query_view_handler.md),
> [StreamingNode Growing Segment Resource Design](streamingnode_growing_segment.md),
> [StreamingNode BM25 and IDFOracle Design](streamingnode_bm25_idf_oracle.md),
> [StreamingNode VChannel WAL View Design](../wal/streamingnode_vchannel_wal_view.md), and
> [WAL Recovery Architecture](../wal/wal-recovery-architecture.md).

## 1. Scope

This document defines how StreamingNode prepares and retains queryable local
resources for QueryViews. Query execution, query plan format, retry policy, and
Coordinator failure policy are out of scope.

The resource boundary is:

- WAL and RecoveryStorage provide a `VChannelWALView` when `AlterLoadConfig` is
  observed or recovered.
- `viewresource.Registry` consumes that WAL view and prepares a vchannel
  runtime asynchronously.
- `snview.StreamingNodeResourceManager` adapts the QueryView state machine to
  the registry.
- QueryViews only check readiness and release DataVersion anchors. They do not
  pull arbitrary historical resources.

## 2. Core Invariants

1. `AlterLoadConfig` is the only WAL trigger that starts StreamingNode-side
   load preparation for a vchannel. A recovered `VChannelMeta.load_config`
   re-emits the same trigger after WAL bounded replay.
2. `DropLoadConfig` clears load intent and cancels in-flight load preparation.
   Prepared runtimes are reclaimed by local DataVersion eviction when no local
   Up view or recovery anchor protects them.
3. Runtime preparation is forward-only. `GetViewRuntime` is a read-only
   readiness check; it never starts a load for a requested QueryView version.
4. Resource GC is driven by the StreamingNode-local required DataVersion
   watermark:

```text
requiredDataVersions(vchannel) =
  { DataVersion of local QueryViews in Up }
  union
  { DataVersion of persisted Up views during local recovery }

evictBefore(vchannel) = min(requiredDataVersions(vchannel))
```

5. `AlterLoadConfig` load tasks are short-lived initialization work. Their
   DataVersion is not a long-lived retention anchor.
6. Only `Up` QueryViews advance the steady-state QueryView side of the
   watermark. `Preparing`, `Ready`, `Down`, `Dropping`, `Dropped`, and
   `Unrecoverable` do not.

## 3. Recovery Inputs

StreamingNode can prepare resources before any QueryView sync because WAL
recovery restores the inputs needed by `VChannelWALView`.

| Input | Owner | Purpose |
|---|---|---|
| `VChannelMeta.load_config` | VChannelModule | Persisted load intent and load settings. |
| Latest schema | VChannelModule | Schema/function/analyzer runtime construction. |
| Segment snapshot | SegmentModule | Visible growing and retained flushed segment data. |
| Delete replay scanner | TransformLogModule | Historical Deletes needed by growing runtime. |
| Live observer stream | RecoveryStorage | Insert/Delete/Flush messages after WALView capture. |
| BM25 resource RPC | QueryCoord | Sealed BM25 resources for a DataVersion. |
| Persisted Up views | `snview` catalog | Temporary recovery anchors after SN crash. |

`VChannelMeta.load_config` stores load intent, not a target DataVersion:

```proto
message VChannelMeta {
    // existing fields...
    VChannelLoadConfig load_config = 5;
}

message VChannelLoadConfig {
    messages.AlterLoadConfigMessageHeader header = 1;
}
```

The initialization DataVersion comes from
`VChannelWALView.SegmentSnapshot.DataVersion`, which is derived from
SegmentModule state at the WAL capture point.

## 4. Load Flow

`VChannelWALView` creation and live observer delivery are defined by
[StreamingNode VChannel WAL View Design](../wal/streamingnode_vchannel_wal_view.md).
From the resource layer perspective:

```text
RecoveryStorage observes AlterLoadConfig
  -> captures VChannelWALView
  -> calls registry.OnAlterLoadConfig(view)
  -> registry returns VChannelLiveObserver quickly
  -> registry asynchronously prepares growing runtime and BM25 runtime
  -> registry publishes ViewRuntime(DataVersion = view.SegmentSnapshot.DataVersion)
```

The asynchronous load task:

1. validates collection and vchannel identity;
2. derives settings from `view.LoadConfig.header`;
3. uses the latest schema from `view.Schema`;
4. builds csegment-backed growing resources from `view.SegmentSnapshot`;
5. applies historical Deletes from `view.DeleteReplay`;
6. buffers and applies later live `ImmutableMessage` values through the returned
   observer;
7. prepares BM25/IDF resources for the same DataVersion;
8. publishes the runtime only after growing and BM25 resources are ready.

Repeated `AlterLoadConfig` for a vchannel that already has an in-flight load
task or a prepared runtime for the same DataVersion is ignored. It must not
cancel or replace the current task.

## 5. Registry Interface

The target registry interface is vchannel-level and should implement the WAL
listener directly:

```go
type Registry interface {
    walview.LoadConfigListener

    GetViewRuntime(desc ViewResourceDescriptor) (*ViewRuntime, bool, error)
    EvictBefore(collectionID int64, vchannel string, min qviews.DataVersion)
    NotifyReady() <-chan struct{}
}
```

The current `PrepareLatestFromAlterLoadConfig(desc)` shape is an adapter during
implementation. The final boundary should pass `walview.VChannelWALView` into
the registry instead of rebuilding inputs from separate modules.

Registry state is keyed by vchannel and DataVersion:

```text
vchannelState
  runtimes: DataVersion -> runtimeState
  loading: current WALView load task
  evictBefore: DataVersion
```

`GetViewRuntime` returns ready only for a prepared and retained runtime at the
requested DataVersion. If the requested DataVersion is below the eviction
watermark, or no forward load can publish it, `snview` reports the view as
unrecoverable.

## 6. QueryView Integration

`snview.StreamingNodeResourceManager` keeps the QueryView callback contract:

| Operation | Callback |
|---|---|
| `Acquire` | exactly one of `OnReady` or `OnUnrecoverable` |
| `Recover` | exactly one of `OnRecoveringDone` or `OnUnrecoverable` |
| `Release` | exactly one `OnDropped` |

Callbacks must be asynchronous because the view handler calls the resource
manager while holding shard locks.

When persisted Up views are recovered after SN restart, each view adds a
temporary recovery anchor and waits for the matching runtime. If a persisted Up
view remains after `DropLoadConfig` has cleared vchannel load intent, the view
is stale and should be reported as `Unrecoverable`.

## 7. Component Boundaries

- WALView capture, live observer dispatch, TimeTick watermarks, and historical
  Delete replay scanner construction belong to WAL/RecoveryStorage.
- Segment visibility and retained flushed segment GC belong to SegmentModule;
  see [StreamingNode Growing Segment Resource Design](streamingnode_growing_segment.md)
  and [Segment View Module](../wal/segment_view_module.md).
- BM25 sealed resource discovery and DataVersion-level IDF lifecycle belong to
  the BM25 provider; see [StreamingNode BM25 and IDFOracle Design](streamingnode_bm25_idf_oracle.md).
- QueryView state transitions belong to `snview`; the registry is only a local
  resource readiness and eviction service.
