# VChannel View Module

`VChannelModule` owns collection-level WAL recovery metadata for a PChannel. It
is responsible for VChannel, partition, and schema state only.

## 1. Ownership

`VChannelModule` owns:

- VChannel creation and lifecycle metadata;
- collection metadata needed by WAL recovery;
- partition lifecycle metadata;
- schema history;
- VChannel and partition tombstones;
- dirty snapshots and VChannel meta persistence;
- the read-only `SchemaAt(vchannel, partitionID, timetick)` view used by
  `SegmentModule`.

`VChannelModule` does not own:

- Insert data;
- Segment assignment metadata;
- Delete or Txn(Delete) data;
- TransformLog buffers, chunks, scanners, or truncation;
- Segment lifecycle side effects;
- broadcast acknowledgement.

## 2. Public View

The only cross-module read required from `VChannelModule` is schema lookup:

```go
type SchemaProvider interface {
    SchemaAt(vchannel string, partitionID int64, timetick uint64) (*schemapb.CollectionSchema, bool)
}
```

`SegmentModule` uses this when observing `CreateSegment` so the segment state
stores the correct historical schema snapshot.

No other module should depend on VChannel tombstone state for its own replay or
cleanup. Segment and TransformLog tombstones are decided by their owning
modules.

## 3. Observe Rules

### CreateCollection

Creates the VChannel View if absent. The View records collection metadata,
initial partitions, schema history, normal VChannel state, and `MetaTimeTick`.
It returns a Meta barrier until the dirty snapshot is persisted.

### CreatePartition

Adds or restores the partition in normal state and advances `MetaTimeTick`.
It returns a Meta barrier.

### DropPartition

Marks the partition as dropped at the message timetick and advances
`MetaTimeTick`. The partition remains retained until VChannelModule-local
tombstone cleanup removes it.

`DropPartition` does not wait for Segment or TransformLog tombstones before
recording the VChannel metadata change. Ack preconditions compose the relevant
data frontiers outside this module.

### DropCollection

Marks the VChannel as dropped at the message timetick and advances
`MetaTimeTick`. The VChannel metadata remains retained until VChannelModule
cleanup removes it.

### TruncateCollection

Advances the VChannel metadata checkpoint to the truncation timetick. The
collection View is not removed by this message.

### AlterCollection

Updates collection metadata. If the message changes schema, a new schema
version is appended to schema history. Existing SegmentModule state keeps its
own schema snapshot; segments created later read the new schema through
`SchemaAt`.

## 4. Tombstone And Cleanup

VChannel and partition tombstones are independent from Segment and TransformLog
tombstones.

`VChannelModule` can physically remove retained VChannel or partition metadata
when:

```text
Meta physical checkpoint > tombstone timetick
Data physical checkpoint > tombstone timetick
```

Cleanup does not inspect `SegmentModule` or `TransformLogModule` internal state.
The safety condition is the persisted physical checkpoints, not cross-module
tombstone ordering.

## 5. Recovery

On WAL open, RecoveryStorage loads VChannel snapshots from catalog and
constructs `VChannelModule` in MetaOnly mode. Historical WAL replay uses the
same `ObserveMessage` implementation. Dirty snapshots are persisted by
VChannelModule-owned persist tasks.

Schema history is VChannel child state. VChannel names are not reusable, so a
final VChannel cleanup removes the VChannel owner key and schema keys under the
same VChannel prefix.

## 6. Invariants

1. VChannel metadata is the source for collection, partition, and schema state.
2. Schema history is owned by VChannelModule.
3. `SchemaAt` is the only required cross-module read from VChannelModule.
4. VChannelModule does not own Insert or Delete data.
5. VChannel tombstone and cleanup decisions are module-local.
6. VChannel cleanup is gated by physical checkpoints, not by direct Segment or
   TransformLog state reads.
