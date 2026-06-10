# WAL Recovery Architecture

This document defines the RecoveryStorage framework shared by all WAL recovery
modules. Module-specific state and workflows are described in separate files:

- [VChannel View Module](vchannel_view_module.md)
- [Segment View Module](segment_view_module.md)
- [TransformLog View Module](transform_log_view_module.md)
- [Broadcast Ack Module](broadcast_ack_module.md)
- [Message Workflow](message-workflow.md)

## 1. Background

StreamingNode consumes persisted WAL messages to rebuild and advance
module-owned recovery state. Each module maintains one or more in-memory Views.
A View is the module's consistency state and can be persisted to the recovery
catalog when dirty.

Each View has two logical parts:

- **Meta**: the synchronous part updated by `ObserveMessage`.
- **Data**: the asynchronous part advanced by durable tasks, lifecycle side
  effects, or coordinator acknowledgements.

Recovery uses persisted View snapshots plus historical WAL messages to rebuild
the same in-memory state through the same `ObserveMessage` path used by live
consumption.

## 2. View Model

A View contains:

- module-specific Meta state;
- module-specific Data state;
- `MetaTimeTick`, the latest WAL timetick reflected by Meta;
- `DataTimeTick`, the latest WAL timetick whose Data-side effects are durable.

The relationship is:

```text
ObserveMessage(M)
  -> module synchronously updates View.Meta and View.MetaTimeTick
  -> View becomes dirty

Data task completion
  -> module asynchronously updates View.Data and View.DataTimeTick
  -> View becomes dirty

View persist task
  -> module persists dirty View snapshot to catalog
  -> module advances MetaBarrier and/or DataBarrier
```

The invariant is:

```text
View.MetaTimeTick >= View.DataTimeTick
```

`MetaTimeTick` usually advances first because Meta updates are synchronous.
`DataTimeTick` advances after asynchronous durable work completes.

## 3. Core Architecture

Each PChannel has one RecoveryStorage instance. RecoveryStorage dispatches
persisted WAL messages to modules, tracks physical checkpoints through
CheckpointManager, and persists WALCheckpoint.

```text
WAL Scanner
    |
    v
RecoveryStorage
    |
    +--> VChannelModule
    |       +-- VChannel/schema/partition views
    |       +-- Meta barriers
    |
    +--> SegmentModule
    |       +-- Segment assignment and L1 views
    |       +-- Segment data tasks
    |       +-- Meta/Data barriers
    |
    +--> TransformLogModule
    |       +-- Delete TransformLog views
    |       +-- TransformLog data tasks
    |       +-- Meta/Data barriers
    |
    +--> AckModule
    |       +-- broadcast ack tasks
    |       +-- Data barriers
    |
    +--> Scheduler
    |       +-- asynchronous module tasks
    |
    +--> CheckpointManager
            +-- Meta physical checkpoint
            +-- Data physical checkpoint
```

The framework has four layers:

1. **Observe layer**: synchronously consumes WAL messages and updates module
   Meta.
2. **Task layer**: runs module-owned Data tasks and View persistence tasks
   through Scheduler.
3. **Barrier layer**: exposes persisted View progress as Meta/Data barriers.
4. **Physical checkpoint layer**: advances WALCheckpoint after
   CheckpointManager sees no remaining barrier for the ordered prefix.

## 4. RecoveryStorage

RecoveryStorage is the PChannel-level orchestration layer.

It owns:

- WAL scanner lifecycle;
- module dispatch;
- CheckpointManager;
- WALCheckpoint persistence;
- WAL truncation by persisted Data physical checkpoint;
- background triggering of module persistence and policy-driven checks.

It does not own:

- module Views;
- module dirty state;
- module business decisions;
- Data tasks;
- object storage writes;
- lifecycle side effects;
- broadcast ack conditions.

RecoveryStorage calls every module for every persisted WAL message. Modules
decide whether the message is relevant, update their own state, and return
barriers. RecoveryStorage registers the message physical point and returned
barriers into CheckpointManager.

RecoveryStorage may trigger modules to submit background tasks, but the module
decides what task to generate and how its View changes.

## 5. CheckpointManager

CheckpointManager owns ordered physical checkpoint advancement.

It tracks two physical lanes:

- **Meta physical checkpoint**: WAL restart point for Meta recovery.
- **Data physical checkpoint**: WAL restart point for Data recovery and the WAL
  retention point.

For each consumed message, RecoveryStorage registers a physical point and
optional barriers:

```text
physical point P + MetaBarrier
physical point P + DataBarrier
```

A physical lane advances only through the continuous prefix whose barriers have
disappeared. A barrier disappears when the owning module has persisted the View
state required for that physical point.

CheckpointManager does not know vchannel, segment, schema, tombstone, import,
ack, object storage, or lifecycle semantics.

## 6. Scheduler

Scheduler is the asynchronous execution plane. Modules submit tasks to Scheduler
for:

- Data persistence;
- lifecycle side effects;
- broadcast ack;
- View persistence;
- cleanup.

Scheduler is parallel by default. Task ordering is expressed through
preconditions:

- Segment Data tasks are ordered per segment.
- TransformLog Data tasks are ordered per vchannel.
- View persist tasks are ordered per module or per owner according to module
  policy.
- Ack tasks are ordered by WAL ack order.
- Cleanup waits for persisted physical checkpoints to pass the retained
  tombstone timetick.

Scheduler does not understand module business keys. It only checks task
preconditions.

## 7. Module Boundaries

RecoveryStorage owns three independent growing-data modules:

- `VChannelModule`: VChannel metadata, schema history, partition lifecycle, and
  VChannel tombstones.
- `SegmentModule`: Segment assignment metadata, Insert/L1 output, segment
  lifecycle side effects, and segment tombstones.
- `TransformLogModule`: Delete and Txn(Delete) TransformLog buffers, chunk
  files, scanners, truncation, L0 materialization, and transform-log
  tombstones.

Each module implements the RecoveryStorage module API directly. There is no
outer data-module coordinator owning their business logic.

The only required cross-module read is:

```text
SegmentModule -> VChannelModule.SchemaAt(vchannel, partitionID, timetick)
```

This is needed to attach the correct historical schema when `SegmentModule`
creates segment state. Tombstone finalize and cleanup are module-local
responsibilities. `TransformLogModule` does not read VChannel or Segment state
for Delete replay, tombstone finalize, or cleanup.

## 8. Normal Workflow

### 8.1 WAL Open

```text
Load WALCheckpoint
Load module View snapshots from catalog
Construct modules in MetaOnly mode
Run bounded Meta scanner from Meta physical checkpoint to open tail
ObserveMessage rebuilds module Meta and dirty Views
Switch modules into MetaAndData mode
Start data/live scanner from Data physical checkpoint
WAL open succeeds
```

Meta recovery and live consumption use the same `ObserveMessage`
implementation. During the bounded Meta scanner, modules are in MetaOnly mode,
so `ObserveMessage` updates only Meta and does not submit Data-chain work. After
modules switch into MetaAndData mode, the data/live scanner enables Data-chain
buffering and task submission.

### 8.2 ObserveMessage

```text
Scanner reads persisted message M
RecoveryStorage dispatches M to every module
Each module synchronously updates relevant Meta and MetaTimeTick
Each module may append lightweight in-memory buffers
Each module may submit tasks according to policy
Each module returns Meta/Data barriers
RecoveryStorage registers M into CheckpointManager
```

`ObserveMessage` must not perform object storage writes, catalog writes,
lifecycle RPCs, broadcast RPCs, or long retry loops.

### 8.3 Data Task Completion

```text
Scheduler runs module Data task
Task performs durable side effect
Owning module updates Data state and DataTimeTick
Owner becomes dirty
RecoveryStorage background or module policy triggers persist task
```

Data task completion updates View state in memory. It does not directly advance
a physical checkpoint.

### 8.4 View Persistence

```text
Scheduler runs View persist task
Task persists dirty View snapshot to catalog
Owning module updates MetaBarrier/DataBarrier from persisted snapshot
CheckpointManager observes that barriers disappeared
RecoveryStorage persists new physical WALCheckpoint
```

View persistence is module-owned asynchronous work. RecoveryStorage only
triggers modules and observes barriers.

### 8.5 Physical Checkpoint Persistence

```text
CheckpointManager advances in-memory physical checkpoints
RecoveryStorage persists WALCheckpoint
RecoveryStorage truncates WAL by persisted Data physical checkpoint
```

WAL truncation never uses Meta physical checkpoint.

## 9. Failure Recovery

Recovery restarts from persisted WALCheckpoint and module View snapshots
persisted in catalog.

| Failure point | Recovery behavior |
|---|---|
| Crash after ObserveMessage but before View persist | WALCheckpoint cannot cross the message because the View-backed barrier still blocks. Recovery scans the message again and rebuilds Meta through `ObserveMessage`. |
| Crash after Data side effect but before View persist | Data physical checkpoint cannot cross the message. The data scanner restarts from the older Data checkpoint and repeats or reconciles the Data task. |
| Crash after View persist but before WALCheckpoint persist | Recovery starts from an older physical checkpoint. The persisted View snapshot already records progress, so repeated `ObserveMessage` is skipped or handled idempotently by module state. |
| Crash after Meta physical checkpoint persist while Data checkpoint is older | Meta recovery starts later; the data scanner starts earlier. Retained Views and tombstones remain available for Data-chain observation. |
| Crash after Data physical checkpoint persist | The data scanner starts after that checkpoint. This is safe because Data checkpoint persistence happens only after Data barriers disappear. |
| Crash after broadcast ack before WALCheckpoint persist | Recovery may ack again. Broadcast ack is replayable and idempotent. |

## 10. Tombstone Retention

Dropped or flushed objects remain in retained Views until the module that owns
the object can safely remove them.

Tombstone state is module-local:

- VChannel and partition tombstones are owned by `VChannelModule`.
- Segment tombstones are owned by `SegmentModule`.
- TransformLog tombstones and truncation cleanup are owned by
  `TransformLogModule`.

Retained tombstoned Views can be physically deleted only when the owning module
has persisted the tombstone state and both physical checkpoint lanes have passed
the tombstone timetick:

```text
Meta physical checkpoint > tombstone timetick
Data physical checkpoint > tombstone timetick
```

This guarantees neither scanner can restart from a point that still needs the
retained View or historical-message filter.

## 11. Design Constraints

- View is the module-owned consistency state in memory and can be persisted
  when dirty.
- Meta and Data are both parts of View.
- `ObserveMessage` synchronously updates View.Meta and `MetaTimeTick`.
- Data tasks asynchronously update Data state and `DataTimeTick`.
- Dirty Views are persisted by module-owned asynchronous tasks.
- MetaBarrier and DataBarrier advance only after the corresponding dirty View
  snapshot is persisted.
- RecoveryStorage persists WALCheckpoint and dispatches module work, but does
  not own module business state.
- CheckpointManager advances physical checkpoints after barriers disappear from
  the ordered prefix.
- Scheduler executes module-owned asynchronous work and uses preconditions for
  ordering.
- WAL truncation uses only the persisted Data physical checkpoint.
