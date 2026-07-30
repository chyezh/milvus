# WAL Replica QueryView Progress 2026-07-31

## Scope

Continued WAL multi-replica implementation toward QueryView integration.

This stage focused on StreamingNode read-only WAL runtime behavior:

- RO WAL opener now initializes QueryView serving resources from PChannel-scoped
  recovery metadata instead of constructing empty vchannel modules from
  persisted QueryViews.
- RO WAL opener filters persisted StreamingNode QueryViews by `wal_replica_id`
  before handler recovery.
- RO WAL opener reads the PChannel consume/data checkpoint only as the local
  projection scanner start point.
- RO WAL starts a read-only local projection scanner that tails the shared
  PChannel WAL and feeds messages into the local `PChannelRecoveryManager`.
- RO WAL still does not append a RecoveryBarrier and does not persist an
  independent recovery checkpoint.
- walmanager tests were updated for the new `walReplicaID` expected-state
  signature.

## Follow-up Progress

This stage added assignment fencing and read-only replica migration cleanup:

- StreamingCoord assignment RPCs and assignment discovery now carry
  `assignment_epoch` for WAL replica operations.
- `PChannelInfoAssigned` carries the WAL replica assignment epoch so
  StreamingNode walmanager can compare same-term replica assignments.
- StreamingNode walmanager orders WAL replica runtime state by `(term,
  assignment_epoch, available)`.
- Stale remove requests with an older `assignment_epoch` are ignored and no
  longer close a newer same-term WAL replica runtime.
- Read-only WAL replica reassignment preserves the old `ActiveNode` only for
  healthy make-before-break migration.
- Failed or unavailable read-only replica reassignment clears the serviceable
  old `ActiveNode` while keeping history for cleanup and diagnostics.
- After a successful read-only replica assignment, StreamingCoord removes
  historical owners using the recorded old assignment epochs, avoiding leaked
  old runtimes without racing the new owner.

## Verification

Commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/server/wal/adaptor -run 'TestWALAdaptorGetQueryPlanBuildsPlanFromLatestUpView|TestBuildQueryPlanWorkNodes|TestROWALAdaptor|TestOpenROWAL' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/distributed/streaming ./internal/streamingcoord/server/balancer ./internal/streamingcoord/server/balancer/channel ./internal/streamingcoord/client/assignment ./internal/streamingcoord/server/service/discover ./internal/coordinator/snmanager ./internal/views/coord/balancer ./internal/views/coord/loadmgr ./internal/views/coord/coordview ./internal/views/coord/coordview/syncer ./internal/views/qviews ./internal/views/worknode/handler ./internal/views/queryclient ./internal/views/queryclient/resolver ./internal/streamingnode/client/handler ./internal/streamingnode/client/handler/registry ./internal/streamingnode/client/handler/transformlog ./internal/streamingnode/server/queryplan ./internal/streamingnode/server/service/handler/transformlog ./internal/streamingnode/server/service ./internal/streamingnode/server/walmanager ./pkg/streaming/util/types -count=1 -timeout 180s
```

```bash
git diff --check
```

All commands passed.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingnode/server/walmanager -run TestWALLifetimeUsesAssignmentEpochForSameTermWALReplica -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingnode/server/service ./internal/streamingnode/client/manager ./internal/streamingnode/server/walmanager ./internal/streamingcoord/server/balancer/channel ./internal/streamingcoord/server/balancer -run 'TestManagerServiceAssignRemoveWALReplica|TestManager|TestWAL|TestChannelManager|TestMutablePChannel|TestBalancer' -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingnode/server/service ./internal/streamingnode/client/manager ./internal/streamingnode/server/walmanager ./internal/streamingcoord/server/balancer/channel ./internal/streamingcoord/server/balancer -count=1 -timeout 180s
```

The broader package set passed through the non-adaptor packages, but the
combined run still needs the adaptor package to be executed separately with
`-gcflags='all=-N -l'`, matching the existing Mockey requirement for those
tests.

## QueryView Assignment Discovery Progress

This stage fixed QueryView shard assignment discovery when an Up shard moves to
a different WAL replica binding:

- `CollectionLoadManager.ObserveShardUp` now treats `(shardID, walReplicaID)`
  as the published assignment identity.
- Re-observing the same shard with the same `walReplicaID` remains a no-op.
- Re-observing the same shard with a different `walReplicaID` updates
  `ShardAssignmentsByPChannel` and triggers the shard assignment notifier.
- This keeps QueryClient routing aligned with the current Up QueryView binding
  after WAL replica rebalance or QueryView rebinding.

The adaptor RO open tests were also adjusted to use a valid segcore test
schema. RO open now initializes real QueryView query resources, so a placeholder
schema containing only field ID 0 is no longer a valid fixture.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/views/coord/loadmgr -run TestCollectionLoadManager_DiscoverableShardAssignments -count=1 -timeout 120s
```

This command was first run before the loadmgr fix and failed with the old
`WALReplicaID: 3` still published after observing `WALReplicaID: 5`. It passed
after the fix.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/views/coord/loadmgr -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/querycoordv2 -run 'Test.*QViews|TestQViews|TestQViewsRuntime|TestSeedDiscoverable' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/server/wal/adaptor -run 'TestWALAdaptorGetQueryPlanBuildsPlanFromLatestUpView|TestBuildQueryPlanWorkNodes|TestROWALAdaptor|TestOpenROWAL' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/distributed/streaming ./internal/streamingcoord/server/balancer ./internal/streamingcoord/server/balancer/channel ./internal/streamingcoord/client/assignment ./internal/streamingcoord/server/service/discover ./internal/coordinator/snmanager ./internal/views/coord/balancer ./internal/views/coord/loadmgr ./internal/views/coord/coordview ./internal/views/coord/coordview/syncer ./internal/views/qviews ./internal/views/worknode/handler ./internal/views/queryclient ./internal/views/queryclient/resolver ./internal/streamingnode/client/handler ./internal/streamingnode/client/handler/registry ./internal/streamingnode/client/handler/transformlog ./internal/streamingnode/server/queryplan ./internal/streamingnode/server/service/handler/transformlog ./internal/streamingnode/server/service ./internal/streamingnode/server/walmanager ./pkg/streaming/util/types -count=1 -timeout 240s
```

All four commands above passed.

## WAL Adaptor Full-Package Verification Progress

This stage fixed the adaptor full-package verification gap:

- `TestWAL` recovery fixture now allows the new recovery background persistence
  calls for segment assignments, segment data-version summaries, transform-log
  metadata, and dropped recovery projections.
- `TestWAL` create-collection fixtures now use a valid segcore test schema
  instead of an empty schema. The empty schema caused segment flush tasks to
  retry forever after real recovery/query-resource initialization became part
  of the WAL replica path.
- The previous full adaptor package timeout is resolved.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/server/wal/adaptor -run '^TestWAL$' -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/server/wal/adaptor -count=1 -timeout 240s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/distributed/streaming ./internal/streamingcoord/server/balancer ./internal/streamingcoord/server/balancer/channel ./internal/streamingcoord/client/assignment ./internal/streamingcoord/server/service/discover ./internal/coordinator/snmanager ./internal/views/coord/balancer ./internal/views/coord/loadmgr ./internal/views/coord/coordview ./internal/views/coord/coordview/syncer ./internal/views/qviews ./internal/views/worknode/handler ./internal/views/queryclient ./internal/views/queryclient/resolver ./internal/streamingnode/client/handler ./internal/streamingnode/client/handler/registry ./internal/streamingnode/client/handler/transformlog ./internal/streamingnode/server/queryplan ./internal/streamingnode/server/service/handler/transformlog ./internal/streamingnode/server/service ./internal/streamingnode/server/walmanager ./pkg/streaming/util/types -count=1 -timeout 240s
```

```bash
git diff --check
```

All four commands above passed.

## Primary Switchover Runtime Propagation Progress

This stage aligned the StreamingCoord runtime assignment path with the WAL
replica meta model:

- `SwitchWALPrimaryReplica` records the old primary WAL replica before the meta
  CAS, increments the PChannel write `term`, and switches access mode in the
  single PChannel meta value.
- After the meta switch succeeds, StreamingCoord sends a demotion assignment to
  the old primary owner with the new `term`, `AccessModeRO`, and a new
  `assignment_epoch`.
- StreamingCoord also sends a promotion assignment to the target owner with the
  same new `term`, `AccessModeRW`, and the target replica assignment epoch.
- `AssignWALReplicasDone` is called for the target primary only after the target
  promotion RPC succeeds, so a failed promotion is not reported as serviceable.
- Demotion and promotion errors are combined and returned to the caller. The
  metadata switch is still the source of truth; failed runtime propagation is a
  follow-up reconciliation problem rather than a metadata rollback.

This keeps `Term` as the PChannel write-chain epoch, while `assignment_epoch`
fences per-replica runtime assignment churn within the same term.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer -run TestBalancerSwitchWALPrimaryReplicaAssignsTargetAsReadWrite -count=1 -timeout 120s
```

The focused test was first run before the runtime demotion fix and failed
because the old primary owner did not receive the expected `AccessModeRO`
assignment. It passed after the fix.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer -count=1 -timeout 180s
```

This command passed.

## RO WAL TransformLog Progress

This stage connected read-only WAL replicas to their local QueryView
TransformLog resources:

- `roWALAdaptorImpl.TransformLog()` now returns the recovered QueryView resource
  manager when it is available.
- If a read-only WAL has not recovered query resources yet, TransformLog access
  returns an unavailable accesser error instead of exposing an empty or
  write-path TransformLog implementation.
- The RO open recovery test now acquires a TransformLog stream for a recovered
  QueryView VChannel and verifies that catchup can reach `SyncUp`.

This matches the selected model: the durable WAL backend remains shared by
PChannel, while each WAL replica serves its own local QueryView projection,
including TransformLog state used by QueryNodes.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/server/wal/adaptor -run TestOpenROWALRecoversQueryViewHandlerAndVChannelModules -count=1 -timeout 120s
```

The focused test was first run before the TransformLog access fix and failed
with `STREAMING_CODE_ON_SHUTDOWN: read only wal does not serve transform log`.
It passed after the fix.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/server/wal/adaptor -run '^TestWAL$' -count=1 -timeout 180s
```

This command passed.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/server/wal/adaptor -count=1 -timeout 240s
```

This full adaptor package command failed during `TestWAL` while appending the
initial RecoveryBarrier:

```text
when building interceptor params: append recovery barrier message failed: random error
```

Investigation found the failure comes from the test WAL implementation's random
append-error injection for non-TimeTick messages. RecoveryBarrier currently
participates in that random failure path, so opening an RW WAL in package-wide
test order can fail before the WAL under test is usable. The focused `TestWAL`
command above passed, so this remains a test-fixture flake to fix separately
rather than evidence of the RO TransformLog change failing.
