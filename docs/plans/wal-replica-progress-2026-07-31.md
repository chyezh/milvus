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

## QueryCoord QueryView Load-Config Service Progress

This stage adapted more QueryCoord service and DDL callback paths to use the
QueryView load-config model instead of assuming legacy `meta/targetMgr` state:

- `CollectionLoadManager.SyncNewCreatedPartition` now updates the stored
  QueryView `LoadConfig` for collection loads and skips partition-load configs
  that should not automatically absorb new partitions.
- `SyncNewCreatedPartition` routes through the QueryView runtime load manager
  when available, with the legacy job path kept as fallback.
- `RefreshCollection`, `ManualUpdateCurrentTarget`, `GetPartitionStates`, and
  `GetReplicas` now read QueryView load-config state when the QueryView runtime
  store is available.
- Release-partition and transfer-replica callbacks now preserve QueryView
  load-config fields such as load type, partition IDs, replica layout, selected
  fields, and user-specified replica mode when generating alter-load messages.
- Focused service tests were moved away from legacy replica/target assertions
  where the architecture now stores desired state in QueryView load configs.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/views/coord/loadmgr -run 'TestCollectionLoadManager_SyncNewCreatedPartition' -count=1 -timeout 120s
```

This command was first run before the load-manager method existed and failed
at compile time. It passed after adding `SyncNewCreatedPartition`.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/views/coord/loadmgr ./internal/querycoordv2 -run 'Test(CollectionLoadManager_SyncNewCreatedPartition|SyncNewCreatedPartitionUsesQViewsLoadConfig)' -count=1 -timeout 120s
```

This command was first run before the QueryCoord service path used the
QueryView load-config store and failed because the QV config was unchanged. It
passed after routing the service through `CollectionLoadManager`.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'Test(ReleasePartitionsUsesQViewsLoadConfig|TransferReplicaUsesQViewsLoadConfig)' -count=1 -timeout 120s
```

This command was first run before release-partition and transfer-replica
callbacks used QueryView state. It passed after the callbacks generated alter
load-config messages from the current QueryView config.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'Test(ReleasePartitionsDropLoadConfigBroadcastsToCollectionVChannels|ReleasePartitionsUsesQViewsLoadConfig|TransferReplicaUsesQViewsLoadConfig|SyncNewCreatedPartitionUsesQViewsLoadConfig)' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'TestService/Test(SyncNewCreatedPartition|ReleasePartition|TransferReplica)$' -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'TestService/Test(LoadCollectionWithUserSpecifiedReplicaMode|LoadCollectionWithoutUserSpecifiedReplicaMode|LoadPartitionsWithUserSpecifiedReplicaMode|LoadPartitionsWithoutUserSpecifiedReplicaMode|LoadPartitionWithLoadFields|LoadPartitionWithUserSpecifiedReplicaMode|LoadPartitionUpdateUserSpecifiedReplicaMode|ManualUpdateCurrentTarget|RefreshCollection)$' -count=1 -timeout 180s
```

The three focused commands above passed.

The full `internal/querycoordv2` package still has legacy-service-suite
failures to adapt:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -count=1 -timeout 240s
```

Known remaining failures include `TestGetReplicas`, `TestGetSegmentInfo`,
`TestGetShardLeaders`, `TestGetShardLeadersFailed`,
`TestGetShardLeadersWithUnserviceableShards`, `TestHandleNodeUp`, and the
legacy load-balance service tests. The common cause is that these paths still
expect legacy replica manager, target manager, or dist-manager state, while the
QueryView architecture now stores desired load state in `loadConfigStore` and
published shard state in `shardViewRegistry`.

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

## WAL Replica Binding And RecoveryBarrier Verification Progress

This stage fixed two follow-up issues found during verification and design
audit:

- The test WAL implementation no longer injects random append errors for
  `RecoveryBarrier` messages. RecoveryBarrier still observes real fenced-channel
  errors, but package tests no longer fail randomly while opening an RW WAL.
- Added a regression test that appends many RecoveryBarrier messages with
  random error injection enabled; before the fix it failed with `random error`,
  after the fix it passed.
- QueryView balancer planning now tracks RW WAL replica usage per vchannel
  within one reconcile cycle. The first QueryView replica can bind to the
  serviceable RW WAL replica; additional QueryReplicas for the same vchannel
  request an RO WAL replica instead of all becoming primary-serving by binding
  to the same RW replica.
- Reprepare of an existing Up primary QueryView keeps its current RW WAL
  binding, so data-version or load-version advancement does not accidentally
  block itself behind the per-vchannel primary slot.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./pkg/streaming/walimpls/impls/walimplstest -run TestRecoveryBarrierAppendBypassesRandomFenceError -count=1 -timeout 120s
```

This command was first run before the fixture fix and failed with
`random error`. It passed after the fix.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./pkg/streaming/walimpls/impls/walimplstest -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/views/coord/balancer -run 'TestDefaultBalancePolicy_(OnlyOneReplicaBindsReadWriteWALPerShard|ReprepareKeepsExistingReadWriteWALBinding)' -count=1 -timeout 120s
```

`TestDefaultBalancePolicy_OnlyOneReplicaBindsReadWriteWALPerShard` was first
run before the QV balancer fix and failed because both QueryReplicas prepared
with `walReplicaID = 0` and no RO WAL demand was emitted. The focused command
above passed after the fix.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/views/coord/balancer -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/distributed/streaming ./internal/streamingcoord/server/balancer ./internal/streamingcoord/server/balancer/channel ./internal/streamingcoord/client/assignment ./internal/streamingcoord/server/service/discover ./internal/coordinator/snmanager ./internal/views/coord/balancer ./internal/views/coord/loadmgr ./internal/views/coord/coordview ./internal/views/coord/coordview/syncer ./internal/views/qviews ./internal/views/worknode/handler ./internal/views/queryclient ./internal/views/queryclient/resolver ./internal/streamingnode/client/handler ./internal/streamingnode/client/handler/registry ./internal/streamingnode/client/handler/transformlog ./internal/streamingnode/server/queryplan ./internal/streamingnode/server/service/handler/transformlog ./internal/streamingnode/server/service ./internal/streamingnode/server/walmanager ./pkg/streaming/util/types ./pkg/streaming/walimpls/impls/walimplstest -count=1 -timeout 240s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/server/wal/adaptor -count=1 -timeout 240s
```

```bash
git diff --check
```

All commands above passed after the corresponding fixes.

## Primary Switchover QueryView Readiness Progress

This stage tightened the StreamingCoord metadata state machine around WAL
primary switchover:

- `ChannelManager.SwitchWALPrimaryReplica` now checks the current published
  QueryView shard assignments before switching `PrimaryReplicaID`.
- If the current primary WAL replica has a discoverable shard assignment on the
  PChannel, the target WAL replica must already have a discoverable assignment
  for the same `(collectionID, shardIndex)`.
- The check intentionally ignores `QueryReplicaID`. QueryView primary status is
  derived from the WAL replica access mode after the switch, so the target only
  needs an Up QueryView for the shard on that WAL replica.
- If no shard assignment provider is registered, or if the current primary has
  no discoverable QueryView shards on the PChannel, the switch keeps the legacy
  behavior and is allowed.
- This turns the design's "prepare target QV first, then switch WAL access
  mode" rule into a hard metadata precondition. The current code still exposes
  primary switch as an explicit StreamingNodeManager operation; automatic
  QV-triggered primary migration policy remains a later integration step.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer/channel -run TestChannelManagerRejectsPrimarySwitchUntilTargetHasPrimaryServingShards -count=1 -timeout 120s
```

This command was first run before the precondition fix and failed because
`SwitchWALPrimaryReplica` advanced the term and switched primary even though
the target WAL replica had no published QueryView shard. It passed after the
fix.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer/channel -run 'TestChannelManager(RejectsPrimarySwitchUntilTargetHasPrimaryServingShards|SwitchWALPrimaryReplicaWhenTargetHasPrimaryServingShards|SwitchWALPrimaryReplica)' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer/channel -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/distributed/streaming ./internal/streamingcoord/server/balancer ./internal/streamingcoord/server/balancer/channel ./internal/streamingcoord/client/assignment ./internal/streamingcoord/server/service/discover ./internal/coordinator/snmanager ./internal/views/coord/balancer ./internal/views/coord/loadmgr ./internal/views/coord/coordview ./internal/views/coord/coordview/syncer ./internal/views/qviews ./internal/views/worknode/handler ./internal/views/queryclient ./internal/views/queryclient/resolver ./internal/streamingnode/client/handler ./internal/streamingnode/client/handler/registry ./internal/streamingnode/client/handler/transformlog ./internal/streamingnode/server/queryplan ./internal/streamingnode/server/service/handler/transformlog ./internal/streamingnode/server/service ./internal/streamingnode/server/walmanager ./pkg/streaming/util/types ./pkg/streaming/walimpls/impls/walimplstest -count=1 -timeout 240s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/server/wal/adaptor -count=1 -timeout 240s
```

```bash
git diff --check
```

All commands above passed after the precondition fix.

## Ordinary Balance RW Primary Pinning Progress

This stage fixed one remaining design violation in the StreamingCoord ordinary
balance path:

- `balance()` still used the legacy `AssignPChannels` path for all serviceable
  RW PChannels. When `streaming.primaryResourceGroup` changed, the status
  collection hint could filter out the old primary owner and make the policy
  treat the RW PChannel as unassigned.
- Added a regression test where the old RW primary's StreamingNode remains
  alive in `rg-old` while the primary RG hint switches to `rg-new`. Before the
  fix, ordinary balance reassigned the RW PChannel to the new RG and advanced
  the term. After the fix, no ordinary `Assign` is issued.
- `CurrentLayout` now distinguishes all nodes needed to represent the current
  layout from nodes that ordinary balance may choose as new targets.
- When a configured primary RG filters out a still-discovered RW primary owner,
  `generateCurrentLayout` keeps that owner as a pinned current assignment but
  excludes it from assignable candidates.
- `CurrentLayout.AllowRebalance` now rejects auto-rebalance of already assigned
  RW PChannels. RW write-owner movement must go through the explicit WAL
  primary switchover path that checks QueryView readiness.
- Existing vchannelfair policy fairness tests were updated to exercise ordinary
  rebalance with RO channels, matching the new split: RO replicas can use
  ordinary make-before-break assignment, while RW primary movement is planned
  separately.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer -run TestBalancer_PrimaryResourceGroupChangeDoesNotReassignServiceableRWPrimary -count=1 -timeout 120s
```

This command was first run before the fix and failed because the serviceable RW
primary was reassigned from node 1 to node 2 by ordinary balance. It passed
after the fix.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer/policy/vchannelfair -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer -count=1 -timeout 180s
```

Both package commands passed after updating the old RW auto-rebalance test
expectation to the new primary-switchover model.

## QueryView Runtime WAL Primary Switch Hook Progress

This stage wired the QueryView runtime into the explicit WAL primary switchover
loop:

- `newQViewsRuntime` installs a WAL replica provider from StreamingCoord
  assignment discovery when one is not injected by tests.
- The default QueryView balancer is connected to a WAL replica demand executor,
  so RO WAL replica creation requests can be routed to StreamingCoord through
  `EnsureReadOnlyWALReplica`.
- The shard stats observer now publishes discoverable shard assignments with
  the Up shard's `walReplicaID`.
- When an Up shard is observed on a serviceable RO WAL replica in the
  configured primary resource group, and the same PChannel has no serviceable
  RW replica in that group, QueryCoord asks StreamingCoord to switch WAL
  primary to that WAL replica.
- Runtime startup now scans recovered Up shard stats after registering the
  shard assignment provider and publishing the initial assignment update. This
  covers QueryCoord restart cases where `RegisterStatsObserver` does not replay
  the recovered snapshot.
- Startup switch attempts are deduplicated by `(pchannel, walReplicaID)` so a
  recovered multi-shard PChannel does not issue repeated identical switch
  requests.
- QueryView still does not persist or decide a separate primary flag. The
  primary-serving property remains derived from the WAL replica's current
  `AccessModeRW` state after StreamingCoord completes the switch.

The switch request remains best-effort from the QueryView runtime. StreamingCoord
keeps the authoritative readiness precondition and rejects the switch until the
target WAL replica has all required published primary-serving shards.

Additional commands run:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run TestNewQViewsRuntimeSwitchesWALPrimaryForRecoveredUpShard -count=1 -timeout 120s
```

This command was first run before the startup scan fix and failed because no
WAL primary switch request was issued for the recovered Up shard. It passed
after the fix.

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'Test(MaybeSwitchWALPrimaryReplicaForShardUp|NewQViewsRuntime|StreamingCoordWALReplicaDemandExecutor|QViewsRuntimeStartRegistersStreamingCoordProviders|QViewsWALReplicaDependencyProvider)' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 ./internal/views/coord/balancer -run 'Test(MaybeSwitchWALPrimaryReplicaForShardUp|NewQViewsRuntime|StreamingCoordWALReplicaDemandExecutor|QViewsRuntimeStartRegistersStreamingCoordProviders|QViewsWALReplicaDependencyProvider|DefaultBalancePolicy|SnapshotBuilder)' -count=1 -timeout 180s
```

Both commands passed.

The broader related package command below passed every listed package except
`internal/querycoordv2`:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 ./internal/distributed/streaming ./internal/streamingcoord/server/balancer ./internal/streamingcoord/server/balancer/channel ./internal/streamingcoord/client/assignment ./internal/streamingcoord/server/service/discover ./internal/coordinator/snmanager ./internal/views/coord/balancer ./internal/views/coord/loadmgr ./internal/views/coord/coordview ./internal/views/coord/coordview/syncer ./internal/views/qviews ./internal/views/worknode/handler ./internal/views/queryclient ./internal/views/queryclient/resolver ./internal/streamingnode/client/handler ./internal/streamingnode/client/handler/registry ./internal/streamingnode/client/handler/transformlog ./internal/streamingnode/server/queryplan ./internal/streamingnode/server/service/handler/transformlog ./internal/streamingnode/server/service ./internal/streamingnode/server/walmanager ./pkg/streaming/util/types ./pkg/streaming/walimpls/impls/walimplstest -count=1 -timeout 300s
```

The remaining `internal/querycoordv2` full-package failures are in legacy
`ServiceSuite` paths such as `TestSyncNewCreatedPartition`,
`TestReleasePartition`, and `TestTransferReplica`. The observed root cause is
that these tests still assert old `meta/targetMgr` load state after the load
path has been moved to QueryView `loadConfigStore` and `shardViewRegistry`.
Focused QueryView runtime and balancer tests pass; the legacy service suite
needs a separate QueryView-architecture adaptation pass.

```bash
git diff --check
```

This command passed.

## QueryView Service API Projection Progress

This stage adapted QueryCoord read APIs that were still assuming legacy
`ReplicaManager`, `targetMgr`, and dist state to the QueryView architecture:

- `GetReplicas` now projects replica information from QueryView load config and
  Up shard stats. QueryNode membership is derived from the serviceable shard
  placements reported by `shardViewRegistry`.
- `GetLoadSegmentInfo` now reads segment placement from Up `ShardStats` and
  returns `ErrSegmentNotLoaded` when a requested segment is not present in the
  QueryView projection.
- `GetShardLeaders` now derives shard leader candidates from Up QueryView shard
  stats. Strict mode still reports `ErrChannelNotAvailable` when no serviceable
  shard is available; relaxed mode can return an empty shard list.
- Legacy manual sealed segment `LoadBalance` is rejected in QueryView mode with
  `ErrParameterInvalid`. Placement movement under QueryView should be expressed
  through the QueryView balancer and view synchronization flow.
- DDL callback tests that observe load state were updated to assert QueryView
  load config instead of legacy collection or partition load metadata.
- Node-up service tests now check resource group membership in QueryView mode
  and no longer expect legacy target manager or dist initialization side
  effects.

Focused commands run and passed:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'Test(GetReplicasUsesQViewsShardStats|Service/Test(GetPartitionStates|GetReplicas))$' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'TestService/TestDDLCallbacksReleasePartition$' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'Test(Service/TestGetSegmentInfo|GetLoadSegmentInfoUsesQViewsShardStats)$' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'Test(GetShardLeadersUsesQViewsShardStats|Service/TestGetShardLeaders|Service/TestGetShardLeadersFailed|Service/TestGetShardLeadersWithUnserviceableShards)$' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'TestService/Test(DDLCallbacksLoadPartition|DDLCallbacksLoadCollectionForceOverrideUserSpecifiedReplicaMode)$' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'TestService/Test(LoadBalance|LoadBalanceFailed|LoadBalanceWithEmptySegmentList|LoadBalanceWithNoDstNode|HandleNodeUp)$' -count=1 -timeout 120s
```

The full QueryCoord package command still fails:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -count=1 -timeout 240s
```

The remaining failures are currently isolated to `TestServer`, with panics in
`TestNodeDown`, `TestNodeUp`, `TestNodeUpdate`, `TestRecover`, `TestStop`, and
`TestUpdateAutoBalanceConfigLoop`. The stack points at QueryView runtime start
registering the StreamingNode shard assignment provider through the mocked
StreamingCoord balancer. The likely next fix is to update `TestServer` mock
setup to expect the provider registration and initial assignment update, matching
the QueryView runtime startup contract.

## QueryCoord ServerSuite QueryView Adaptation

This stage fixed the remaining QueryCoord package failures after the service API
projection work:

- The shared `initStreamingSystem()` test helper now allows the QueryView
  runtime startup calls to register the shard assignment provider, register the
  WAL replica dependency provider, and publish the initial shard assignment
  update through the mocked StreamingCoord balancer.
- `ServerSuite` setup and recovery checks now verify QueryView load config when
  the QueryView runtime is active. Legacy `meta.Exist` checks are kept only for
  non-QueryView runtime paths.
- `ServerSuite` node-up and node-down checks now validate `nodeMgr` plus
  ResourceGroup membership in QueryView mode instead of expecting legacy
  collection replica membership to be updated.
- `qviewsRuntime.stop()` is idempotent, so repeated server shutdown does not
  close QueryView clients twice.
- `Server.Stop()` is now guarded by `sync.Once`, preserving the existing
  idempotent Stop contract even when downstream clients are not idempotent.

Commands run and passed:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run '^TestServer$/^TestStop$' -count=1 -timeout 60s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run '^TestServer$/^TestUpdateAutoBalanceConfigLoop$' -count=1 -timeout 80s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run '^TestServer$/^Test(NodeDown|NodeUp|NodeUpdate|Recover|Stop|UpdateAutoBalanceConfigLoop)$' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -count=1 -timeout 300s
```

```bash
git diff --check
```

All commands above passed.

## WAL Replica Assignment Error Report Progress

This stage fixed the read-only WAL replica assignment error path:

- `ReportAssignmentErrorRequest` now carries optional `wal_replica_id` and
  `assignment_epoch` fields. `wal_replica_id = 0` keeps the legacy PChannel
  primary behavior.
- `AssignmentService` exposes `ReportWALReplicaAssignmentError`, and the
  assignment discover client sends replica-specific error reports fenced by
  `(pchannel, walReplicaID, assignmentEpoch)`.
- Duplicate or stale RO replica error reports are ignored by assignment epoch
  instead of PChannel write term, because RO reassignments do not advance term.
- StreamingCoord discover server routes replica-specific reports to
  `MarkWALReplicasAsUnavailable` and keeps legacy `MarkAsUnavailable` for
  primary/PChannel reports.
- `ChannelManager.MarkWALReplicasAsUnavailable` marks only non-primary
  read-only replicas unavailable when the reported epoch still matches. It does
  not mutate the PChannel primary projection or advance `Term`.
- StreamingNode handler creation for WAL replica-bound TransformLog streams now
  reports permanent wrong-node failures through the replica-specific path, with
  a legacy fallback for callers that only implement the old rebalance trigger.
- A WAL replica report is treated as replica-specific only when the reported
  assignment is `AccessModeRO`. After primary switchover, the primary replica
  may have a non-zero `walReplicaID`; its wrong-node failures must still use
  the legacy PChannel primary report so StreamingCoord can mark the RW
  assignment unavailable.
- Generated proto code and generated test mocks were updated for the new
  interface and request fields.

Commands run and passed:

```bash
source scripts/setenv.sh && make generated-proto
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingcoord/client/assignment -run '^TestAssignmentDiscoverClientReportWALReplicaAssignmentError$' -count=1 -timeout 60s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingcoord/client/assignment -run '^TestAssignmentDiscoverClientReportsReadWriteWALReplicaErrorAsPChannelError$' -count=1 -timeout 60s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/client/handler -run '^TestHandlerClientReportsWALReplicaAssignmentError$' -count=1 -timeout 60s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingnode/client/handler -run '^TestHandlerClientReportsReadWriteWALReplicaErrorAsPChannelError$' -count=1 -timeout 60s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/streamingcoord/server/balancer/channel ./internal/streamingcoord/server/service/discover ./internal/streamingcoord/client/assignment ./internal/streamingnode/client/handler -count=1 -timeout 300s
```

```bash
source scripts/setenv.sh && go test -p 1 -tags 'test,dynamic' -gcflags='all=-N -l' $(git diff --name-only -- '*.go' | xargs -r dirname | sort -u | sed 's#^#./#') -count=1 -timeout 300s 2>&1 | tee /tmp/qv_changed_pkgs_p1.log
```

```bash
git diff --check
```

The first changed-package run failed because `querycoordv2`'s local
`fakeRuntimeAssignmentService` had not yet implemented the new
`ReportWALReplicaAssignmentError` method. The fake was updated and the second
changed-package run passed.

## Read-Only WAL Replica Release Progress

This stage added the release side of QueryView-owned read-only WAL replica
demand and tightened final deletion safety:

- `ChannelManager.RemoveWALReplicas` now rechecks the registered QueryView WAL
  replica dependency provider immediately before deleting a dropping WAL
  replica entry. This protects the race where a replica is marked `DROPPING`,
  but a pending or Up QueryView dependency appears before the final remove CAS.
- `balancerImpl.ReleaseReadOnlyWALReplica` now implements the explicit
  StreamingCoord release flow for non-primary read-only WAL replicas:
  record the current active runtime assignment, mark the replica `DROPPING`,
  send `Remove` to the active StreamingNode with the old runtime
  `assignment_epoch`, then remove the replica entry from the single PChannel
  meta key.
- The release flow does not advance the PChannel write `Term` and does not
  remove the primary WAL replica.
- `StreamingNodeManager` exposes `ReleaseReadOnlyWALReplica`, and QueryCoord's
  `streamingCoordWALReplicaDemandExecutor` forwards QueryView release intents
  through that API.
- QueryView balance plans now carry `WALReplicaReleases`. The default policy
  emits releases for serviceable `AccessModeRO` WAL replicas that are not
  referenced by current shard WAL dependencies or by QueryViews prepared in the
  same reconcile cycle. StreamingCoord remains the final dependency gate,
  including pending sync dependencies that are not visible in the balancer
  snapshot.
- Existing mock balancer code was updated for the new release method.

Commands run and passed:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer/channel -run TestChannelManagerRejectsRemovingDroppingWALReplicaWithQueryViewDependency -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer -run TestBalancerReleaseReadOnlyWALReplicaRemovesActiveRuntimeAndMeta -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/coordinator/snmanager -run TestStreamingNodeManagerReleaseReadOnlyWALReplica -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/views/coord/balancer -run 'TestBalancer_ReconcileAppliesWALReplicaRelease|TestDefaultBalancePolicy_ReleasesUnusedReadOnlyWALReplica' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'TestStreamingCoordWALReplicaDemandExecutorDelegatesReleaseToStreamingNodeManager' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer/channel -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/views/coord/balancer -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/coordinator/snmanager -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'Test(StreamingCoordWALReplicaDemandExecutor|MaybeSwitchWALPrimaryReplicaForShardUp|NewQViewsRuntime|QViewsRuntimeStartRegistersStreamingCoordProviders|QViewsWALReplicaDependencyProvider)' -count=1 -timeout 180s
```

Follow-up changed-package rerun:

```bash
source scripts/setenv.sh && go test -p 1 -tags 'test,dynamic' -gcflags='all=-N -l' $(git diff --name-only -- '*.go' | xargs -r dirname | sort -u | sed 's#^#./#') -count=1 -timeout 300s 2>&1 | tee /tmp/qv_changed_pkgs_release_rerun.log
```

This rerun reported a remaining `internal/querycoordv2` test failure in
`TestStreamingCoordWALReplicaDemandExecutorDelegatesToStreamingNodeManager`:
the old ensure-path test still expects `WatchChannelAssignments` to be called
unconditionally. The related release-path test already treats that watch as
optional, which matches the runtime path when the assignment watcher is not
started by this unit test.

```bash
git diff --check -- docs/plans/wal-replica-progress-2026-07-31.md
```

Passed.

## Read-Only WAL Replica Release Follow-Up

This follow-up closed two gaps found during the current-state audit:

- `ReleaseReadOnlyWALReplica` now collects cleanup assignments for both
  `ActiveNode` and `TargetNode` before marking the replica `DROPPING`. This
  covers cancellation of an `AccessModeRO` make-before-break migration and
  prevents half-prepared target runtimes from being leaked.
- `mutablePChannel.MarkWALReplicaAsDropping` now permits non-primary
  read-only replicas in `ASSIGNING` to enter `DROPPING`; the target remains
  persisted until final metadata removal, so recovery still has an explicit
  cleanup target.
- The active cleanup uses the matching assignment history epoch when present,
  while the target cleanup uses the current assignment epoch.
- `internal/distributed/streaming`'s balancer test fixture now treats the
  background `StreamingNodeManager` assignment watcher as optional/reusable and
  closes the global manager at test end. This removes a mock panic exposed by
  the changed-package run.

Commands run and passed:

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' -gcflags='all=-N -l' ./internal/querycoordv2 -run 'TestStreamingCoordWALReplicaDemandExecutorDelegates(ToStreamingNodeManager|ReleaseToStreamingNodeManager)' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer -run 'TestBalancerReleaseReadOnlyWALReplica(RemovesActiveRuntimeAndMeta|CleansActiveAndTargetRuntime)' -count=1 -timeout 120s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer/channel -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/streamingcoord/server/balancer -count=1 -timeout 180s
```

```bash
source scripts/setenv.sh && go test -tags 'test,dynamic' ./internal/distributed/streaming -count=1 -timeout 120s
```

```bash
set -o pipefail && source scripts/setenv.sh && go test -p 1 -tags 'test,dynamic' -gcflags='all=-N -l' $(git diff --name-only -- '*.go' | xargs -r dirname | sort -u | sed 's#^#./#') -count=1 -timeout 300s 2>&1 | tee /tmp/qv_changed_pkgs_release_rerun4.log
```

```bash
git diff --check
```

Passed.
