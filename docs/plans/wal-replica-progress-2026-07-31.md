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
