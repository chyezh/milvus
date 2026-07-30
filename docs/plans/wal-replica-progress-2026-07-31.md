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

