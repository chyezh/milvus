# Two-Phase Query Process Design

## 1. Overview

This document describes the implementation design for the Two-Phase Query Process
defined in the [Distributed Query View Design](distributed_query_view_design.md) (Section 3).

The core idea: Proxy obtains a query plan from StreamingNode (Phase 1), then dispatches
queries directly to StreamingNode and QueryNodes in parallel (Phase 2), and reduces all
results itself. This eliminates the Delegator as a single-point bottleneck.

### Current Flow

```
Client → Proxy → QueryNode (ShardLeader/Delegator)
              → fan-out to workers → reduce at Delegator
         → Proxy reduces across shards → Client
```

### New Flow

```
Client → Proxy → StreamingNode: Phase 1 (get query plan)
              → StreamingNode + QueryNodes: Phase 2 (execute, parallel)
         → Proxy streaming reduce → Client
```

## 2. Architecture

### 2.1 Service Separation

Two new gRPC services are introduced, both on the **data plane**, separate from the
existing `ViewSyncService` (control plane, Coord→Node):

- **QueryPlanService** (Phase 1): Only implemented by StreamingNode. Generates a
  shard-level query plan containing a consistent snapshot reference (view version,
  MVCC timestamp, work node list) and runs global optimizers.

- **ViewQueryService** (Phase 2): Implemented by both StreamingNode and QueryNode.
  Provides Search, Query, and Requery operations. Each node determines which segments
  to execute based on the view version.

All proto definitions are in [view.proto](../../pkg/proto/view.proto).

### 2.2 Error Handling

Errors are transported via gRPC status details (not embedded in response messages),
following the same pattern as `StreamingCode` / `StreamingError`. Key error codes:

- **VIEW_INVALIDATED**: View version is no longer valid (Down/Dropped) → retry from Phase 1.
- **VIEW_NOT_FOUND**: View version not found on this node → retry from Phase 1.
- **ON_SHUTDOWN**: Node is shutting down → retry from Phase 1 (SN may have moved).

### 2.3 Plan Reuse

A QueryPlan is a **reusable snapshot reference**. Proxy can issue multiple Phase 2 calls
against the same plan (same version + work nodes) as long as the view remains valid.
This naturally supports requery:

```
Phase 1:  Get query plan → plan with version V
Phase 2a: Search(version=V) → PKs + scores (no output fields)
          Proxy reduces → final top-k PKs
Phase 2b: Requery(version=V, PKs) → output fields for final PKs only
          Proxy merges → return to client
```

This reduces bandwidth by deferring output field retrieval until after reduce.
If the view is invalidated between 2a and 2b, the client retries from Phase 1.

## 3. Optimizer Framework

Request rewriting logic currently in the Delegator is split into two levels:

### 3.1 Global Optimizer

Runs during **Phase 1** on StreamingNode. Has access to global information (all segments
across all nodes via the query view).

| Optimizer | Description |
|---|---|
| BM25 IDF | Compute avgdl + IDF vectors from global segment stats, transform query vectors and plan |
| Search Param Tuning | Adjust topk/search params based on global segment count (QueryHook) |

SN loads BM25 statistics during the view lifecycle (Preparing→Ready). Both optimizers
are initially placeholder interfaces.

### 3.2 Local Optimizer

Runs during **Phase 2** on each node (SN/QN). Has access to local segment information only.

| Optimizer | Description |
|---|---|
| Segment Pruning | Prune segments based on local partition statistics |

Initially a placeholder interface.

## 4. Client Architecture

An independent internal query client in `internal/views/queryclient/`, decoupled from Proxy.
Future Proxy integration will use this client as a backend.

### 4.1 Layers

- **ViewQueryClient**: Top-level interface exposing `Search` and `Query`. Resolves
  collection → shards, orchestrates Phase 1 + Phase 2 across all shards, and performs
  streaming reduce.

### 4.2 Dependencies

All dependencies are injected as interfaces:

- **QueryPlanServiceResolver**: Resolves vchannel → QueryPlanServiceClient via WAL binding
  to find the owning StreamingNode.
- **ViewQueryServiceResolver**: Resolves a work node → ViewQueryServiceClient.
- **ShardResolver**: Resolves collection → list of ShardIDs.

### 4.3 Streaming Reducer

A **shard-aware streaming reducer** that processes results incrementally as they arrive
from work nodes. Key properties:

- **Thread-safe**: `Add` can be called concurrently from multiple goroutines.
- **Per-shard rollback**: `ResetShard` discards a single shard's accumulated results
  in O(1), enabling shard-level retry without discarding other shards' results.
- **Eager reduce**: Each per-shard sub-reducer maintains only top-k entities at all
  times. When a new result arrives via `Add`, it is immediately merged into the
  sub-reducer's top-k, and excess entries are discarded. This bounds memory to
  N × top-k (N = number of shards, typically 2–16) regardless of how many work
  nodes contribute results. `Finish` performs a final cross-shard top-k merge.

Both `SearchResultReducer` and `RetrieveResultReducer` follow this pattern.

### 4.4 Execution Flow

```
Search(req):
  1. Resolve collection → shards
  2. For all shards concurrently: Phase 1 (get query plan from SN)
  3. Create streaming reducer
  4. For all work nodes across all plans concurrently: Phase 2 (execute)
     → Each result feeds into reducer.Add(shardID, result)
     → On VIEW_INVALIDATED: reducer.ResetShard, retry that shard from Phase 1
  5. reducer.Finish() → final result
  6. Optional requery: reuse same plans for output field retrieval
```

### 4.5 Retry Strategy

- **Scope**: Per-shard. Only the failed shard retries from Phase 1; other shards'
  results are preserved.
- **Max retries**: Configurable (default 3).
- **Timeout**: Shared with the overall request context.
- **On retry**: `ResetShard` discards stale results before re-executing.

## 5. Node-Side Implementation

### 5.1 StreamingNode

Implements both QueryPlanService and ViewQueryService.

**Phase 1 (GetQueryPlan):**
1. Find the latest Up-state query view for the requested shard.
2. Generate MVCC timestamp based on consistency level (from WAL).
3. Run Global Optimizers on the request.
4. Build work node list from the query view (SN itself + all QNs).
5. Return query plan with optimized request.

**Phase 2 (Search/Query/Requery):**
1. Validate view version exists and is Up/UpRecovering.
2. Delegate to **SearchScheduler** for execution (see Section 5.3).

### 5.2 QueryNode

Implements ViewQueryService only.

**Phase 2 (Search/Query/Requery):**
1. Validate view version exists and is Ready.
2. Delegate to **SearchScheduler** for execution (see Section 5.3).

### 5.3 SearchScheduler

After view version validation, both SN and QN delegate Phase 2 execution to a
per-node **SearchScheduler**. The scheduler is responsible for:

1. **Local Optimization**: Runs LocalOptimizer (segment pruning) on the node's
   local segments under the given view version.
2. **Segment-level scheduling**: Breaks the query into per-segment tasks and
   manages their concurrent execution with resource-aware scheduling.
3. **MVCC confirmation**: Ensures each segment's data is consistent up to the
   requested MVCC timestamp before executing queries against it.
4. **Stream reduce**: Incrementally reduces results across segments as they
   complete, maintaining only top-k entries to bound memory usage.
5. Returns the final reduced result for this node.

## 6. Package Layout

```
internal/views/
├── queryclient/
│   ├── client.go              # ViewQueryClient implementation
│   ├── resolver.go            # Dependency interfaces
│   ├── reducer.go             # Streaming reducers
│   └── retry.go               # Shard-level retry logic
├── optimizer/
│   ├── global.go              # GlobalOptimizer interface + no-op placeholder
│   └── local.go               # LocalOptimizer interface + no-op placeholder
├── viewerror/
│   ├── error.go               # ViewError type (parallels StreamingError)
│   └── rpc_error.go           # gRPC status ↔ ViewError conversion
├── coordview/                 # (existing) Coord-side view management
├── nodeview/                  # (existing) Node-side view management
└── qviews/                    # (existing) Shared types
```
