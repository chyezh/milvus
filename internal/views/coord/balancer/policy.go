package balancer

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// BalancePolicy consumes a BalancerSnapshot and the set of dirty shards
// flagged by the work queue, and returns an execution plan.
//
// The Policy sees the entire snapshot so it can coordinate across shards
// (e.g., avoid two shards contending for the same target node via a shared
// predicted-load tracker maintained internally during Plan).
//
// Implementations must be stateless: the snapshot and dirty list are the
// only inputs, and the BalancePlan is the only output. No side effects.
type BalancePolicy interface {
	Plan(snap *BalancerSnapshot, dirty []qviews.ShardID) *BalancePlan
}

// BalancePlan is the complete set of actions to execute for one reconcile
// batch. The Balancer applies Prepares (via AddPreparing) and Releases (via
// RequestRelease) in an unspecified order; both are idempotent operations on
// the per-shard ShardViewManager.
//
// A shard listed in neither Prepares nor Releases is implicitly a no-op for
// this batch.
type BalancePlan struct {
	// Prepares lists shards that should receive a new Preparing view.
	// The value is the builder the Balancer passes to AddPreparing.
	Prepares map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder

	// WALReplicaDemands lists read-only WAL replicas that must exist before
	// the corresponding QueryViews can be prepared in their resource group.
	WALReplicaDemands []WALReplicaDemand

	// WALReplicaReleases lists read-only WAL replicas that no current or
	// planned QueryView depends on and can be released by StreamingCoord.
	WALReplicaReleases []WALReplicaRelease

	// Releases lists shards whose existing views should be released
	// (desired state absent but current views still exist).
	Releases []qviews.ShardID
}

type WALReplicaDemand struct {
	PChannel      string
	ResourceGroup string
}

type WALReplicaRelease struct {
	PChannel     string
	WALReplicaID int64
}
