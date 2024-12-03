package qviews

import "github.com/milvus-io/milvus/internal/proto/viewpb"

type (
	QueryViewState viewpb.QueryViewState

	NodeSyncState int // NodeSyncState marks the on-syncing node state.
)

// Preparing -> Ready: If all worknodes are ready.
// Preparing -> Unrecoverable: If any worknode are unrecoverable, or the query view is deprecated when preparing by balancer.
// Ready -> Up: If the streamingnode is up.
// Up -> Down: By Down interface.
// Down -> Dropping: If the streamingnode is down.
// Unrecoverable -> Dropping: By DropView interface.
// Dropping -> Dropped: If all worknodes are dropped.
const (
	QueryViewStatePreparing     = QueryViewState(viewpb.QueryViewState_QueryViewStatePreparing)
	QueryViewStateReady         = QueryViewState(viewpb.QueryViewState_QueryViewStateReady)
	QueryViewStateUp            = QueryViewState(viewpb.QueryViewState_QueryViewStateUp)
	QueryViewStateDown          = QueryViewState(viewpb.QueryViewState_QueryViewStateDown)
	QueryViewStateUnrecoverable = QueryViewState(viewpb.QueryViewState_QueryViewStateUnrecoverable)
	QueryViewStateDropping      = QueryViewState(viewpb.QueryViewState_QueryViewStateDropping)
	QueryViewStateDropped       = QueryViewState(viewpb.QueryViewState_QueryViewStateDropped)
)

// ShardID is the unique identifier of a shard.
type ShardID struct {
	ReplicaID int64
	VChannel  string
}

func (s ShardID) GetShardID() ShardID {
	return s
}

// NewShardIDFromQVMeta creates a new shard id from the query view meta.
func NewShardIDFromQVMeta(meta *viewpb.QueryViewMeta) ShardID {
	return ShardID{
		ReplicaID: meta.ReplicaId,
		VChannel:  meta.Vchannel,
	}
}

// StateTransition is the transition of the query view state.
type StateTransition struct {
	From QueryViewState
	To   QueryViewState
}

// FromProtoQueryViewVersion converts a QueryViewVersion proto to a QueryViewVersion.
func FromProtoQueryViewVersion(qvv *viewpb.QueryViewVersion) QueryViewVersion {
	return QueryViewVersion{
		DataVersion:  qvv.DataVersion,
		QueryVersion: qvv.QueryVersion,
	}
}

type QueryViewVersion struct {
	DataVersion  int64
	QueryVersion int64
}

// GTE returns true if qv is greater than or equal to qv2.
func (qv QueryViewVersion) GTE(qv2 QueryViewVersion) bool {
	return qv.DataVersion > qv2.DataVersion ||
		(qv.DataVersion == qv2.DataVersion && qv.QueryVersion >= qv2.QueryVersion)
}

// GT returns true if qv is greater than qv2.
func (qv QueryViewVersion) GT(qv2 QueryViewVersion) bool {
	return qv.DataVersion > qv2.DataVersion ||
		(qv.DataVersion == qv2.DataVersion && qv.QueryVersion > qv2.QueryVersion)
}

// EQ returns true if qv is equal to qv2.
func (qv QueryViewVersion) EQ(qv2 QueryViewVersion) bool {
	return qv.DataVersion == qv2.DataVersion && qv.QueryVersion == qv2.QueryVersion
}
