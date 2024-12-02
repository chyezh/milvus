package qview

import "github.com/milvus-io/milvus/internal/proto/viewpb"

type (
	QueryViewState viewpb.QueryViewState

	NodeSyncState int // NodeSyncState marks the on-syncing node state.
)

const (
	QueryViewStatePreparing     = QueryViewState(viewpb.QueryViewState_QueryViewStatePreparing)
	QueryViewStateReady         = QueryViewState(viewpb.QueryViewState_QueryViewStateReady)
	QueryViewStateUp            = QueryViewState(viewpb.QueryViewState_QueryViewStateUp)
	QueryViewStateDown          = QueryViewState(viewpb.QueryViewState_QueryViewStateDown)
	QueryViewStateUnrecoverable = QueryViewState(viewpb.QueryViewState_QueryViewStateUnrecoverable)
	QueryViewStateDropping      = QueryViewState(viewpb.QueryViewState_QueryViewStateDropping)
	QueryViewStateDropped       = QueryViewState(viewpb.QueryViewState_QueryViewStateDropped)
)
