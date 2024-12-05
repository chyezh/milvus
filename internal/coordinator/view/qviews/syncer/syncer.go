package syncer

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// CoordSyncer is the interface to sync the query view at coord into the worknode.
type CoordSyncer interface {
	// Sync sync up the query view to the coord.
	// Sync do a async operation at background,
	// and make sure that the query view will be sent as much as possible (except node gone).
	// Once the target node is gone, a unrecoverable work node view will be returned from the receiver.
	Sync(views ...*viewpb.QueryViewOfShard)

	// Receiver returns the channel to receive the query view from the coord.
	Receiver() <-chan events.SyncerEvent
}

// NodeReporter is the interface to sync the query view at worknode into the coord.
type NodeReporter[ViewAtWorkNode qviews.QueryViewAtWorkNode, BalanceInfoAtWorkNode qviews.BalanceAttrAtWorkNode] interface {
	Report(views []ViewAtWorkNode, balanceInfo BalanceInfoAtWorkNode)

	Receiver() <-chan events.SyncerEvent
}
