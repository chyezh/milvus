package qview

type SyncTxn struct {
	views []*QueryViewOfShardAtCoord
}

// CoordSyncer is the interface to sync the query view at coord into the worknode.
type CoordSyncer interface {
	// SyncQueryView sync up the query view to the coord.
	// SyncQueryView do a async operation at background,
	// and make sure that the query view will be sent as much as possible (except node gone).
	// Once the target node is gone, a unrecoverable work node view will be returned from the receiver.
	SyncQueryView(txn *SyncTxn)

	// Receiver returns the channel to receive the query view from the coord.
	Receiver() <-chan *QueryViewOfShardAtWorkNode
}
