package qview

type Recovery interface {
	// SwapPreparing swaps the old preparing view with new preparing view.
	SwapPreparing(old *QueryViewOfShardAtCoord, new *QueryViewOfShardAtCoord) error

	// Save saves the recovery infos into underlying persist storage.
	// The operation should be executed atomically.
	Save(saved []*QueryViewOfShardAtCoord, deleted []*QueryViewOfShardAtCoord) error
}
