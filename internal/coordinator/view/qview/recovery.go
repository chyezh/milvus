package qview

import "context"

// Recovery is the interface to recover the query view at coord.
type Recovery interface {
	// SwapPreparing swaps the old preparing view with new preparing view.
	SwapPreparing(ctx context.Context, old *QueryViewAtCoord, new *QueryViewAtCoord)

	// UpNewPerparingView remove the preparing view and make it as a up view.
	UpNewPreparingView(ctx context.Context, newUp *QueryViewAtCoord)

	// Save saves the recovery infos into underlying persist storage.
	// The operation should be executed atomically.
	Save(ctx context.Context, saved *QueryViewAtCoord)

	Delete(ctx context.Context, deleted *QueryViewAtCoord)
}
