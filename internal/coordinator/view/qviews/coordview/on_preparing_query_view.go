package coordview

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/recovery"
)

var ErrOnPreparingViewIsNotPersisted = errors.New("on preparing view is not persisted")

// newEmptyOnPreparingQueryView creates a new empty on preparing query view.
func newEmptyOnPreparingQueryView(r recovery.RecoveryStorage) *onPreparingQueryView {
	return &onPreparingQueryView{
		recovery:     r,
		currentView:  nil,
		previousView: nil,
		persisted:    false,
	}
}

// onPreparingQueryView is the struct to store the preparing query view at coord.
// A preparing view is globally unique
type onPreparingQueryView struct {
	recovery     recovery.RecoveryStorage
	currentView  *queryViewAtCoord // The on preparing view, the state of the view may be preparing or unrecoverable.
	previousView *queryViewAtCoord // The old one unrecoverable view.
	persisted    bool
}

// Swap swaps the old preparing view with the new preparing view.
func (qvs *onPreparingQueryView) Swap(ctx context.Context, shardID qviews.ShardID, newQV *queryViewAtCoord) error {
	// Check the old view current state, reject if the swap cannot be done.
	if qvs.currentView != nil {
		if !qvs.persisted {
			// If the old view is not persisted, the new view cannot be join.
			return ErrOnPreparingViewIsNotPersisted
		}

		switch qvs.currentView.State() {
		case qviews.QueryViewStateReady, qviews.QueryViewStateUp:
			// If the old view is ready, the new view cannot be join before it up.
			return ErrOnPreparingViewIsReady
		case qviews.QueryViewStatePreparing:
			// make the old view unrecoverable.
			qvs.currentView.UnrecoverableView()
		}
		qvs.previousView = qvs.currentView
		qvs.currentView = nil
	}
	// Submit a swap operation into the recovery module.
	qvs.recovery.SwapPreparing(ctx, shardID, qvs.previousView.Proto(), newQV.Proto())
	qvs.currentView = newQV
	qvs.persisted = false
	return nil
}

// Reset resets the preparing query view.
func (qvs *onPreparingQueryView) Reset() {
	if qvs.currentView.State() != qviews.QueryViewStateUp {
		panic("Reset should always be called when current view is ready")
	}
	qvs.currentView = nil
}

// WhenSwapPersisted is called when the preparing view is persisted.
func (qvs *onPreparingQueryView) WhenSwapPersisted() (previous *queryViewAtCoord, current *queryViewAtCoord) {
	if qvs.previousView != nil {
		previous = qvs.previousView
		previous.DropView()
		qvs.previousView = nil
	}
	qvs.persisted = true
	return previous, qvs.currentView
}
