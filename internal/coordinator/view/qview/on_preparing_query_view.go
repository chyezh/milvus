package qview

import (
	"context"
	"errors"
)

var ErrOnPreparingViewIsNotPersisted = errors.New("on preparing view is not persisted")

// onPreparingQueryView is the struct to store the preparing query view at coord.
// A preparing view is globally unique
type onPreparingQueryView struct {
	recovery     Recovery
	currentView  *QueryViewAtCoord // The on preparing view, the state of the view may be preparing or unrecoverable.
	previousView *QueryViewAtCoord // The old one unrecoverable view.
	persisted    bool
}

// Swap swaps the old preparing view with the new preparing view.
func (qvs *onPreparingQueryView) Swap(ctx context.Context, newQV *QueryViewAtCoord) error {
	// Check the old view current state, reject if the swap cannot be done.
	if qvs.currentView != nil {
		if !qvs.persisted {
			// If the old view is not persisted, the new view cannot be join.
			return ErrOnPreparingViewIsNotPersisted
		}

		switch qvs.currentView.State() {
		case QueryViewStateReady, QueryViewStateUp:
			// If the old view is ready, the new view cannot be join before it up.
			return ErrOnPreparingViewIsReady
		case QueryViewStatePreparing:
			// make the old view unrecoverable.
			qvs.currentView.UnrecoverableView()
		}
		qvs.previousView = qvs.currentView
		qvs.currentView = nil
	}
	// Submit a swap operation into the recovery module.
	qvs.recovery.SwapPreparing(ctx, qvs.previousView, newQV)
	qvs.currentView = newQV
	qvs.persisted = false
	return nil
}

// Reset resets the preparing query view.
func (qvs *onPreparingQueryView) Reset() {
	if qvs.currentView.State() != QueryViewStateUp {
		panic("Reset should always be called when current view is ready")
	}
	qvs.currentView = nil
}

// WhenPreparingPersisted is called when the preparing view is persisted.
func (qvs *onPreparingQueryView) WhenPreparingPersisted() (previous *QueryViewAtCoord, current *QueryViewAtCoord) {
	if qvs.previousView != nil {
		previous = qvs.previousView
		previous.DropView()
		qvs.previousView = nil
	}
	qvs.persisted = true
	return previous, qvs.currentView
}
