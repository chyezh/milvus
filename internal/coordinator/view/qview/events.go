package qview

type EventType int

type Event interface {
	ReplicaID() int64

	Describe() string
}

type IncomingViewEvent struct{}

type PersistedEvent struct{}

type WorkNodeAckedEvent struct{}

// NewEvent creates a new event.
func NewEvent(op func() *EventPoller) (*EventWaker, *EventPoller) {
	ch := make(chan struct{})
	return &EventWaker{waker: ch}, &EventPoller{
		waker: ch,
		op:    op,
	}
}

// EventWaker is the waker for the event.
type EventWaker struct {
	waker chan<- struct{}
}

// Done closes the notifier.
func (en EventWaker) Done() {
	close(en.waker)
}

// EventPoller is the poller for the event.
type EventPoller struct {
	waker <-chan struct{}
	op    func() *EventPoller
}

// Execute block and execute the event.
func (e *EventPoller) Execute() *EventPoller {
	<-e.waker
	return e.op()
}

//var ErrPending = errors.New("pending")
//
//type Event interface {
//	// Poll polls the event, when the event is ready, it will return nil.
//	// Otherwise, it will be return ErrPending.
//	Poll() error
//}
//
//// newPreparingEvent creates a new preparing event.
//func newPreparingEvent(oldPreparing, newPreparing *QueryViewAtCoord, syncer CoordSyncer) Event {
//	if oldPreparing != nil && oldPreparing.State() != QueryViewStateUnrecoverable {
//		panic("old view must be a unrecoverable view")
//	}
//	if newPreparing == nil || newPreparing.State() != QueryViewStatePreparing {
//		panic("new view must be a preparing view")
//	}
//	return &QueryViewAtCoordWhenPreparing{
//		oldPreparing: oldPreparing,
//		newPreparing: newPreparing,
//		syncer:       syncer,
//	}
//}
//
//// QueryViewAtCoordWhenPreparing is the event to sync the query view at coord when preparing.
//type QueryViewAtCoordWhenPreparing struct {
//	oldPreparing *QueryViewAtCoord // oldView is a old unrecoverable view, it must be sync with the newView at same worknode at the same time.
//	newPreparing *QueryViewAtCoord // newView is a new incoming preparing view.
//	syncer       CoordSyncer
//}
//
//// Poll polls the event, when the event is ready, it will return nil.
//func (e *QueryViewAtCoordWhenPreparing) Poll() error {
//	if err := e.syncer.SyncQueryView(&SyncTxn{
//		views: []*QueryViewAtCoord{e.oldPreparing, e.newPreparing},
//	}); err != nil {
//		return ErrPending
//	}
//	return nil
//}
//
