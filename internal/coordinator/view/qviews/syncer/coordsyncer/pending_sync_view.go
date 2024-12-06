package coordsyncer

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
)

// We only sync the finial state of the query view.
// So the (Node, Version, ShardID) is the unique key to identify the query view to be sync.
type uniqueKey struct {
	NodeID  qviews.WorkNode
	Version qviews.QueryViewVersion
	ShardID qviews.ShardID
}

// newPendingsAckView creates a new pendingsAckView.
func newPendingsAckView() *pendingsAckView {
	return &pendingsAckView{
		indexes:  make(map[uniqueKey]syncer.QueryViewAtWorkNodeWithAck),
		pendings: make(map[qviews.WorkNode][]syncer.QueryViewAtWorkNodeWithAck),
	}
}

// pendingsAckView is the struct to store the pending query views that need to be synced.
type pendingsAckView struct {
	indexes  map[uniqueKey]syncer.QueryViewAtWorkNodeWithAck
	pendings map[qviews.WorkNode][]syncer.QueryViewAtWorkNodeWithAck
}

// SyncNewView syncs the new query view to the pending sync view.
func (v *pendingsAckView) Add(g syncer.SyncGroup) []events.SyncerEvent {
	evs := make([]events.SyncerEvent, 0, len(g.Views))
	for _, views := range g.Views {
		for _, view := range views {
			key := uniqueKey{
				NodeID:  view.WorkNode(),
				Version: view.Version(),
				ShardID: view.ShardID(),
			}
			if _, ok := v.indexes[key]; !ok {
				evs = append(evs, v.overwritePreviousState(key, view))
			} else {
				v.pendings[view.WorkNode()] = append(v.pendings[view.WorkNode()], view)
				v.indexes[key] = view
				evs = append(evs, events.SyncerEventSent{
					SyncerViewEventBase: events.NewSyncerViewEventBase(view.ShardID(), view.Version(), view.State()),
				})
			}
		}
	}
	return evs
}

func (v *pendingsAckView) Observe(ev events.SyncerEvent) {
	switch ev := ev.(type) {
	case events.SyncerEventAck:
		key := uniqueKey{
			NodeID:  ev.AcknowledgedView.WorkNode(),
			ShardID: ev.AcknowledgedView.ShardID(),
			Version: ev.AcknowledgedView.Version(),
		}
		if ack, ok := v.indexes[key]; ok {
			if isAck := ack.ObserveSyncerEvent(ev); isAck {
				v.remove(key)
			}
		}
	}
}

// CollectResync returns the query views that need to be resynced.
func (v *pendingsAckView) CollectResync(WorkNode qviews.WorkNode) []syncer.QueryViewAtWorkNodeWithAck {
	return v.pendings[WorkNode]
}

// remove removes the query view from the pending sync view.
func (v *pendingsAckView) remove(key uniqueKey) {
	views := v.pendings[key.NodeID]
	for idx, view := range views {
		if view.ShardID() == key.ShardID && view.Version() == key.Version {
			v.pendings[key.NodeID] = append(views[:idx], views[idx+1:]...)
			delete(v.indexes, key)
			if len(v.pendings[key.NodeID]) == 0 {
				delete(v.pendings, key.NodeID)
			}
			return
		}
	}
	panic("the view should always be in the pending list")
}

// overwritePreviousState overwrites the previous state of the query view.
func (v *pendingsAckView) overwritePreviousState(key uniqueKey, view syncer.QueryViewAtWorkNodeWithAck) events.SyncerEventOverwrite {
	for idx, previous := range v.pendings[view.WorkNode()] {
		if previous.ShardID() == view.ShardID() && previous.Version() == view.Version() {
			previousState := previous.State()
			v.indexes[key] = view
			v.pendings[view.WorkNode()][idx] = view
			return events.SyncerEventOverwrite{
				SyncerViewEventBase: events.NewSyncerViewEventBase(view.ShardID(), view.Version(), view.State()),
				PreviousState:       previousState,
			}
		}
	}
	panic("the view should always be in the pending list")
}
