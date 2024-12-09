package coordsyncer

import (
	"errors"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer/client"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var _ syncer.CoordSyncer = (*coordSyncerImpl)(nil)

// NewCoordSyncer creates a new CoordSyncer.
func NewCoordSyncer() syncer.CoordSyncer {
	cs := &coordSyncerImpl{
		pendingAckViews:   newPendingsAckView(),
		pendingSentEvents: nil,

		serviceClient: nil,
		syncers:       make(map[qviews.WorkNode]client.QueryViewServiceSyncer),
		syncChan:      make(chan syncer.SyncGroup),
		eventReceiver: make(chan []events.SyncerEvent),
	}
	go cs.loop()
	return cs
}

type coordSyncerImpl struct {
	pendingAckViews     *pendingsAckView
	pendingSentEvents   []events.SyncerEvent
	serviceClient       client.QueryViewServiceClient
	syncers             map[qviews.WorkNode]client.QueryViewServiceSyncer
	syncChan            chan syncer.SyncGroup
	syncMessageReceiver chan client.SyncMessage
	eventReceiver       chan []events.SyncerEvent
}

func (cs *coordSyncerImpl) Sync(g syncer.SyncGroup) {
	// Empty group should be ignored.
	if len(g.Views) == 0 {
		panic("empty sync operation is never allowed, at least streaming node")
	}
	cs.syncChan <- g
}

func (cs *coordSyncerImpl) Receiver() <-chan []events.SyncerEvent {
	return cs.eventReceiver
}

// loop is the background loop to handle the sync events.
func (cs *coordSyncerImpl) loop() {
	for {
		var receiver chan []events.SyncerEvent
		if len(cs.pendingSentEvents) > 0 {
			receiver = cs.eventReceiver
		} else {
			receiver = nil
		}

		select {
		case newGroup := <-cs.syncChan:
			// When new sync group comes, the views should be dispatched right away,
			// and add it into pending view to wait for the ack.
			cs.pendingAckViews.Add(newGroup)
			cs.dispatch(newGroup.Views)
		case receiver <- cs.pendingSentEvents:
			cs.pendingSentEvents = nil
		case syncMessage := <-cs.syncMessageReceiver:
			cs.handleMessage(syncMessage)
		}
	}
}

// handleMessage handles the sync message from the syncer.
func (cs *coordSyncerImpl) handleMessage(syncMessage client.SyncMessage) {
	switch msg := syncMessage.(type) {
	case client.SyncResponseMessage:
		for _, viewProto := range msg.Response.QueryViews {
			view := qviews.NewQueryViewAtWorkNodeFromProto(viewProto)
			cs.addPendingEvent(events.SyncerEventAck{
				SyncerViewEventBase: events.NewSyncerViewEventBase(view.ShardID(), view.Version(), view.State()),
				AcknowledgedView:    view,
			})
		}
		if msg.Response.BalanceAttributes != nil {
			cs.addPendingEvent(events.SyncerEventBalanceAttrUpdate{
				BalanceAttr: qviews.NewBalanceAttrAtWorkNodeFromProto(msg.WorkNode(), msg.Response),
			})
		}
	case client.SyncErrorMessage:
		if errors.Is(msg.Error, client.ErrStreamBroken) {
			// When the stream is broken, the syncer should be refreshed and
			// the views should be resynced.
			cs.whenSyncerBroken(msg.WorkNode())
		} else if errors.Is(msg.Error, client.ErrNodeGone) {
			// When the node is gone, all the views should be unrecoverable.
			cs.whenWorkNodeIsDown(msg.WorkNode())
		}
	}
}

// dispatch dispatches the views to the work nodes.
func (cs *coordSyncerImpl) dispatch(viewOnNodes map[qviews.WorkNode][]syncer.QueryViewAtWorkNodeWithAck) {
	for node, views := range viewOnNodes {
		// Create a new syncer if the syncer is not exist.
		if _, ok := cs.syncers[node]; !ok {
			cs.syncers[node] = cs.serviceClient.Sync(client.SyncOption{
				WorkNode: node,
				Receiver: cs.syncMessageReceiver,
			})
		}
		cs.syncWorkNode(node, views)
	}
}

// syncWorkNode sync the views to the work node.
func (cs *coordSyncerImpl) syncWorkNode(node qviews.WorkNode, views []syncer.QueryViewAtWorkNodeWithAck) {
	syncer, ok := cs.syncers[node]
	if !ok {
		panic("syncer must always exist")
	}
	req := &viewpb.SyncQueryViewsRequest{
		QueryViews: make([]*viewpb.QueryViewOfShard, 0, len(views)),
	}
	for _, view := range views {
		req.QueryViews = append(req.QueryViews, view.IntoProto())
	}
	syncer.SyncAtBackground(req)
}

// whenWorkNodeIsDown handles the situation when the work node is down.
func (cs *coordSyncerImpl) whenWorkNodeIsDown(node qviews.WorkNode) {
	views := cs.pendingAckViews.CollectResync(node)
	// client not exist, the related node is down or gone.
	// so generate a query view when node is down.
	for _, view := range views {
		downView := view.WhenNodeDown()
		cs.addPendingEvent(events.SyncerEventAck{
			SyncerViewEventBase: events.NewSyncerViewEventBase(view.ShardID(), view.Version(), downView.State()),
			AcknowledgedView:    downView,
		})
	}
	client, ok := cs.syncers[node]
	if !ok {
		return
	}
	client.Close()
	delete(cs.syncers, node)
}

// whenSyncerBroken refresh the syncer for the work node and resync the related views.
func (cs *coordSyncerImpl) whenSyncerBroken(node qviews.WorkNode) {
	if client, ok := cs.syncers[node]; ok {
		client.Close()
		delete(cs.syncers, node)
	}
	cs.syncers[node] = cs.serviceClient.Sync(client.SyncOption{
		WorkNode: node,
		Receiver: cs.syncMessageReceiver,
	})
	views := cs.pendingAckViews.CollectResync(node)
	cs.syncWorkNode(node, views)
}

// addPendingEvent add the events to the pending list.
func (cs *coordSyncerImpl) addPendingEvent(ev events.SyncerEvent) {
	cs.pendingAckViews.Observe(ev)
	cs.pendingSentEvents = append(cs.pendingSentEvents, ev)
}
