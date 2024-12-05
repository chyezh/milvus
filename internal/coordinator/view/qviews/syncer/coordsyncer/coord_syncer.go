package coordsyncer

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer/service"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var _ syncer.CoordSyncer = (*coordSyncerImpl)(nil)

// NewCoordSyncer creates a new CoordSyncer.
func NewCoordSyncer() syncer.CoordSyncer {
	cs := &coordSyncerImpl{
		pendingAckViews:   newPendingsAckView(),
		pendingSentEvents: nil,
		clients:           make(map[qviews.WorkNode]*service.SyncClient),
		resumeChan:        make(chan qviews.WorkNode),
		nodeDownChan:      make(chan qviews.WorkNode),
		syncChan:          make(chan *syncer.SyncGroup),
		receiver:          make(chan []events.SyncerEvent),
	}
	go cs.loop()
	return cs
}

type coordSyncerImpl struct {
	pendingAckViews   *pendingsAckView
	pendingSentEvents []events.SyncerEvent

	clients          map[qviews.WorkNode]*service.SyncClient
	resumeChan       chan qviews.WorkNode
	nodeDownChan     chan qviews.WorkNode
	syncChan         chan *syncer.SyncGroup
	syncViewReceiver chan *syncView
	receiver         chan []events.SyncerEvent
}

type syncView struct {
	resp     *viewpb.SyncQueryViewsResponse
	workNode qviews.WorkNode
}

func (cs *coordSyncerImpl) Sync(g *syncer.SyncGroup) {
	cs.syncChan <- g
}

func (cs *coordSyncerImpl) loop() {
	for {
		var receiver chan []events.SyncerEvent
		if len(cs.pendingSentEvents) > 0 {
			receiver = cs.receiver
		} else {
			receiver = nil
		}

		select {
		case newGroup := <-cs.syncChan:
			cs.pendingAckViews.Add(newGroup)
			cs.dispatch(newGroup.Views)
		case node := <-cs.resumeChan:
			// When resuming happens on one node,
			// The underlying grpc stream is broken,
			// syncer should resync all the pending views for this node.
			views := cs.pendingAckViews.CollectResync(node)
			cs.syncWorkNode(node, views)
		case node := <-cs.nodeDownChan:
			// When the node is gone, the related client should be stopped.
			cs.clients[node].Close()
			delete(cs.clients, node)
			// And the pending views on this node should generate a query node down event.
			views := cs.pendingAckViews.CollectResync(node)
			cs.syncWorkNode(node, views)
		case receiver <- cs.pendingSentEvents:
			cs.pendingSentEvents = nil
		case syncView := <-cs.syncViewReceiver:
			for _, viewProto := range syncView.resp.QueryViews {
				view := qviews.NewQueryViewAtWorkNodeFromProto(viewProto)
				cs.addPendingEvent(events.SyncerEventAck{
					SyncerViewEventBase: events.NewSyncerViewEventBase(view.ShardID(), view.Version(), view.State()),
					AcknowledgedView:    view,
				})
			}
			if syncView.resp.BalanceAttributes != nil {
				cs.addPendingEvent(events.SyncerEventBalanceAttrUpdate{
					BalanceAttr: qviews.NewBalanceAttrAtWorkNodeFromProto(syncView.workNode, syncView.resp),
				})
			}
		}
	}
}

func (cs *coordSyncerImpl) Receiver() <-chan []events.SyncerEvent {
	return cs.receiver
}

func (cs *coordSyncerImpl) dispatch(viewOnNodes map[qviews.WorkNode][]syncer.QueryViewAtWorkNodeWithAck) {
	for node, views := range viewOnNodes {
		cs.syncWorkNode(node, views)
	}
}

func (cs *coordSyncerImpl) syncWorkNode(node qviews.WorkNode, views []syncer.QueryViewAtWorkNodeWithAck) {
	client, ok := cs.clients[node]
	if !ok {
		// client not exist, the related node is down or gone.
		// so generate a query view when node is down.
		for _, view := range views {
			downView := view.WhenNodeDown()
			cs.addPendingEvent(events.SyncerEventAck{
				SyncerViewEventBase: events.NewSyncerViewEventBase(view.ShardID(), view.Version(), downView.State()),
				AcknowledgedView:    downView,
			})
		}
		return
	}
	req := &viewpb.SyncQueryViewsRequest{
		QueryViews: make([]*viewpb.QueryViewOfShard, 0, len(views)),
	}
	for _, view := range views {
		req.QueryViews = append(req.QueryViews, view.IntoProto())
	}
	client.SyncAtBackground(req)
}

// addPendingEvent add the events to the pending list.
func (cs *coordSyncerImpl) addPendingEvent(ev events.SyncerEvent) {
	cs.pendingAckViews.Observe(ev)
	cs.pendingSentEvents = append(cs.pendingSentEvents, ev)
}
