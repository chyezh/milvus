package coordview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var _ syncer.QueryViewAtWorkNodeWithAck = (*queryViewAtWorkNodeWithAckImpl)(nil)

// queryViewAtWorkNodeWithAckImpl is the implementation of QueryViewAtWorkNodeWithAck.
type queryViewAtWorkNodeWithAckImpl struct {
	qviews.QueryViewAtWorkNode
	expectedStates          []qviews.QueryViewState
	expectStateWhenNodeDown qviews.QueryViewState
}

func (qv *queryViewAtWorkNodeWithAckImpl) WhenNodeDown() qviews.QueryViewAtWorkNode {
	if qv.expectStateWhenNodeDown == qviews.QueryViewStateNil {
		panic("WhenNodeDown should never be called")
	}
	proto := qv.IntoProto()
	proto.Meta.State = viewpb.QueryViewState(qv.expectStateWhenNodeDown)
	return qviews.NewQueryViewAtWorkNodeFromProto(proto)
}

func (qv *queryViewAtWorkNodeWithAckImpl) ObserveSyncerEvent(event events.SyncerEventAck) bool {
	for _, expectedState := range qv.expectedStates {
		if expectedState == event.AcknowledgedView.State() {
			return true
		}
	}
	return false
}
