package snview

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type localViewReportsKey struct{}

// EventQueryViewHandler submits coordinator intent before changing local view
// state. Report callbacks are process-local input context, never event payload.
// Replica readiness continues to be driven by each local resource manager.
type EventQueryViewHandler struct {
	ctx     context.Context
	source  *walevent.Source
	local   handler.QueryViewHandler
	onFatal func(error)
}

func NewEventQueryViewHandler(ctx context.Context, source *walevent.Source, local handler.QueryViewHandler, onFatal func(error)) *EventQueryViewHandler {
	return &EventQueryViewHandler{ctx: ctx, source: source, local: local, onFatal: onFatal}
}

func (h *EventQueryViewHandler) ApplyViews(views []handler.ApplyView) {
	if len(views) == 0 {
		return
	}
	batch := walevent.QueryViewIntentBatch{Views: make([]*viewpb.QueryViewOfShard, len(views))}
	reports := make([]func(qviews.QueryViewAtWorkNode), len(views))
	for i, view := range views {
		batch.Views[i] = view.View.IntoProto()
		reports[i] = view.OnReport
	}
	ctx := context.WithValue(h.ctx, localViewReportsKey{}, reports)
	if _, err := h.source.Publish(ctx, batch); err != nil {
		reportUnrecoverable(views)
		h.onFatal(err)
	}
}

func (h *EventQueryViewHandler) TakeViewFeedback() qviews.ViewFeedbackWindow {
	return h.local.TakeViewFeedback()
}

func (h *EventQueryViewHandler) ViewFeedbackReady() <-chan struct{} {
	return h.local.ViewFeedbackReady()
}

// ApplyQueryViewIntents is called only by the ordered event applier. Remote
// events have no primary callbacks; their reports belong to the local replica.
func ApplyQueryViewIntents(ctx context.Context, local handler.QueryViewHandler, views []*viewpb.QueryViewOfShard) {
	reports, _ := ctx.Value(localViewReportsKey{}).([]func(qviews.QueryViewAtWorkNode))
	apply := make([]handler.ApplyView, len(views))
	for i, view := range views {
		apply[i].View = qviews.NewQueryViewAtWorkNodeFromProto(proto.CloneOf(view))
		if i < len(reports) {
			apply[i].OnReport = reports[i]
		}
	}
	local.ApplyViews(apply)
}
