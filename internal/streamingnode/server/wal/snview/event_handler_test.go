package snview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewIntentAppliesThroughEventSource(t *testing.T) {
	ctx := context.Background()
	local := &SNQueryViewHandler{}
	var applied []handler.ApplyView
	patch := mockey.Mock((*SNQueryViewHandler).ApplyViews).To(func(_ *SNQueryViewHandler, views []handler.ApplyView) {
		applied = append(applied, views...)
	}).Build()
	defer patch.UnPatch()
	source := walevent.NewSource(7, 10, 1<<20, func(ctx context.Context, e walevent.Event) error {
		ApplyQueryViewIntents(ctx, local, e.Payload.(walevent.QueryViewIntentBatch).Views)
		return nil
	})
	defer source.Close()
	r, err := source.Subscribe(walevent.Cursor{Term: 7})
	require.NoError(t, err)
	defer r.Close()
	reports := 0
	h := NewEventQueryViewHandler(ctx, source, local, func(err error) { t.Errorf("unexpected source failure: %v", err) })
	h.ApplyViews(nil)
	h.ApplyViews([]handler.ApplyView{{View: newPreparingSNView(1), OnReport: func(qviews.QueryViewAtWorkNode) { reports++ }}})
	require.Len(t, applied, 1)
	e, err := r.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, walevent.Cursor{Term: 7, Seq: 1}, e.Cursor)
	batch := e.Payload.(walevent.QueryViewIntentBatch)
	require.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, batch.Views[0].GetMeta().GetState())
	// The callback remains local and usable after the publishing call returns.
	applied[0].OnReport(applied[0].View)
	require.Equal(t, 1, reports)
	ApplyQueryViewIntents(ctx, local, batch.Views)
	require.Len(t, applied, 2)
	require.Nil(t, applied[1].OnReport, "replaying an event cannot copy a primary callback")
	ready := make(chan struct{})
	feedbackReady := mockey.Mock((*SNQueryViewHandler).ViewFeedbackReady).Return((<-chan struct{})(ready)).Build()
	defer feedbackReady.UnPatch()
	feedback := mockey.Mock((*SNQueryViewHandler).TakeViewFeedback).Return(qviews.ViewFeedbackWindow{}).Build()
	defer feedback.UnPatch()
	require.Equal(t, (<-chan struct{})(ready), h.ViewFeedbackReady())
	require.Equal(t, qviews.ViewFeedbackWindow{}, h.TakeViewFeedback())
}

func TestQueryViewIntentRejectsFailedSource(t *testing.T) {
	failure := merr.WrapErrServiceUnavailable("source failed")
	source := walevent.NewSource(7, 1, 1024, func(context.Context, walevent.Event) error { return failure })
	defer source.Close()
	var reported qviews.QueryViewAtWorkNode
	var fatal error
	h := NewEventQueryViewHandler(context.Background(), source, &SNQueryViewHandler{}, func(err error) { fatal = err })
	h.ApplyViews([]handler.ApplyView{{View: newPreparingSNView(1), OnReport: func(view qviews.QueryViewAtWorkNode) { reported = view }}})
	require.ErrorIs(t, fatal, failure)
	require.Equal(t, qviews.QueryViewStateUnrecoverable, reported.State())
}
