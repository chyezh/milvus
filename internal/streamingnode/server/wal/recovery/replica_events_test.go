package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRecoveryEventsShareAppliedPrefix(t *testing.T) {
	ctx := context.Background()
	r := newTestRecoveryStorage(t, &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	defer r.closeRecoveryResources()
	reader, err := r.Events().Subscribe(walevent.Cursor{Term: 1})
	require.NoError(t, err)
	defer reader.Close()
	var queryCount int
	WithQueryViewEventApplier(func(context.Context, []*viewpb.QueryViewOfShard) error { queryCount++; return nil })(r)
	_, err = r.Events().Publish(ctx, walevent.WALEntry{Message: newAckTestTimeTickMessage(t, 2, 2)})
	require.NoError(t, err)
	require.EqualValues(t, 2, r.ackTracker.CompletedPoint().TimeTick)
	require.EqualValues(t, 1, r.GetCheckpoint(ctx).TimeTick, "observation is not persistence")
	_, err = r.Events().Publish(ctx, walevent.QueryViewIntentBatch{})
	require.NoError(t, err)
	require.Equal(t, 1, queryCount)
	cp := &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(2), TimeTick: 2, Term: 1}
	_, err = r.Events().Publish(ctx, walevent.CheckpointPublished{Checkpoint: cp.IntoProto()})
	require.NoError(t, err)
	require.EqualValues(t, 2, r.GetCheckpoint(ctx).TimeTick)
	for i := uint64(1); i <= 3; i++ {
		e, err := reader.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, i, e.Cursor.Seq)
	}
}

func TestRecoveryStoragePublicationDispatchAndFailure(t *testing.T) {
	r := newTestRecoveryStorage(t, &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	defer r.closeRecoveryResources()
	segment := mockey.Mock((*vchannel.PChannelRecoveryManager).ApplySegmentPublication).Return(nil).Build()
	defer segment.UnPatch()
	channel := mockey.Mock((*vchannel.PChannelRecoveryManager).ApplyVChannelPublication).Return(nil).Build()
	defer channel.UnPatch()
	payload := walevent.StoragePublished{
		Segments:  []*streamingpb.SegmentAssignmentMeta{{SegmentId: 1, Vchannel: "v1"}},
		VChannels: []*streamingpb.VChannelMeta{{Vchannel: "v1", TransformMaterializedTimeTick: 20}},
	}
	require.NoError(t, r.publishStorage(context.Background(), payload))
	require.Equal(t, 1, segment.Times())
	require.Equal(t, 1, channel.Times())
	failure := merr.WrapErrServiceInternalMsg("invalid publication")
	segment.UnPatch()
	segment = mockey.Mock((*vchannel.PChannelRecoveryManager).ApplySegmentPublication).Return(failure).Build()
	defer segment.UnPatch()
	require.ErrorIs(t, r.applyEvent(context.Background(), walevent.Event{Payload: payload}), failure)
	payload.Segments = nil
	channel.UnPatch()
	channel = mockey.Mock((*vchannel.PChannelRecoveryManager).ApplyVChannelPublication).Return(failure).Build()
	defer channel.UnPatch()
	require.ErrorIs(t, r.applyEvent(context.Background(), walevent.Event{Payload: payload}), failure)
	var fatal error
	r.onFatal = func(err error) { fatal = err }
	require.ErrorIs(t, r.publishStorage(context.Background(), payload), failure)
	require.ErrorIs(t, fatal, failure)
	require.ErrorIs(t, r.applyEvent(context.Background(), walevent.Event{Payload: walevent.StoragePublished{Summary: &streamingpb.PChannelSummaryManifest{}}}), merr.ErrServiceInternal)
	r.summaryManager = walsummary.NewManager(walsummary.ManagerConfig{ReadOnly: true})
	require.NoError(t, r.applyEvent(context.Background(), walevent.Event{Payload: walevent.StoragePublished{Summary: &streamingpb.PChannelSummaryManifest{}}}))
	require.Error(t, r.applyEvent(context.Background(), walevent.Event{Payload: walevent.StoragePublished{Summary: &streamingpb.PChannelSummaryManifest{
		Coverage: &streamingpb.SummaryCoverage{StartTimeTick: 2, EndTimeTick: 1},
	}}}))
}

func TestLiveStreamEndStopsReplicaSource(t *testing.T) {
	r := newTestRecoveryStorage(t, &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	defer r.closeRecoveryResources()
	var fatal error
	r.onFatal = func(err error) { fatal = err }
	r.runLiveScanner(&closedRecoveryStream{ch: make(chan message.ImmutableMessage)})
	require.ErrorIs(t, fatal, merr.ErrServiceUnavailable)
	_, err := r.Events().Subscribe(walevent.Cursor{Term: 1})
	require.Error(t, err)
}

func TestRecoveryEventRejectsMissingTarget(t *testing.T) {
	r := newTestRecoveryStorage(t, &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	defer r.closeRecoveryResources()
	err := r.applyEvent(context.Background(), walevent.Event{Payload: walevent.QueryViewIntentBatch{}})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	err = r.applyEvent(context.Background(), walevent.Event{})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
