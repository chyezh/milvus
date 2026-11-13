package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func readOnlyTestConfig() ReadOnlyConfig {
	return ReadOnlyConfig{
		Channel: types.PChannelInfo{Name: "test-pchannel", Term: 1, AccessMode: types.AccessModeRO},
		Applied: walevent.Cursor{Term: 1},
		Checkpoint: &WALCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1, Term: 1,
		},
		Modules: vchannel.PChannelManagerConfig{Logger: mlog.With()},
	}
}

func TestReadOnlyRecoveryResubscribesToTheSameAppliedRuntime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	primary := newTestRecoveryStorage(t, readOnlyTestConfig().Checkpoint)
	defer primary.closeRecoveryResources()
	var primaryViews, replicaViews int
	WithQueryViewEventApplier(func(context.Context, []*viewpb.QueryViewOfShard) error { primaryViews++; return nil })(primary)
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	ro, err := NewReadOnlyRecoveryStorage(readOnlyTestConfig(), WithNodeScheduler(scheduler),
		WithQueryViewEventApplier(func(context.Context, []*viewpb.QueryViewOfShard) error { replicaViews++; return nil }))
	require.NoError(t, err)
	defer ro.Close()
	require.Nil(t, ro.Events())
	require.Nil(t, ro.ackTracker)
	require.Nil(t, ro.broadcastAck)
	resources := ro.VChannelManager()
	reader, err := primary.Events().Subscribe(ro.Applied())
	require.NoError(t, err)
	_, err = primary.Events().Publish(ctx, walevent.WALEntry{Message: newAckTestTimeTickMessage(t, 2, 2)})
	require.NoError(t, err)
	event, err := reader.Next(ctx)
	require.NoError(t, err)
	require.NoError(t, ro.Apply(ctx, event))
	require.NoError(t, ro.Apply(ctx, event), "same-term duplicate does not observe twice")
	require.EqualValues(t, 2, ro.Metrics().RecoveryTimeTick)
	require.EqualValues(t, 1, ro.GetCheckpoint(ctx).TimeTick)
	reader.Close()
	_, err = primary.Events().Publish(ctx, walevent.QueryViewIntentBatch{})
	require.NoError(t, err)
	_, err = primary.Events().Publish(ctx, walevent.CheckpointPublished{Checkpoint: (&WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(2), TimeTick: 2, Term: 1,
	}).IntoProto()})
	require.NoError(t, err)
	reader, err = primary.Events().Subscribe(ro.Applied())
	require.NoError(t, err)
	defer reader.Close()
	for i := 0; i < 2; i++ {
		event, err = reader.Next(ctx)
		require.NoError(t, err)
		require.NoError(t, ro.Apply(ctx, event))
	}
	require.Equal(t, primaryViews, replicaViews)
	require.Equal(t, walevent.Cursor{Term: 1, Seq: 3}, ro.Applied())
	require.EqualValues(t, 2, ro.GetCheckpoint(ctx).TimeTick)
	require.Same(t, resources, ro.VChannelManager())
	require.Empty(t, ro.persistNotifier)
	ro.Close()
	require.ErrorIs(t, ro.Apply(ctx, event), merr.ErrServiceUnavailable)
}

func TestReadOnlyRecoveryAdmissionAndConstructionFailure(t *testing.T) {
	_, err := NewReadOnlyRecoveryStorage(ReadOnlyConfig{})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	cfg := readOnlyTestConfig()
	cfg.Manifest = &streamingpb.PChannelSummaryManifest{Coverage: &streamingpb.SummaryCoverage{StartTimeTick: 2, EndTimeTick: 1}}
	_, err = NewReadOnlyRecoveryStorage(cfg, WithNodeScheduler(scheduler))
	require.Error(t, err)
	cfg.Manifest = &streamingpb.PChannelSummaryManifest{}
	failure := merr.WrapErrServiceUnavailable("module initialization failed")
	patch := mockey.Mock(vchannel.NewPChannelRecoveryManager).Return(nil, failure).Build()
	defer patch.UnPatch()
	_, err = NewReadOnlyRecoveryStorage(cfg, WithNodeScheduler(scheduler))
	require.ErrorIs(t, err, failure)
	patch.UnPatch()
	ro, err := NewReadOnlyRecoveryStorage(cfg, WithNodeScheduler(scheduler))
	require.NoError(t, err)
	defer ro.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ro.Apply(ctx, walevent.Event{}), context.Canceled)
	// A new term is rejected before mutation. The existing read runtime stays
	// installed until its owner has reconciled the new primary's baseline.
	err = ro.Apply(context.Background(), walevent.Event{Cursor: walevent.Cursor{Term: 2, Seq: 1}, Payload: walevent.QueryViewIntentBatch{}})
	var reset *walevent.ResetError
	require.ErrorAs(t, err, &reset)
	require.Equal(t, cfg.Applied, ro.Applied())
}

func TestReadOnlyRecoveryKeepsDeletesAndScopedBarrierWithoutPersistence(t *testing.T) {
	ctx := context.Background()
	cfg := readOnlyTestConfig()
	cfg.Modules.VChannelMetas = map[string]*streamingpb.VChannelMeta{
		"v1": {Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	}
	cfg.Summary.FlushMaxBytes = 1
	scheduler := &recordingAckTaskScheduler{}
	ro, err := NewReadOnlyRecoveryStorage(cfg, WithNodeScheduler(scheduler))
	require.NoError(t, err)
	defer ro.Close()
	ro.VChannelManager().Start()
	messages := []message.ImmutableMessage{
		newRecoveryTestDeleteMessage(t, "v1", 10),
		message.NewMVCCBarrierMessageBuilderV2().
			WithHeader(&message.MVCCBarrierMessageHeader{Vchannels: []string{"v1"}}).
			WithBody(&message.MVCCBarrierMessageBody{}).WithAllVChannel().WithNotPersisted().
			MustBuildMutable().WithTimeTick(20).WithLastConfirmed(walimplstest.NewTestMessageID(10)).
			IntoImmutableMessage(walimplstest.NewTestMessageID(10)),
	}
	for i, msg := range messages {
		require.NoError(t, ro.Apply(ctx, walevent.Event{Cursor: walevent.Cursor{Term: 1, Seq: uint64(i + 1)}, Payload: walevent.WALEntry{Message: msg}}))
	}
	entries, err := ro.summaryManager.ReadTransformEntries(ctx, "v1", 0, 20)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.EqualValues(t, 10, entries[0].GetTimeTick())
	require.EqualValues(t, 10, ro.Metrics().RecoveryTimeTick, "a volatile barrier cannot advance durable observation")
	require.EqualValues(t, 1, ro.GetCheckpoint(ctx).TimeTick)
	require.Empty(t, scheduler.snapshot())
	require.Empty(t, ro.persistNotifier)
	require.False(t, ro.summaryManager.HasPendingWork())
}
