package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func replicaBootstrapSnapshot() *streamingpb.WALReplicaSnapshot {
	return &streamingpb.WALReplicaSnapshot{
		Cursor:        &streamingpb.WALReplicaCursor{Term: 1, Seq: 42},
		Checkpoint:    readOnlyTestConfig().Checkpoint.IntoProto(),
		Observed:      (&WALCheckpoint{MessageID: walimplstest.NewTestMessageID(10), TimeTick: 10}).IntoProto(),
		Vchannels:     []*streamingpb.VChannelMeta{{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL}},
		QueryBarriers: map[string]uint64{"v1": 20, "removed": 20},
	}
}

func TestReplicaBootstrapReplaysBeforeScopedQueryProgress(t *testing.T) {
	image := replicaBootstrapSnapshot()
	entries := []message.ImmutableMessage{newAckTestTimeTickMessage(t, 1, 1), newRecoveryTestDeleteMessage(t, "v1", 10)}
	// The replay contract accepts assembled transactions. Fragments below C
	// are assembled by the scanner, never discarded by a raw TimeTick filter.
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTimeTick(1).WithTxnContext(message.TxnContext{TxnID: 7}).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTimeTick(5).WithTxnContext(message.TxnContext{TxnID: 7}).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(5))
	txn, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	entries = append(entries[:1], txn, entries[1])
	index := 0
	scheduler := &recordingAckTaskScheduler{}
	ro, err := BootstrapReadOnlyRecoveryStorage(t.Context(), readOnlyTestConfig(), image, func(context.Context) (message.ImmutableMessage, error) {
		msg := entries[index]
		index++
		return msg, nil
	}, WithNodeScheduler(scheduler))
	require.NoError(t, err)
	defer ro.Close()
	require.Equal(t, 3, index)
	require.Equal(t, walevent.Cursor{Term: 1, Seq: 42}, ro.Applied())
	require.EqualValues(t, 10, ro.Metrics().RecoveryTimeTick)
	deletes, err := ro.summaryManager.ReadTransformEntries(t.Context(), "v1", 0, 20)
	require.NoError(t, err)
	require.Len(t, deletes, 1)
	require.Empty(t, scheduler.snapshot())
	require.Empty(t, ro.persistNotifier)
	require.Nil(t, ro.ackTracker)
	require.NoError(t, ro.Apply(t.Context(), walevent.Event{Cursor: walevent.Cursor{Term: 1, Seq: 43}, Payload: walevent.WALEntry{Message: newAckTestTimeTickMessage(t, 30, 30)}}))
	require.EqualValues(t, 30, ro.Metrics().RecoveryTimeTick)
}

func TestReplicaBootstrapRejectsIncompleteAndDifferentLog(t *testing.T) {
	failure := merr.WrapErrServiceUnavailable("retained WAL was lost")
	tests := []struct {
		name     string
		messages []message.ImmutableMessage
	}{
		{"read failure", nil},
		{"early stream end", []message.ImmutableMessage{nil}},
		{"different upper boundary", []message.ImmutableMessage{newAckTestTimeTickMessage(t, 10, 9)}},
		{"skipped upper boundary", []message.ImmutableMessage{newAckTestTimeTickMessage(t, 11, 11)}},
		{"out of order", []message.ImmutableMessage{newAckTestTimeTickMessage(t, 5, 5), newAckTestTimeTickMessage(t, 4, 4)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := 0
			ro, err := BootstrapReadOnlyRecoveryStorage(t.Context(), readOnlyTestConfig(), replicaBootstrapSnapshot(), func(context.Context) (message.ImmutableMessage, error) {
				if index == len(test.messages) {
					return nil, failure
				}
				msg := test.messages[index]
				index++
				return msg, nil
			})
			require.Error(t, err)
			require.Nil(t, ro)
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ro, err := BootstrapReadOnlyRecoveryStorage(ctx, readOnlyTestConfig(), replicaBootstrapSnapshot(), nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ro)
	image := replicaBootstrapSnapshot()
	image.Observed = proto.CloneOf(image.Checkpoint)
	ro, err = BootstrapReadOnlyRecoveryStorage(t.Context(), readOnlyTestConfig(), image, nil)
	require.NoError(t, err)
	ro.Close()
	image.Storage = &streamingpb.WALReplicaStoragePublication{Segments: []*streamingpb.SegmentAssignmentMeta{{Vchannel: "v1", SegmentId: 1}}}
	ro, err = BootstrapReadOnlyRecoveryStorage(t.Context(), readOnlyTestConfig(), image, nil)
	require.Error(t, err)
	require.Nil(t, ro)
	image.Storage = &streamingpb.WALReplicaStoragePublication{Summary: &streamingpb.PChannelSummaryManifest{Coverage: &streamingpb.SummaryCoverage{StartTimeTick: 2, EndTimeTick: 1}}}
	ro, err = BootstrapReadOnlyRecoveryStorage(t.Context(), readOnlyTestConfig(), image, nil)
	require.Error(t, err)
	require.Nil(t, ro)
}

func TestReplicaBootstrapValidatesMetadataBeforeAllocatingRuntime(t *testing.T) {
	for _, mutate := range []func(*streamingpb.WALReplicaSnapshot){
		func(s *streamingpb.WALReplicaSnapshot) { s.Cursor.Term = 2 },
		func(s *streamingpb.WALReplicaSnapshot) { s.Observed.LogGeneration++ },
		func(s *streamingpb.WALReplicaSnapshot) { s.Checkpoint = nil },
		func(s *streamingpb.WALReplicaSnapshot) { s.Checkpoint.TimeTick = 11 },
		func(s *streamingpb.WALReplicaSnapshot) { s.Checkpoint.MessageId.Id = "bad" },
		func(s *streamingpb.WALReplicaSnapshot) { s.Observed.MessageId.Id = "bad" },
		func(s *streamingpb.WALReplicaSnapshot) {
			s.Observed.MessageId = walimplstest.NewTestMessageID(0).IntoProto()
		},
		func(s *streamingpb.WALReplicaSnapshot) { s.Observed.TimeTick = s.Checkpoint.TimeTick },
		func(s *streamingpb.WALReplicaSnapshot) {
			s.Vchannels = append(s.Vchannels, proto.CloneOf(s.Vchannels[0]))
		},
		func(s *streamingpb.WALReplicaSnapshot) {
			s.Segments = []*streamingpb.SegmentAssignmentMeta{{SegmentId: 1}, {SegmentId: 1}}
		},
	} {
		snapshot := replicaBootstrapSnapshot()
		mutate(snapshot)
		ro, err := BootstrapReadOnlyRecoveryStorage(t.Context(), readOnlyTestConfig(), snapshot, nil)
		require.Error(t, err)
		require.Nil(t, ro)
	}
	require.Error(t, walevent.ValidateSnapshot(0, nil))
}
