package walevent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func codecTransaction(t *testing.T) message.ImmutableTxnMessage {
	t.Helper()
	txn := message.TxnContext{TxnID: 1, Keepalive: time.Minute}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txn).WithTimeTick(10).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	body := message.NewDeleteMessageBuilderV1().WithVChannel("p_1v0").
		WithHeader(&message.DeleteMessageHeader{}).WithBody(&message.DeleteRequest{}).
		MustBuildMutable().WithTxnContext(txn).WithTimeTick(20).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(3))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txn).WithTimeTick(30).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(4))
	result, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Add(body).Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	return result
}

func TestReplicaEventWireRoundTrip(t *testing.T) {
	txn := codecTransaction(t)
	barrier := message.NewMVCCBarrierMessageBuilderV2().WithAllVChannel().WithNotPersisted().
		WithHeader(&message.MVCCBarrierMessageHeader{Vchannels: []string{"p_1v0"}}).
		WithBody(&message.MVCCBarrierMessageBody{}).MustBuildMutable().WithTimeTick(40).
		WithLastConfirmed(walimplstest.NewTestMessageID(4)).IntoImmutableMessage(walimplstest.NewTestMessageID(4))
	for _, payload := range []Payload{
		WALEntry{Message: txn},
		WALEntry{Message: barrier},
		QueryViewIntentBatch{Views: []*viewpb.QueryViewOfShard{{Meta: &viewpb.QueryViewMeta{ReplicaId: 5}}}},
		StoragePublished{VChannels: []*streamingpb.VChannelMeta{{Vchannel: "p_1v0"}}, Segments: []*streamingpb.SegmentAssignmentMeta{{SegmentId: 8}}, Summary: &streamingpb.PChannelSummaryManifest{}, Catalog: &streamingpb.WALReplicaCatalogPublication{RemovedSegments: []*streamingpb.SegmentAssignmentMeta{{SegmentId: 8, Vchannel: "p_1v0"}}}},
		CheckpointPublished{Checkpoint: &streamingpb.WALCheckpoint{MessageId: walimplstest.NewTestMessageID(4).IntoProto(), TimeTick: 30}},
	} {
		event := Event{Cursor: Cursor{Term: 7, Seq: 11}, Payload: payload}
		wire := event.IntoProto()
		encoded, err := proto.Marshal(wire)
		require.NoError(t, err)
		frame := &streamingpb.WALReplicaEvent{}
		require.NoError(t, proto.Unmarshal(encoded, frame))
		decoded, err := EventFromProto(frame)
		require.NoError(t, err)
		require.True(t, proto.Equal(wire, decoded.IntoProto()))
		if entry, ok := decoded.Payload.(WALEntry); ok {
			if tx := message.AsImmutableTxnMessage(entry.Message); tx != nil {
				require.Equal(t, 1, tx.Size())
				require.NoError(t, tx.RangeOver(func(m message.ImmutableMessage) error {
					require.EqualValues(t, 30, m.TimeTick())
					require.True(t, txn.LastConfirmedMessageID().EQ(m.LastConfirmedMessageID()))
					return nil
				}))
			} else {
				require.False(t, entry.Message.IsPersisted())
				require.Equal(t, []string{"p_1v0"}, message.MustAsImmutableMVCCBarrierMessageV2(entry.Message).Header().GetVchannels())
			}
		}
		proto.Reset(frame)
		require.True(t, proto.Equal(wire, decoded.IntoProto()), "decoded event owns its frame data")
	}
}

func TestReplicaEventRejectsIncompleteWireInput(t *testing.T) {
	_, err := EventFromProto(nil)
	require.Error(t, err)
	_, err = EventFromProto(&streamingpb.WALReplicaEvent{Cursor: (&Cursor{Term: 7, Seq: 1}).IntoProto()})
	require.Error(t, err)
	base := Event{Cursor: Cursor{Term: 7, Seq: 1}, Payload: WALEntry{Message: codecTransaction(t)}}.IntoProto()
	for _, mutate := range []func(*streamingpb.WALReplicaEntry){
		func(e *streamingpb.WALReplicaEntry) { e.Message.Id = nil },
		func(e *streamingpb.WALReplicaEntry) { e.Begin = nil },
		func(e *streamingpb.WALReplicaEntry) { e.Commit = nil },
		func(e *streamingpb.WALReplicaEntry) { e.Body = nil },
		func(e *streamingpb.WALReplicaEntry) { e.Body[0] = nil },
		func(e *streamingpb.WALReplicaEntry) { e.Begin = e.Commit },
		func(e *streamingpb.WALReplicaEntry) { e.Commit = e.Begin },
		func(e *streamingpb.WALReplicaEntry) { e.Message.Id = walimplstest.NewTestMessageID(100).IntoProto() },
		func(e *streamingpb.WALReplicaEntry) { e.Message = e.Body[0] },
		func(e *streamingpb.WALReplicaEntry) { e.Body[0].Properties["_tx"] = "" },
	} {
		wire := proto.CloneOf(base)
		mutate(wire.GetWalEntry())
		_, err := EventFromProto(wire)
		require.Error(t, err)
	}
	_, err = EventFromProto(&streamingpb.WALReplicaEvent{Cursor: base.Cursor, Payload: &streamingpb.WALReplicaEvent_CheckpointPublished{
		CheckpointPublished: &streamingpb.WALCheckpoint{MessageId: &commonpb.MessageID{WALName: commonpb.WALName(-1)}},
	}})
	require.Error(t, err)
}
