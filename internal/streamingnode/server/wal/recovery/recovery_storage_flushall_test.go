package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// buildFlushAllOnControlChannel builds a FlushAll ImmutableMessage for the control channel
// using WithClusterLevelBroadcast + SplitIntoMutableMessage, which mirrors the production path.
func buildFlushAllOnControlChannel(pchannel string, controlChannel string, broadcastID uint64, timeTick uint64, lastConfirmedID, msgID int64) message.ImmutableMessage {
	cc := message.ClusterChannels{
		Channels:       []string{pchannel},
		ControlChannel: controlChannel,
	}
	broadcastMsg := message.NewFlushAllMessageBuilderV2().
		WithHeader(&message.FlushAllMessageHeader{}).
		WithBody(&message.FlushAllMessageBody{}).
		WithClusterLevelBroadcast(cc).
		MustBuildBroadcast()
	broadcastMsg.WithBroadcastID(broadcastID)
	splitMsgs := broadcastMsg.SplitIntoMutableMessage()
	// Single pchannel, so exactly one split message with vchannel = controlChannel.
	return splitMsgs[0].
		WithTimeTick(timeTick).
		WithLastConfirmed(rmq.NewRmqID(lastConfirmedID)).
		IntoImmutableMessage(rmq.NewRmqID(msgID))
}

func TestFlushAllOnControlChannel(t *testing.T) {
	pchannel := "test_channel"
	controlChannel := funcutil.GetControlChannel(pchannel)

	newGrowingSegment := func(segmentID, collectionID, partitionID int64, vchannel string) *streamingpb.SegmentAssignmentMeta {
		return &streamingpb.SegmentAssignmentMeta{
			SegmentId:          segmentID,
			CollectionId:       collectionID,
			PartitionId:        partitionID,
			Vchannel:           vchannel,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 1,
			Stat: &streamingpb.SegmentAssignmentStat{
				MaxRows:      1000,
				ModifiedRows: 10,
			},
		}
	}

	newTestRS := func() *recoveryStorageImpl {
		rs := &recoveryStorageImpl{
			cfg:              newConfig(),
			currentClusterID: "test1",
			channel:          types.PChannelInfo{Name: pchannel},
			checkpoint: &WALCheckpoint{
				MetaCheckpoint: &Checkpoint{MessageID: rmq.NewRmqID(1), TimeTick: 1},
			},
			vchannels:      make(map[string]*vchannelRecoveryInfo),
			metrics:        newRecoveryStorageMetrics(types.PChannelInfo{Name: pchannel}),
			segmentManager: newTestSegmentManager(t),
		}
		rs.segmentManager.RecoverFromSnapshot(map[int64]*streamingpb.SegmentAssignmentMeta{
			100: newGrowingSegment(100, 1, 1, pchannel+"_v0"),
			200: newGrowingSegment(200, 2, 2, pchannel+"_v1"),
		})
		return rs
	}

	t.Run("FlushAllOnControlChannel", func(t *testing.T) {
		rs := newTestRS()

		// Verify segments are initially GROWING
		for _, seg := range rs.segmentManager.GetSnapshots() {
			assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, seg.GetState())
		}

		// Create FlushAll message on control channel via broadcast split
		flushAllMsg := buildFlushAllOnControlChannel(pchannel, controlChannel, 1, 10, 5, 10)

		rs.observeMessage(flushAllMsg)

		// All segments should be FLUSHED
		for segID, seg := range rs.segmentManager.GetSnapshots() {
			assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, seg.GetState(),
				"segment %d should be flushed", segID)
		}

		// Observed checkpoint should be updated; persisted checkpoint is gated
		// until dirty snapshot consumption.
		assert.Equal(t, uint64(10), rs.observedCheckpoint.MetaCheckpoint.TimeTick)
		assert.True(t, rs.observedCheckpoint.MetaCheckpoint.MessageID.EQ(rmq.NewRmqID(5)))
	})

	t.Run("FlushAllOnRegularChannel", func(t *testing.T) {
		rs := newTestRS()

		// Create FlushAll message on regular channel (not control channel)
		flushAllMsg := message.NewFlushAllMessageBuilderV2().
			WithVChannel(pchannel).
			WithHeader(&message.FlushAllMessageHeader{}).
			WithBody(&message.FlushAllMessageBody{}).
			MustBuildMutable().
			WithTimeTick(10).
			WithLastConfirmed(rmq.NewRmqID(5)).
			IntoImmutableMessage(rmq.NewRmqID(10))

		rs.observeMessage(flushAllMsg)

		// All segments should be FLUSHED
		for segID, seg := range rs.segmentManager.GetSnapshots() {
			assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, seg.GetState(),
				"segment %d should be flushed", segID)
		}

		assert.Equal(t, uint64(10), rs.observedCheckpoint.MetaCheckpoint.TimeTick)
	})

	t.Run("FlushAllIdempotent", func(t *testing.T) {
		rs := newTestRS()

		// First FlushAll
		flushAllMsg1 := buildFlushAllOnControlChannel(pchannel, controlChannel, 1, 10, 5, 10)

		rs.observeMessage(flushAllMsg1)

		for _, seg := range rs.segmentManager.GetSnapshots() {
			assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, seg.GetState())
		}

		// Second FlushAll with higher timetick - should be idempotent
		flushAllMsg2 := buildFlushAllOnControlChannel(pchannel, controlChannel, 2, 20, 15, 20)

		rs.observeMessage(flushAllMsg2)

		// Segments should remain FLUSHED
		for segID, seg := range rs.segmentManager.GetSnapshots() {
			assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, seg.GetState(),
				"segment %d should still be flushed", segID)
		}

		assert.Equal(t, uint64(20), rs.observedCheckpoint.MetaCheckpoint.TimeTick)
	})
}
