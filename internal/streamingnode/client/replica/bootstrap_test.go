package replica

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type bootstrapTestClient struct {
	streamingpb.StreamingNodeReplicaServiceClient
}

func (*bootstrapTestClient) BootstrapWALReplica(context.Context, *streamingpb.WALReplicaBootstrapRequest, ...grpc.CallOption) (streamingpb.StreamingNodeReplicaService_BootstrapWALReplicaClient, error) {
	panic("mockey")
}

type bootstrapTestStream struct {
	streamingpb.StreamingNodeReplicaService_BootstrapWALReplicaClient
}

func (*bootstrapTestStream) Recv() (*streamingpb.WALReplicaBootstrapFrame, error) { panic("mockey") }

func TestBootstrapClientRequiresCompleteTransfer(t *testing.T) {
	for _, failure := range []string{"open", "metadata-eof", "metadata-kind", "metadata-term", "replay-eof", "replay-kind", "replay-decode", "replay-cancel", "complete-eof", "complete-cut", "complete-cancel", "trailing-error", "trailing-frame", "success"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("partial candidates never become complete", t, func() {
				snapshot := &streamingpb.WALReplicaSnapshot{Cursor: &streamingpb.WALReplicaCursor{Term: 7, Seq: 12}, Checkpoint: &streamingpb.WALCheckpoint{TimeTick: 1, MessageId: walimplstest.NewTestMessageID(1).IntoProto()}, Observed: &streamingpb.WALCheckpoint{TimeTick: 2, MessageId: walimplstest.NewTestMessageID(1).IntoProto()}}
				if failure == "metadata-term" {
					snapshot.Cursor.Term = 8
				}
				frames := []*streamingpb.WALReplicaBootstrapFrame{}
				require.NoError(t, walevent.WriteSnapshot(t.Context(), snapshot, func(chunk *streamingpb.WALReplicaSnapshotChunk) error {
					frames = append(frames, &streamingpb.WALReplicaBootstrapFrame{Payload: &streamingpb.WALReplicaBootstrapFrame_Snapshot{Snapshot: chunk}})
					return nil
				}))
				tick := message.NewTimeTickMessageBuilderV1().WithAllVChannel().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).MustBuildMutable().WithTimeTick(2).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
				entry := &streamingpb.WALReplicaBootstrapFrame{Payload: &streamingpb.WALReplicaBootstrapFrame_Replay{Replay: walevent.EncodeEntry(tick)}}
				complete := &streamingpb.WALReplicaBootstrapFrame{Payload: &streamingpb.WALReplicaBootstrapFrame_Complete{Complete: proto.CloneOf(snapshot.Cursor)}}
				frames = append(frames, entry, complete)
				endErr := io.EOF
				switch failure {
				case "metadata-eof":
					frames = nil
				case "metadata-kind":
					frames[0] = complete
				case "replay-eof":
					frames = frames[:1]
				case "replay-kind":
					frames[1] = complete
				case "replay-decode":
					entry.GetReplay().Message.Id = nil
				case "complete-eof":
					frames = frames[:2]
				case "complete-cut":
					complete.GetComplete().Seq++
				case "trailing-error":
					endErr = io.ErrUnexpectedEOF
				case "trailing-frame":
					frames = append(frames, entry)
				}
				var streamCtx context.Context
				mockey.Mock((*bootstrapTestClient).BootstrapWALReplica).To(func(ctx context.Context, req *streamingpb.WALReplicaBootstrapRequest, _ ...grpc.CallOption) (streamingpb.StreamingNodeReplicaService_BootstrapWALReplicaClient, error) {
					streamCtx = ctx
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.LessOrEqual(t, time.Until(deadline), 2*time.Minute)
					require.EqualValues(t, 7, req.Pchannel.Term)
					require.EqualValues(t, 5, req.Checkpoint.TimeTick)
					if failure == "open" {
						return nil, io.ErrClosedPipe
					}
					return &bootstrapTestStream{}, nil
				}).Build()
				mockey.Mock((*bootstrapTestStream).Recv).To(func() (*streamingpb.WALReplicaBootstrapFrame, error) {
					if len(frames) == 0 {
						return nil, endErr
					}
					frame := frames[0]
					frames = frames[1:]
					return frame, nil
				}).Build()
				source := &Source{primary: types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 7}}, events: &bootstrapTestClient{}}
				transfer, err := source.Bootstrap(t.Context(), &streamingpb.WALCheckpoint{TimeTick: 5})
				if failure == "open" || failure == "metadata-eof" || failure == "metadata-kind" || failure == "metadata-term" {
					require.Error(t, err)
					require.Nil(t, transfer)
					require.ErrorIs(t, streamCtx.Err(), context.Canceled)
					return
				}
				require.NoError(t, err)
				defer transfer.Close()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if failure == "replay-cancel" {
					cancel()
				}
				actual, err := transfer.Replay(ctx)
				if failure == "replay-eof" || failure == "replay-kind" || failure == "replay-decode" || failure == "replay-cancel" {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.EqualValues(t, 2, actual.TimeTick())
				if failure == "complete-cancel" {
					cancel()
				}
				err = transfer.Complete(ctx)
				if failure == "success" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				transfer.Close()
				require.ErrorIs(t, streamCtx.Err(), context.Canceled)
			})
		})
	}
}
