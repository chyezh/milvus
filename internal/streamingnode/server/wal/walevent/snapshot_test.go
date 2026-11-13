package walevent

import (
	"context"
	"crypto/sha256"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func snapshotFrames(t *testing.T) (*streamingpb.WALReplicaSnapshot, []*streamingpb.WALReplicaSnapshotChunk) {
	t.Helper()
	image := &streamingpb.WALReplicaSnapshot{
		Cursor:        &streamingpb.WALReplicaCursor{Term: 7, Seq: 10},
		Vchannels:     []*streamingpb.VChannelMeta{{Vchannel: strings.Repeat("v", SnapshotChunkBytes+10)}},
		QueryBarriers: map[string]uint64{"v": 100},
	}
	var frames []*streamingpb.WALReplicaSnapshotChunk
	require.NoError(t, WriteSnapshot(t.Context(), image, func(frame *streamingpb.WALReplicaSnapshotChunk) error {
		frames = append(frames, proto.CloneOf(frame))
		return nil
	}))
	require.Len(t, frames, 2)
	return image, frames
}

func snapshotReceiver(frames []*streamingpb.WALReplicaSnapshotChunk) func() (*streamingpb.WALReplicaSnapshotChunk, error) {
	return func() (*streamingpb.WALReplicaSnapshotChunk, error) {
		if len(frames) == 0 {
			return nil, io.EOF
		}
		frame := frames[0]
		frames = frames[1:]
		return frame, nil
	}
}

func TestSnapshotFramingRoundTrip(t *testing.T) {
	image, frames := snapshotFrames(t)
	got, err := ReadSnapshot(t.Context(), snapshotReceiver(frames))
	require.NoError(t, err)
	require.True(t, proto.Equal(image, got))
	require.EqualValues(t, SnapshotChunkBytes, len(frames[0].Data))
	require.EqualValues(t, len(frames[0].Data), frames[1].Offset)
	require.Equal(t, frames[0].Sha256, frames[1].Sha256)
	// No RPC transport/session identity is needed: all frames bind to Term+Seq.
	require.True(t, proto.Equal(frames[0].Cursor, frames[1].Cursor))
}

func TestSnapshotFramingRejectsPartialOrMixedCuts(t *testing.T) {
	for _, invalid := range []string{"nil", "term", "empty-total", "large-total", "digest-size", "empty-data", "large-frame", "first-offset", "different-term", "different-seq", "different-size", "different-digest", "duplicate", "missing", "overflow", "corruption", "payload-cut", "malformed-proto", "records"} {
		t.Run(invalid, func(t *testing.T) {
			_, frames := snapshotFrames(t)
			switch invalid {
			case "nil":
				frames[0] = nil
			case "term":
				frames[0].Cursor.Term = 0
			case "empty-total":
				frames[0].TotalBytes = 0
			case "large-total":
				frames[0].TotalBytes = MaxSnapshotBytes + 1
			case "digest-size":
				frames[0].Sha256 = nil
			case "empty-data":
				frames[0].Data = nil
			case "large-frame":
				frames[0].Data = make([]byte, SnapshotChunkBytes+1)
			case "first-offset":
				frames[0].Offset = 1
			case "different-term":
				frames[1].Cursor.Term++
			case "different-seq":
				frames[1].Cursor.Seq++
			case "different-size":
				frames[1].TotalBytes++
			case "different-digest":
				frames[1].Sha256[0]++
			case "duplicate":
				frames[1] = frames[0]
			case "missing":
				frames = frames[:1]
			case "overflow":
				frames[1].Data = append(frames[1].Data, 0)
			case "corruption":
				frames[1].Data[0]++
			case "payload-cut":
				frames[0].Cursor.Term++
				frames[1].Cursor.Term++
			case "malformed-proto", "records":
				data := []byte{255}
				if invalid == "records" {
					image := &streamingpb.WALReplicaSnapshot{Cursor: frames[0].Cursor, Vchannels: make([]*streamingpb.VChannelMeta, MaxSnapshotRecords+1)}
					var err error
					data, err = proto.Marshal(image)
					require.NoError(t, err)
				}
				digest := sha256.Sum256(data)
				frames = []*streamingpb.WALReplicaSnapshotChunk{{Cursor: frames[0].Cursor, TotalBytes: uint64(len(data)), Data: data, Sha256: digest[:]}}
			}
			image, err := ReadSnapshot(t.Context(), snapshotReceiver(frames))
			require.Error(t, err)
			require.Nil(t, image)
			if invalid == "missing" {
				require.ErrorIs(t, err, io.EOF)
			}
			if invalid == "records" {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			}
		})
	}
}

func TestSnapshotFramingCancellationAndProducerErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	image, _ := snapshotFrames(t)
	require.ErrorIs(t, WriteSnapshot(ctx, image, nil), context.Canceled)
	_, err := ReadSnapshot(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, WriteSnapshot(t.Context(), nil, nil), merr.ErrServiceInternal)
	require.ErrorIs(t, WriteSnapshot(t.Context(), image, func(*streamingpb.WALReplicaSnapshotChunk) error { return io.ErrClosedPipe }), io.ErrClosedPipe)
	ctx, cancel = context.WithCancel(t.Context())
	require.ErrorIs(t, WriteSnapshot(ctx, image, func(*streamingpb.WALReplicaSnapshotChunk) error { cancel(); return nil }), context.Canceled)
	image.Vchannels[0].Vchannel = "\xff"
	require.Error(t, WriteSnapshot(t.Context(), image, nil))
	image.Vchannels[0].Vchannel = strings.Repeat("v", MaxSnapshotBytes)
	require.ErrorIs(t, WriteSnapshot(t.Context(), image, nil), merr.ErrServiceUnavailable)
	image.Vchannels = make([]*streamingpb.VChannelMeta, MaxSnapshotRecords+1)
	require.ErrorIs(t, WriteSnapshot(t.Context(), image, nil), merr.ErrServiceUnavailable)
}
