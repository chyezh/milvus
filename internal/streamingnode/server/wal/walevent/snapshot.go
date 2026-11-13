package walevent

import (
	"bytes"
	"context"
	"crypto/sha256"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	MaxSnapshotBytes   = 64 << 20
	MaxSnapshotRecords = 100000
	SnapshotChunkBytes = 256 << 10
)

// WriteSnapshot sends metadata from one immutable cut in bounded frames. WAL
// replay and live events are separate bootstrap phases, never snapshot fields.
// The caller must retain the cut's object/replay protection for the transfer.
func WriteSnapshot(ctx context.Context, snapshot *streamingpb.WALReplicaSnapshot, send func(*streamingpb.WALReplicaSnapshotChunk) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot.GetCursor().GetTerm() <= 0 {
		return merr.WrapErrServiceInternalMsg("WAL replica snapshot has no source term")
	}
	if proto.Size(snapshot) > MaxSnapshotBytes {
		return merr.WrapErrServiceUnavailable("WAL replica snapshot byte budget exhausted")
	}
	if snapshotRecords(snapshot) > MaxSnapshotRecords {
		return merr.WrapErrServiceUnavailable("WAL replica snapshot record budget exhausted")
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(snapshot)
	if err != nil {
		return merr.Wrap(err, "encode WAL replica snapshot")
	}
	digest := sha256.Sum256(data)
	for offset := 0; offset < len(data); offset += SnapshotChunkBytes {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame := &streamingpb.WALReplicaSnapshotChunk{
			Cursor: proto.CloneOf(snapshot.Cursor), TotalBytes: uint64(len(data)), Offset: uint64(offset),
			Sha256: bytes.Clone(digest[:]), Data: data[offset:min(offset+SnapshotChunkBytes, len(data))],
		}
		if err := send(frame); err != nil {
			return err
		}
	}
	return nil
}

// ReadSnapshot accepts exactly one complete metadata image. All headers are
// validated before allocating the image buffer. A missing, duplicated, shuffled
// or mixed-cut frame fails the whole candidate; no partial metadata is installed.
func ReadSnapshot(ctx context.Context, receive func() (*streamingpb.WALReplicaSnapshotChunk, error)) (*streamingpb.WALReplicaSnapshot, error) {
	var data, digest []byte
	var cursor *streamingpb.WALReplicaCursor
	var total uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		frame, err := receive()
		if err != nil {
			return nil, merr.Wrap(err, "receive WAL replica snapshot")
		}
		if frame.GetCursor().GetTerm() <= 0 || frame.GetTotalBytes() == 0 || frame.GetTotalBytes() > MaxSnapshotBytes ||
			len(frame.GetSha256()) != sha256.Size || len(frame.GetData()) == 0 || len(frame.GetData()) > SnapshotChunkBytes {
			return nil, merr.WrapErrServiceInternalMsg("invalid WAL replica snapshot frame")
		}
		if cursor == nil {
			if frame.Offset != 0 {
				return nil, merr.WrapErrServiceInternalMsg("WAL replica snapshot does not start at zero")
			}
			cursor, total, digest = proto.CloneOf(frame.Cursor), frame.TotalBytes, bytes.Clone(frame.Sha256)
			data = make([]byte, 0, int(total))
		}
		if !proto.Equal(frame.Cursor, cursor) || frame.TotalBytes != total || !bytes.Equal(frame.Sha256, digest) ||
			frame.Offset != uint64(len(data)) || uint64(len(frame.Data)) > total-uint64(len(data)) {
			return nil, merr.WrapErrServiceInternalMsg("WAL replica snapshot frame does not continue its metadata cut")
		}
		data = append(data, frame.Data...)
		if uint64(len(data)) == total {
			break
		}
	}
	actualDigest := sha256.Sum256(data)
	if !bytes.Equal(actualDigest[:], digest) {
		return nil, merr.WrapErrServiceInternalMsg("WAL replica snapshot checksum mismatch")
	}
	snapshot := &streamingpb.WALReplicaSnapshot{}
	if err := (proto.UnmarshalOptions{RecursionLimit: 128}).Unmarshal(data, snapshot); err != nil {
		return nil, merr.Wrap(err, "decode WAL replica snapshot")
	}
	if !proto.Equal(snapshot.Cursor, cursor) {
		return nil, merr.WrapErrServiceInternalMsg("WAL replica snapshot payload belongs to a different cut")
	}
	if snapshotRecords(snapshot) > MaxSnapshotRecords {
		return nil, merr.WrapErrServiceUnavailable("WAL replica snapshot record budget exhausted")
	}
	return snapshot, nil
}

func snapshotRecords(snapshot *streamingpb.WALReplicaSnapshot) int {
	return len(snapshot.Vchannels) + len(snapshot.Segments) + len(snapshot.QueryViews) + len(snapshot.QueryBarriers) +
		len(snapshot.GetStorage().GetVchannels()) + len(snapshot.GetStorage().GetSegments())
}
