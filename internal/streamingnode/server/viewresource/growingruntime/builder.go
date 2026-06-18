package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type NoopBuilder struct{}

func (NoopBuilder) Build(context.Context, Descriptor) (*Runtime, error) {
	return &Runtime{}, nil
}

type SnapshotBuilder struct {
	NewApplier ApplierFactory
}

func (p SnapshotBuilder) Build(ctx context.Context, desc Descriptor) (*Runtime, error) {
	if err := validateWALViewSnapshot(desc); err != nil {
		return nil, err
	}
	applierFactory := p.NewApplier
	if applierFactory == nil {
		applierFactory = newSegcoreApplier
	}
	applier, err := applierFactory(ctx, desc)
	if err != nil {
		return nil, err
	}
	applierPrepared := false
	defer func() {
		if !applierPrepared {
			applier.Close()
		}
	}()
	segments := desc.WALView.SegmentSnapshot.Segments
	segmentIDs := make([]int64, 0, len(segments))
	sealedAtDataVersions := make(map[int64]qviews.DataVersion)
	flushedSegments := make(map[int64]struct{})
	for _, segment := range segments {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		segmentIDs = append(segmentIDs, segment.SegmentID)
		if segment.Data.PersistedStorage != nil {
			if err := applier.LoadPersistedSegment(ctx, segment); err != nil {
				return nil, err
			}
		}
		for _, msg := range segment.Data.InsertMessages {
			if err := applier.ApplySnapshotInsert(ctx, segment, msg); err != nil {
				return nil, err
			}
		}
		if segment.SealedAtDataVersion != nil {
			sealedAtDataVersions[segment.SegmentID] = qviews.FromProtoDataVersion(segment.SealedAtDataVersion)
			flushedSegments[segment.SegmentID] = struct{}{}
			if marker, ok := applier.(segmentFlushMarker); ok {
				marker.markSegmentFlushed(segment.SegmentID)
			}
		}
	}
	deleteEntries, err := drainDeleteReplay(ctx, desc.WALView.DeleteReplay)
	if err != nil {
		return nil, err
	}
	for _, entry := range deleteEntries {
		if err := applier.ApplyDeleteReplay(ctx, entry); err != nil {
			return nil, err
		}
	}
	runtime := &Runtime{
		SegmentIDs:           segmentIDs,
		Segments:             segmentsFromApplier(applier),
		DeleteReplayEntries:  deleteEntries,
		LiveEvents:           desc.LiveEvents,
		applier:              applier,
		flushedSegments:      flushedSegments,
		sealedAtDataVersions: sealedAtDataVersions,
	}
	runtime.SetBM25Runtime(desc.BM25)
	runtime.appliedGrowingTimeTick.Store(desc.WALView.BaseGrowingTimeTick)
	runtime.appliedTransformTimeTick.Store(desc.WALView.BaseTransformTimeTick)
	runtime.startLiveApply(ctx, desc.LiveDone, desc.OnApplied)
	applierPrepared = true
	return runtime, nil
}

func validateWALViewSnapshot(desc Descriptor) error {
	snapshot := desc.WALView.SegmentSnapshot
	if snapshot.CollectionID != 0 && snapshot.CollectionID != desc.CollectionID() {
		return errors.Errorf(
			"wal view snapshot mismatch: view collection %d, snapshot collection %d",
			desc.CollectionID(),
			snapshot.CollectionID,
		)
	}
	if snapshot.VChannel != "" && snapshot.VChannel != desc.VChannel() {
		return errors.Errorf(
			"wal view snapshot mismatch: view vchannel %s, snapshot vchannel %s",
			desc.VChannel(),
			snapshot.VChannel,
		)
	}
	return nil
}

func drainDeleteReplay(ctx context.Context, scanner wal.TransformLogScanner) ([]*streamingpb.TransformLogEntry, error) {
	if scanner == nil {
		return nil, nil
	}
	entries := make([]*streamingpb.TransformLogEntry, 0)
	for {
		select {
		case event, ok := <-scanner.Chan():
			if !ok {
				return entries, scanner.Close()
			}
			if event.Entry != nil {
				entries = append(entries, event.Entry)
			}
			if event.CaughtUp != nil {
				return entries, scanner.Close()
			}
		case <-scanner.Done():
			return entries, scanner.Close()
		case <-ctx.Done():
			_ = scanner.Close()
			return nil, ctx.Err()
		}
	}
}

func segmentsFromApplier(applier Applier) map[int64]segcore.CSegment {
	if concrete, ok := applier.(*segcoreApplier); ok {
		return concrete.snapshotSegments()
	}
	return nil
}
