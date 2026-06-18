package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type NoopBuilder struct{}

func (NoopBuilder) Build(context.Context, Descriptor) (*Runtime, error) {
	return newRuntime(nil), nil
}

type SnapshotBuilder struct{}

func (p SnapshotBuilder) Build(ctx context.Context, desc Descriptor) (*Runtime, error) {
	if err := validateWALViewSnapshot(desc); err != nil {
		return nil, err
	}
	collection, err := newCollection(desc)
	if err != nil {
		return nil, err
	}
	runtime := newRuntime(collection)
	runtime.LiveEvents = desc.LiveEvents
	defer func() {
		if runtime != nil {
			runtime.Close()
		}
	}()
	for _, visible := range desc.WALView.SegmentSnapshot.Segments {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		segment, err := newGrowingSegmentFromVisible(ctx, collection, visible)
		if err != nil {
			return nil, err
		}
		runtime.addSegment(segment)
	}
	deleteEntries, err := drainDeleteReplay(ctx, desc.WALView.DeleteReplay)
	if err != nil {
		return nil, err
	}
	runtime.setDeleteReplayEntries(deleteEntries)
	for _, entry := range deleteEntries {
		if err := runtime.applyTransformLogEntry(ctx, entry); err != nil {
			return nil, err
		}
	}
	runtime.SetBM25Runtime(desc.BM25)
	runtime.appliedGrowingTimeTick.Store(desc.WALView.BaseGrowingTimeTick)
	runtime.appliedTransformTimeTick.Store(desc.WALView.BaseTransformTimeTick)
	runtime.startLiveApply(ctx, desc.LiveDone, desc.OnApplied)
	readyRuntime := runtime
	runtime = nil
	return readyRuntime, nil
}

func newCollection(desc Descriptor) (*segcore.CCollection, error) {
	if desc.Schema() == nil {
		return nil, nil
	}
	return segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID:  desc.CollectionID(),
		Schema:        desc.Schema(),
		LoadFieldList: desc.Settings().GetRequiredFields(),
	})
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
