package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type NoopBuilder struct{}

func (NoopBuilder) NewRuntime(desc Descriptor) (*Runtime, error) {
	runtime := newRuntime(desc)
	runtime.prepareFunc = func(context.Context) error {
		runtime.markReady()
		return nil
	}
	return runtime, nil
}

type SnapshotBuilder struct{}

func (p SnapshotBuilder) NewRuntime(desc Descriptor) (*Runtime, error) {
	if err := validateWALViewSnapshot(desc); err != nil {
		return nil, err
	}
	return newRuntime(desc), nil
}

func (r *Runtime) Prepare(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if r.prepareFunc != nil {
		return r.prepareFunc(ctx)
	}
	desc := r.desc
	collection, err := newCollection(desc)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if r.state == stateClosed {
		r.mu.Unlock()
		if collection != nil {
			collection.Release()
		}
		return context.Canceled
	}
	r.collection = collection
	r.mu.Unlock()

	prepared := false
	defer func() {
		if !prepared {
			r.Close()
		}
	}()
	for _, visible := range desc.WALView.SegmentSnapshot.Segments {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		segment, err := newGrowingSegmentFromVisible(ctx, collection, visible)
		if err != nil {
			return err
		}
		if !r.addSegment(segment) {
			segment.release()
			return context.Canceled
		}
	}
	deleteEntries, err := drainDeleteReplay(ctx, desc.WALView.DeleteReplay)
	if err != nil {
		return err
	}
	r.setDeleteReplayEntries(deleteEntries)
	for _, entry := range deleteEntries {
		if err := r.applyTransformLogEntry(ctx, entry); err != nil {
			return err
		}
	}
	r.appliedGrowingTimeTick.Store(desc.WALView.BaseGrowingTimeTick)
	r.appliedTransformTimeTick.Store(desc.WALView.BaseTransformTimeTick)
	r.markReady()
	prepared = true
	return nil
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
