package recovery

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WithQueryViewEventApplier connects the QueryView target state machine to the
// same serialized Apply boundary as ordered WAL input and storage publication.
func WithQueryViewEventApplier(apply func(context.Context, []*viewpb.QueryViewOfShard) error) RecoveryStorageOption {
	return func(r *recoveryStorageImpl) { r.applyQueryViews = apply }
}

func (r *recoveryStorageImpl) Events() *walevent.Source { return r.events }

func (r *recoveryStorageImpl) applyEvent(ctx context.Context, event walevent.Event) error {
	switch payload := event.Payload.(type) {
	case walevent.WALEntry:
		r.observeMessage(ctx, payload.Message)
	case walevent.QueryViewIntentBatch:
		if r.applyQueryViews == nil {
			return merr.WrapErrServiceInternalMsg("WAL QueryView event applier is not installed")
		}
		return r.applyQueryViews(ctx, payload.Views)
	case walevent.CheckpointPublished:
		checkpoint := utility.NewWALCheckpointFromProto(payload.Checkpoint)
		r.mu.Lock()
		r.checkpoint = checkpoint
		r.mu.Unlock()
		r.metrics.ObServePersistedMetrics(checkpoint.TimeTick)
	case walevent.StoragePublished:
		if payload.Catalog != nil {
			if err := r.vchannelManager.ApplyCatalogPublication(payload.Catalog); err != nil {
				return err
			}
		}
		if payload.Summary != nil {
			if r.summaryManager == nil {
				return merr.WrapErrServiceInternalMsg("WAL summary event applier is not installed")
			}
			if err := r.summaryManager.ApplyStoragePublished(payload.Summary); err != nil {
				return err
			}
		}
		for _, meta := range payload.Segments {
			if err := r.vchannelManager.ApplySegmentPublication(ctx, meta); err != nil {
				return err
			}
		}
		for _, meta := range payload.VChannels {
			if err := r.vchannelManager.ApplyVChannelPublication(meta); err != nil {
				return err
			}
		}
	default:
		return merr.WrapErrServiceInternalMsg("unsupported WAL replica event %T", payload)
	}
	return nil
}

func (r *recoveryStorageImpl) publishStorage(ctx context.Context, payload walevent.StoragePublished) error {
	_, err := r.events.Publish(ctx, payload)
	if err != nil && r.backgroundTaskNotifier.Context().Err() == nil && r.onFatal != nil {
		r.onFatal(merr.Wrap(err, "publish WAL storage completion"))
	}
	return err
}
