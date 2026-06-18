package growingruntime

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type NoopApplier struct{}

func NoopApplierFactory(context.Context, Descriptor) (Applier, error) {
	return NoopApplier{}, nil
}

func (NoopApplier) LoadPersistedSegment(context.Context, walview.VisibleSegment) error {
	return nil
}

func (NoopApplier) ApplySnapshotInsert(context.Context, walview.VisibleSegment, message.ImmutableMessage) error {
	return nil
}

func (NoopApplier) ApplyDeleteReplay(context.Context, *streamingpb.TransformLogEntry) error {
	return nil
}

func (NoopApplier) ApplyLiveMessage(context.Context, message.ImmutableMessage) error {
	return nil
}

func (NoopApplier) Close() {}
