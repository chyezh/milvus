package metastore

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// WALReplayRetentionCatalog serializes bootstrap pins with physical truncation.
// Checkpoint publication itself is not blocked. Every backend truncation path,
// including cold-open backlog clearing, must use the returned authorized bound.
// Autonomous backend retention must separately respect these obligations.
type WALReplayRetentionCatalog interface {
	PinWALReplay(context.Context, string, *streamingpb.WALReplayPin) error
	ReleaseWALReplay(context.Context, string, string) error
	AuthorizeWALTruncation(context.Context, string, int64, *streamingpb.WALCheckpoint) (*streamingpb.WALCheckpoint, error)
}
