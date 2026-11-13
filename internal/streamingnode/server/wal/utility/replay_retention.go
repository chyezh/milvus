package utility

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// AuthorizeWALTruncation returns the physical bound protected by every retained
// replay pin. A catalog without retention support cannot authorize any deletion;
// callers must preserve the backlog when the returned position is nil.
func AuthorizeWALTruncation(ctx context.Context, catalog metastore.StreamingNodeCataLog, channel string, term int64, checkpoint *streamingpb.WALCheckpoint) (message.MessageID, error) {
	retention, ok := catalog.(metastore.WALReplayRetentionCatalog)
	if !ok {
		return nil, nil
	}
	bound, err := retention.AuthorizeWALTruncation(ctx, channel, term, checkpoint)
	if err != nil {
		return nil, err
	}
	return message.UnmarshalMessageID(bound.GetMessageId())
}
