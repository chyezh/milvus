package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func (c *DDLCallback) putCollectionV2CheckCallback(ctx context.Context, msg message.BroadcastPutCollectionMessageV2) (message.BroadcastMutableMessage, error) {
	return nil, nil
}

func (c *DDLCallback) putCollectionV2AckCallback(ctx context.Context, msg ...message.ImmutablePutCollectionMessageV2) error {
	return nil
}
