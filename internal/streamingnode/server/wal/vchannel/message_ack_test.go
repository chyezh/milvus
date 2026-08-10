package vchannel

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type testMessageObserver interface {
	ObserveMessage(context.Context, messageack.Message)
}

func observeTestMessage(
	ctx context.Context,
	t *testing.T,
	observer testMessageObserver,
	raw message.ImmutableMessage,
) messageack.Record {
	t.Helper()
	record := messageack.NewRecord(utility.WALConsumeCheckpoint{
		MessageID: raw.LastConfirmedMessageID(),
		TimeTick:  raw.TimeTick(),
	}, nil)
	observer.ObserveMessage(ctx, messageack.NewMessage(raw, record))
	record.Seal()
	return record
}
