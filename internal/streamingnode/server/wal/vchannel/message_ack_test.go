package vchannel

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type testMessageObserver interface {
	ObserveMessage(context.Context, message.OwnedImmutableMessage)
}

func observeTestMessage(
	ctx context.Context,
	t *testing.T,
	observer testMessageObserver,
	raw message.ImmutableMessage,
) message.ImmutableMessage {
	t.Helper()
	owner := message.NewOwnedImmutableMessage(raw, nil)
	observer.ObserveMessage(ctx, owner)
	owner.Release()
	return raw
}
