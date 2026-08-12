package vchannel

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type testMessageObserver interface {
	ObserveDataMessage(context.Context, message.RefCountedImmutableMessageOwner)
}

func observeTestMessage(
	ctx context.Context,
	t *testing.T,
	observer testMessageObserver,
	raw message.ImmutableMessage,
) message.ImmutableMessage {
	t.Helper()
	owner := message.NewRefCountedImmutableMessageOwner(raw, nil)
	observer.ObserveDataMessage(ctx, owner)
	owner.Release()
	return raw
}
