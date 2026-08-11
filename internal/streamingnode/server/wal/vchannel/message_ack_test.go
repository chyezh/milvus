package vchannel

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type testMessageObserver interface {
	ObserveMessage(context.Context, message.ImmutableMessage)
}

func observeTestMessage(
	ctx context.Context,
	t *testing.T,
	observer testMessageObserver,
	raw message.ImmutableMessage,
) message.RefCountedImmutableMessageController {
	t.Helper()
	controller := message.NewRefCountedImmutableMessage(raw, nil)
	observer.ObserveMessage(ctx, controller)
	controller.Seal()
	return controller
}
