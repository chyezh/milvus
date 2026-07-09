package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiveObserverRegistryBroadcastsEmptyVChannelMessages(t *testing.T) {
	registry := newLiveObserverRegistry()
	observer1 := &recordingLiveObserver{}
	observer2 := &recordingLiveObserver{}
	registry.Register("v1", observer1)
	registry.Register("v2", observer2)

	msg := newRecoveryTestTimeTickMessage(t, "", 10)
	registry.Dispatch(context.Background(), msg)

	require.Len(t, observer1.messages, 1)
	require.Same(t, msg, observer1.messages[0])
	require.Len(t, observer2.messages, 1)
	require.Same(t, msg, observer2.messages[0])
}
