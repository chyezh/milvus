package l0materializer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReadOnlyMaterializerDoesNotRetainPersistenceHandles(t *testing.T) {
	m := NewWALMaterializer(WALConfig{VChannel: "v1", ReadOnly: true})
	for _, msg := range []message.ImmutableMessage{walDelete(100), walFlush(110)} {
		released := false
		owner := message.NewOwnedImmutableMessage(msg, func() { released = true })
		h := owner.Clone()
		m.ObserveMessage(h)
		h.Release()
		owner.Release()
		require.True(t, released)
	}
	m.RequestPersistThrough(110)
	m.RequestPersistThroughUnderPressure(110)
	m.FlushStale(time.Now().Add(time.Hour), time.Second)
	require.Nil(t, m.task)
	require.Empty(t, m.pending)
}

func TestMaterializationPublishesBeforeReleasingACK(t *testing.T) {
	m, tasks, batches := testWALMaterializer(t, 0, 1)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	walObserve(m, tracker, walDelete(100))
	fail := true
	publications := 0
	m.runtime.PublishStorage = func(_ context.Context, event walevent.StoragePublished) error {
		publications++
		require.Zero(t, tracker.CompletedPoint().TimeTick)
		require.Zero(t, m.MaterializedTimeTick())
		require.Equal(t, "v1", event.VChannels[0].Vchannel)
		require.EqualValues(t, 100, event.VChannels[0].TransformMaterializedTimeTick)
		if fail {
			return merr.WrapErrServiceUnavailable("event publication failed")
		}
		return nil
	}
	require.Error(t, (*tasks)[0].Execute(context.Background()))
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	fail = false
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, 2, publications)
	require.Len(t, *batches, 1, "publication retry cannot write another L0 output")
	require.EqualValues(t, 100, tracker.CompletedPoint().TimeTick)
}
