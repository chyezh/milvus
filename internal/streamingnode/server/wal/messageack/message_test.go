package messageack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestMessageWrapsImmutableMessageAndRecord(t *testing.T) {
	lastConfirmed := walimplstest.NewTestMessageID(10)
	raw := message.CreateTestTimeTickSyncMessage(t, 1, 20, lastConfirmed).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	record := NewRecord(utility.WALConsumeCheckpoint{
		MessageID: lastConfirmed,
		TimeTick:  raw.TimeTick(),
	}, nil)

	wrapped := NewMessage(raw, record)

	assert.Equal(t, raw.MessageType(), wrapped.MessageType())
	assert.Equal(t, raw.TimeTick(), wrapped.TimeTick())
	assert.Same(t, raw, wrapped.Message())
	require.NotNil(t, wrapped.Ack())
	assert.Equal(t, record, wrapped.Ack())
	var _ message.ImmutableMessage = wrapped
}

func TestMetaMessageHasNoAckRecord(t *testing.T) {
	raw := message.CreateTestTimeTickSyncMessage(t, 1, 20, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))

	wrapped := NewMetaMessage(raw)

	assert.Nil(t, wrapped.Ack())
	assert.Equal(t, raw.TimeTick(), wrapped.TimeTick())
}
