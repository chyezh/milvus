package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestInsertBufferSnapshotMessagesOutliveRetainedHandles(t *testing.T) {
	raw := message.CreateTestInsertMessage(t, 100, 1, 20, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	controller := message.NewRefCountedImmutableMessage(raw, nil)
	handle := controller.Retain()
	buffer := writeOnlyInsertBuffer{entries: []message.RetainedImmutableMessage{handle}}

	snapshot := buffer.Messages()
	controller.Seal()
	handle.Release()

	require.Len(t, snapshot, 1)
	assert.Equal(t, uint64(20), snapshot[0].TimeTick())
}
