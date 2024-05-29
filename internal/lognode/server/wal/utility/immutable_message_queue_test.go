package utility

import (
	"testing"

	"github.com/milvus-io/milvus/internal/mocks/util/logserviceutil/mock_message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/stretchr/testify/assert"
)

func TestImmutableMessageQueue(t *testing.T) {
	q := NewImmutableMessageQueue()
	for i := 0; i < 100; i++ {
		q.Add([]message.ImmutableMessage{
			mock_message.NewMockImmutableMessage(t),
		})
		assert.Equal(t, i+1, q.Len())
	}
	for i := 100; i > 0; i-- {
		assert.NotNil(t, q.Next())
		q.UnsafeAdvance()
		assert.Equal(t, i-1, q.Len())
	}
}
