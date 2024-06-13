package ack

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/stretchr/testify/assert"
)

func TestDetail(t *testing.T) {
	assert.Panics(t, func() {
		newAckDetail(0, message.NewTestMessageID(1))
	})
	assert.Panics(t, func() {
		newAckDetail(1, nil)
	})
	ackDetail := newAckDetail(1, message.NewTestMessageID(1))
	assert.Equal(t, uint64(1), ackDetail.Timestamp)
	assert.True(t, ackDetail.LastConfirmedMessageID.EQ(message.NewTestMessageID(1)))
	assert.False(t, ackDetail.IsSync)
	assert.NoError(t, ackDetail.Err)

	OptSync()(ackDetail)
	assert.True(t, ackDetail.IsSync)
	OptError(errors.New("test"))(ackDetail)
	assert.Error(t, ackDetail.Err)
}
