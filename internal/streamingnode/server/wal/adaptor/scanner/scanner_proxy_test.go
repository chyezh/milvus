package scanner

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/stretchr/testify/assert"
)

func TestScannerProxy(t *testing.T) {
	startMessageID := rmq.NewRmqID(2)
	s := newScannerProxy("rocksmq", "test", walimpls.ReadOption{
		Name:                "test_s",
		DeliverPolicy:       options.DeliverPolicyStartFrom(startMessageID),
		ReadAheadBufferSize: 10,
	})
	assert.Equal(t, s.ExpectedMessageID(), startMessageID)

	ctx := context.Background()
	msg := mock_message.NewMockImmutableMessage(t)
	messageID1 := rmq.NewRmqID(1)
	msg.EXPECT().MessageID().Return(messageID1)
	err := s.Push(ctx, msg)
	assert.NoError(t, err)
	assert.True(t, shouldBlock(s.Chan()))

	msg.EXPECT().MessageID().Unset()
	msg.EXPECT().MessageID().Return(startMessageID)
	s.Push(ctx, msg)
	assert.NoError(t, err)
	<-s.Chan()

	s.Push(ctx, msg)
	assert.NoError(t, err)
	assert.True(t, shouldBlock(s.Chan()))

	s.Close()
	assert.ErrorIs(t, s.Push(ctx, msg), errScannerProxyClosed)
	assert.NoError(t, s.Error())
	assert.Equal(t, s.Name(), "test_s")
	assert.Equal(t, s.VChannel(), "test")
}

func shouldBlock(ch <-chan message.ImmutableMessage) bool {
	select {
	case <-ch:
		return false
	default:
		return true
	}
}
