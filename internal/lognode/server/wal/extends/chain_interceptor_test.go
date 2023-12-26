package extends

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	mock_wal "github.com/milvus-io/milvus/internal/mocks/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestChainInterceptor(t *testing.T) {
	for i := 0; i < 5; i++ {
		testChainInterceptor(t, i)
	}
}

func testChainInterceptor(t *testing.T, count int) {
	type record struct {
		before bool
		after  bool
		closed bool
	}

	interceptorRecords := make([]record, 0, count)
	interceptors := make([]wal.AppendInterceptor, 0, count)
	for i := 0; i < count; i++ {
		j := i
		interceptorRecords = append(interceptorRecords, record{})

		interceptor := mock_wal.NewMockAppendInterceptor(t)
		interceptor.EXPECT().Do(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, mm message.MutableMessage, f func(context.Context, message.MutableMessage) (mqwrapper.MessageID, error)) (mqwrapper.MessageID, error) {
				interceptorRecords[j].before = true
				msgID, err := f(ctx, mm)
				interceptorRecords[j].after = true
				return msgID, err
			})
		interceptor.EXPECT().Close().Run(func() {
			interceptorRecords[j].closed = true
		})
		interceptors = append(interceptors, interceptor)
	}
	interceptor := newChainedInterceptor(interceptors...)

	// fast return
	<-interceptor.Ready()

	msg, err := interceptor.Do(context.TODO(), nil, func(context.Context, message.MutableMessage) (mqwrapper.MessageID, error) {
		return nil, nil
	})
	assert.NoError(t, err)
	assert.Nil(t, msg)
	interceptor.Close()
	for i := 0; i < count; i++ {
		assert.True(t, interceptorRecords[i].before)
		assert.True(t, interceptorRecords[i].after)
		assert.True(t, interceptorRecords[i].closed)
	}
}
