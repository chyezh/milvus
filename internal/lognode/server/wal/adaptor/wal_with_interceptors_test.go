package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/mocks/lognode/server/wal/mock_walimpls"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNoInterceptor(t *testing.T) {
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
		return nil, nil
	})
	l.EXPECT().Close().Run(func() {})

	lWithInterceptors := newWALWithInterceptors(l)

	_, err := lWithInterceptors.Append(context.Background(), nil)
	assert.NoError(t, err)
	lWithInterceptors.Close()
}

func TestWALWithInterceptor(t *testing.T) {
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
		return nil, nil
	})
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
		return nil, nil
	})
	l.EXPECT().Close().Run(func() {})

	b := mock_walimpls.NewMockInterceptorBuilder(t)

	readyCh := make(chan struct{})
	b.EXPECT().Build(mock.Anything).RunAndReturn(func(_ <-chan walimpls.WALImpls) walimpls.BasicInterceptor {
		interceptor := mock_walimpls.NewMockInterceptorWithReady(t)
		interceptor.EXPECT().Ready().Return(readyCh)
		interceptor.EXPECT().DoAppend(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, mm message.MutableMessage, f func(context.Context, message.MutableMessage) (message.MessageID, error)) (message.MessageID, error) {
				return f(ctx, mm)
			})
		interceptor.EXPECT().Close().Run(func() {})
		return interceptor
	})
	lWithInterceptors := newWALWithInterceptors(l, b)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	// Interceptor is not ready, so the append/read will be blocked until timeout.
	_, err := lWithInterceptors.Append(ctx, nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Interceptor is ready, so the append/read will return soon.
	close(readyCh)
	_, err = lWithInterceptors.Append(context.Background(), nil)
	assert.NoError(t, err)

	_, err = lWithInterceptors.Read(context.Background(), walimpls.ReadOption{})
	assert.NoError(t, err)

	lWithInterceptors.Close()
}
