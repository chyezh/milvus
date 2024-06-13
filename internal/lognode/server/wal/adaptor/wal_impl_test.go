package adaptor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/mocks/lognode/server/wal/mock_walimpls"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWalImplReadFail(t *testing.T) {
	l := mock_walimpls.NewMockWALImpls(t)
	expectedErr := errors.New("test")
	l.EXPECT().Channel().Return(&logpb.PChannelInfo{})
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, ro walimpls.ReadOption) (walimpls.ScannerImpls, error) {
			return nil, expectedErr
		})

	lExtend := adaptImplsToWAL(l, func() {})
	scanner, err := lExtend.Read(context.Background(), wal.ReadOption{})
	assert.NoError(t, err)
	assert.NotNil(t, scanner)
	assert.ErrorIs(t, scanner.Error(), expectedErr)
}

func TestWALImpl(t *testing.T) {
	// Create a mock WAL implementation
	l := mock_walimpls.NewMockWALImpls(t)
	l.EXPECT().Channel().Return(&logpb.PChannelInfo{})
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
			return nil, nil
		})
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ro walimpls.ReadOption) (walimpls.ScannerImpls, error) {
		scanner := mock_walimpls.NewMockScannerImpls(t)
		ch := make(chan message.ImmutableMessage, 1)
		scanner.EXPECT().Chan().Return(ch)
		scanner.EXPECT().Close().RunAndReturn(func() error {
			close(ch)
			return nil
		})
		return scanner, nil
	})
	l.EXPECT().Close().Return()

	lExtend := adaptImplsToWAL(l, func() {})
	assert.NotNil(t, lExtend.Channel())
	_, err := lExtend.Append(context.Background(), nil)
	assert.NoError(t, err)
	lExtend.AppendAsync(context.Background(), nil, func(mi message.MessageID, err error) {
		assert.Nil(t, err)
	})

	// Test in concurrency env.
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			scanner, err := lExtend.Read(context.Background(), wal.ReadOption{})
			if err != nil {
				assertShutdownError(t, err)
				return
			}
			assert.NoError(t, err)
			<-scanner.Chan()
		}(i)
	}
	time.Sleep(time.Second * 1)
	lExtend.Close()

	// All wal should be closed with Opener.
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()

	select {
	case <-time.After(time.Second * 3):
		t.Errorf("wal close should be fast")
	case <-ch:
	}

	_, err = lExtend.Append(context.Background(), nil)
	assertShutdownError(t, err)
	lExtend.AppendAsync(context.Background(), nil, func(mi message.MessageID, err error) {
		assertShutdownError(t, err)
	})
	_, err = lExtend.Read(context.Background(), wal.ReadOption{})
	assertShutdownError(t, err)
}

func assertShutdownError(t *testing.T, err error) {
	e := status.AsLogError(err)
	assert.Equal(t, e.Code, logpb.LogCode_LOG_CODE_ON_SHUTDOWN)
}
