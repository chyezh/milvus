package walmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestWALLifetime(t *testing.T) {
	channel := "test"
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			// Respect context cancellation like the real opener does.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Channel().Return(oo.Channel)
			l.EXPECT().Close().Return()
			return l, nil
		})

	wlt := newWALLifetime(opener, channel, log.With())
	assert.Nil(t, wlt.GetWAL())

	// Test open.
	store := wlt.AsyncOpen(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 2,
	})
	err := store.BlockUntilReady(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(2), wlt.GetWAL().Channel().Term)

	// Test expired term remove.
	err = wlt.Remove(context.Background(), 1)
	assertErrorOperationIgnored(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(2), wlt.GetWAL().Channel().Term)

	// Test remove.
	err = wlt.Remove(context.Background(), 2)
	assert.NoError(t, err)
	assert.Nil(t, wlt.GetWAL())

	// Test expired term open.
	store = wlt.AsyncOpen(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 1,
	})
	progress := store.Get()
	assertErrorOperationIgnored(t, progress.Error)
	assert.Nil(t, wlt.GetWAL())

	// Test open after close.
	store = wlt.AsyncOpen(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 5,
	})
	err = store.BlockUntilReady(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(5), wlt.GetWAL().Channel().Term)

	// Test overwrite open.
	store = wlt.AsyncOpen(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 10,
	})
	err = store.BlockUntilReady(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(10), wlt.GetWAL().Channel().Term)

	// Test context canceled - AsyncOpen itself doesn't block, but the background open uses the context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store = wlt.AsyncOpen(ctx, types.PChannelInfo{
		Name: channel,
		Term: 11,
	})
	err = store.BlockUntilReady(context.Background())
	assert.Error(t, err)

	err = wlt.Remove(ctx, 11)
	assert.ErrorIs(t, err, context.Canceled)

	store = wlt.AsyncOpen(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 11,
	})
	progress = store.Get()
	assertErrorOperationIgnored(t, progress.Error)

	store = wlt.AsyncOpen(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 12,
	})
	err = store.BlockUntilReady(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(12), wlt.GetWAL().Channel().Term)

	wlt.Close()
}
