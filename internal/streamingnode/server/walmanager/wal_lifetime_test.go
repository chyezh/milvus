package walmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestWALLifetime(t *testing.T) {
	const defaultWALReplicaID int64 = 0
	channel := "test"
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(testMixCoordClient{MockMixCoordClient: mixcoord})
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	// Gate the term-11 open so the background task cannot converge while the
	// canceled-context assertions below run: the waiters must actually wait
	// (and observe the cancellation) instead of racing with the background
	// convergence.
	term11OpenGate := make(chan struct{})
	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			if oo.Channel.Term == 11 {
				<-term11OpenGate
			}
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Channel().Return(oo.Channel)
			l.EXPECT().Close().Return()
			return l, nil
		})

	wlt := newWALLifetime(opener, channel, mlog.With())
	assert.Nil(t, wlt.GetWAL())

	// Test open.
	err := wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 2,
	}, defaultWALReplicaID, 0)
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(2), wlt.GetWAL().Channel().Term)

	// Test expired term remove.
	err = wlt.Remove(context.Background(), 1, 0)
	assertErrorOperationIgnored(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(2), wlt.GetWAL().Channel().Term)

	// Test remove.
	err = wlt.Remove(context.Background(), 2, 0)
	assert.NoError(t, err)
	assert.Nil(t, wlt.GetWAL())

	// Test expired term open.
	err = wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 1,
	}, defaultWALReplicaID, 0)
	assertErrorOperationIgnored(t, err)
	assert.Nil(t, wlt.GetWAL())

	// Test open after close.
	err = wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 5,
	}, defaultWALReplicaID, 0)
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(5), wlt.GetWAL().Channel().Term)

	// Test overwrite open.
	err = wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 10,
	}, defaultWALReplicaID, 0)
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(10), wlt.GetWAL().Channel().Term)

	// Test context canceled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = wlt.Open(ctx, types.PChannelInfo{
		Name: channel,
		Term: 11,
	}, defaultWALReplicaID, 0)
	assert.ErrorIs(t, err, context.Canceled)

	err = wlt.Remove(ctx, 11, 0)
	assert.ErrorIs(t, err, context.Canceled)

	// Release the background convergence of term 11.
	close(term11OpenGate)

	err = wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 11,
	}, defaultWALReplicaID, 0)
	assertErrorOperationIgnored(t, err)

	wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 12,
	}, defaultWALReplicaID, 0)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(12), wlt.GetWAL().Channel().Term)

	wlt.Close()
}

func TestWALLifetimeDuplicateOpenWaitsForInFlightExpectedState(t *testing.T) {
	const walReplicaID int64 = 1
	channel := "test"
	entered := make(chan struct{})
	unblock := make(chan struct{})
	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			close(entered)
			<-unblock
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Channel().Return(oo.Channel).Maybe()
			l.EXPECT().Close().Return().Maybe()
			return l, nil
		}).Once()

	wlt := newWALLifetime(opener, channel, mlog.With())
	defer wlt.Close()

	open := func() error {
		return wlt.Open(context.Background(), types.PChannelInfo{
			Name:       channel,
			Term:       3,
			AccessMode: types.AccessModeRW,
		}, walReplicaID, 2)
	}

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- open()
	}()
	<-entered

	secondErr := make(chan error, 1)
	go func() {
		secondErr <- open()
	}()

	select {
	case err := <-secondErr:
		require.NoError(t, err)
		t.Fatal("duplicate open returned before the in-flight expected state converged")
	case <-time.After(50 * time.Millisecond):
	}

	close(unblock)
	require.NoError(t, <-firstErr)
	require.NoError(t, <-secondErr)
}

func TestWALLifetimeUsesAssignmentEpochForSameTermWALReplica(t *testing.T) {
	const walReplicaID int64 = 2
	channel := "test-ro"
	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Channel().Return(oo.Channel).Maybe()
			l.EXPECT().Close().Return().Maybe()
			return l, nil
		})

	wlt := newWALLifetime(opener, channel, mlog.With())
	defer wlt.Close()

	require.NoError(t, wlt.Open(context.Background(), types.PChannelInfo{
		Name:       channel,
		Term:       7,
		AccessMode: types.AccessModeRO,
	}, walReplicaID, 1))
	require.NoError(t, wlt.Remove(context.Background(), 7, 1))
	assert.Nil(t, wlt.GetWAL())

	require.NoError(t, wlt.Open(context.Background(), types.PChannelInfo{
		Name:       channel,
		Term:       7,
		AccessMode: types.AccessModeRO,
	}, walReplicaID, 3))
	assert.NotNil(t, wlt.GetWAL())

	err := wlt.Remove(context.Background(), 7, 1)
	assertErrorOperationIgnored(t, err)
	assert.NotNil(t, wlt.GetWAL())
}
