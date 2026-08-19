package walmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/partialupdate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestOpenManager(t *testing.T) {
	resource.InitForTest(t)

	m, err := OpenManager()
	assert.NoError(t, err)
	assert.NotNil(t, m)
	m.Close()
}

func TestPartialUpdateInterceptorRunsAfterShard(t *testing.T) {
	builders := newInterceptorBuilders()
	assert.Len(t, builders, 6)
	assert.IsType(t, shard.NewInterceptorBuilder(), builders[4])
	assert.IsType(t, partialupdate.NewInterceptorBuilder(), builders[5])
}

func TestManager(t *testing.T) {
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(testMixCoordClient{MockMixCoordClient: mixcoord})
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Metrics().Return(types.RWWALMetrics{}).Maybe()
			l.EXPECT().Channel().Return(oo.Channel)
			l.EXPECT().IsAvailable().Return(true).Maybe()
			l.EXPECT().Close().Return()
			l.EXPECT().IsAvailable().Return(true).Maybe()
			l.EXPECT().Metrics().Return(types.RWWALMetrics{})
			return l, nil
		})
	opener.EXPECT().Close().Return()

	m := newManager(opener)
	channelName := "ch1"

	l, err := m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorChannelNotExist(t, err)
	assert.Nil(t, l)

	h, err := m.Metrics()
	assert.NoError(t, err)
	assert.Len(t, h.WALMetrics, 0)

	err = m.Remove(context.Background(), types.PChannelInfo{Name: channelName, Term: 1})
	assert.NoError(t, err)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorChannelNotExist(t, err)
	assert.Nil(t, l)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: channelName,
		Term: 1,
	})
	assertErrorOperationIgnored(t, err)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: channelName,
		Term: 2,
	})
	assert.NoError(t, err)

	err = m.Remove(context.Background(), types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorOperationIgnored(t, err)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorTermExpired(t, err)
	assert.Nil(t, l)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 2})
	assert.NoError(t, err)
	assert.NotNil(t, l)

	h, err = m.Metrics()
	assert.NoError(t, err)
	assert.Len(t, h.WALMetrics, 1)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: "term2",
		Term: 3,
	})
	assert.NoError(t, err)

	h, err = m.Metrics()
	assert.NoError(t, err)
	assert.Len(t, h.WALMetrics, 2)

	m.Close()

	h, err = m.Metrics()
	assertShutdownError(t, err)
	assert.Nil(t, h)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: "term2",
		Term: 4,
	})
	assertShutdownError(t, err)

	err = m.Remove(context.Background(), types.PChannelInfo{Name: channelName, Term: 2})
	assertShutdownError(t, err)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 2})
	assertShutdownError(t, err)
	assert.Nil(t, l)
}

func TestManagerAllowsMultipleWALReplicasForSamePChannel(t *testing.T) {
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(testMixCoordClient{MockMixCoordClient: mixcoord})
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Metrics().Return(types.RWWALMetrics{}).Maybe()
			l.EXPECT().Channel().Return(oo.Channel).Maybe()
			l.EXPECT().IsAvailable().Return(true).Maybe()
			l.EXPECT().Close().Return().Maybe()
			return l, nil
		})
	opener.EXPECT().Close().Return()

	m := newManager(opener)
	channel := types.PChannelInfo{Name: "shared", Term: 1, AccessMode: types.AccessModeRW}
	roChannel := types.PChannelInfo{Name: "shared", Term: 1, AccessMode: types.AccessModeRO}

	assert.NoError(t, m.Open(context.Background(), channel))
	assert.NoError(t, m.OpenWALReplica(context.Background(), roChannel, 2, 1))

	metrics, err := m.Metrics()
	assert.NoError(t, err)
	assert.Contains(t, metrics.WALMetrics, types.ChannelID{Name: "shared"})
	assert.Contains(t, metrics.WALMetrics, types.ChannelID{Name: "shared", WALReplicaID: 2})

	wal0, err := m.GetAvailableWAL(channel)
	assert.NoError(t, err)
	assert.NotNil(t, wal0)
	wal2, err := m.GetAvailableWALReplica(roChannel, 2)
	assert.NoError(t, err)
	assert.NotNil(t, wal2)

	assert.NoError(t, m.RemoveWALReplica(context.Background(), roChannel, 2, 1))
	wal2, err = m.GetAvailableWALReplica(roChannel, 2)
	assertErrorChannelNotExist(t, err)
	assert.Nil(t, wal2)

	wal0, err = m.GetAvailableWAL(channel)
	assert.NoError(t, err)
	assert.NotNil(t, wal0)
	m.Close()
}

func TestManagerAllowsReadOnlyWALReplicaAcrossPChannelTermAdvance(t *testing.T) {
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(testMixCoordClient{MockMixCoordClient: mixcoord})
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Metrics().Return(types.RWWALMetrics{}).Maybe()
			l.EXPECT().Channel().Return(oo.Channel).Maybe()
			l.EXPECT().IsAvailable().Return(true).Maybe()
			l.EXPECT().Close().Return().Maybe()
			return l, nil
		})
	opener.EXPECT().Close().Return()

	m := newManager(opener)
	defer m.Close()

	roChannel := types.PChannelInfo{Name: "shared", Term: 10, AccessMode: types.AccessModeRO}
	require.NoError(t, m.OpenWALReplica(context.Background(), roChannel, 2, 1))

	advancedTermRO := types.PChannelInfo{Name: "shared", Term: 11, AccessMode: types.AccessModeRO}
	roWAL, err := m.GetAvailableWALReplica(advancedTermRO, 2)
	require.NoError(t, err)
	require.NotNil(t, roWAL)

	advancedTermRW := types.PChannelInfo{Name: "shared", Term: 11, AccessMode: types.AccessModeRW}
	rwWAL, err := m.GetAvailableWALReplica(advancedTermRW, 2)
	assertErrorTermExpired(t, err)
	require.Nil(t, rwWAL)
}

func assertShutdownError(t *testing.T, err error) {
	assert.Error(t, err)
	e := status.AsStreamingError(err)
	assert.Equal(t, e.Code, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN)
}
