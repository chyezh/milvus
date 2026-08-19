package registry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type mockWALManager struct {
	t            *testing.T
	walReplicaID int64
}

func (m *mockWALManager) Metrics() (*types.StreamingNodeMetrics, error) {
	return &types.StreamingNodeMetrics{}, nil
}

func (m *mockWALManager) GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	return m.GetAvailableWALReplica(channel, 0)
}

func (m *mockWALManager) GetAvailableWALReplica(_ types.PChannelInfo, walReplicaID int64) (wal.WAL, error) {
	m.walReplicaID = walReplicaID
	l := mock_wal.NewMockWAL(m.t)
	l.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{}, nil).Maybe()
	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(mock_wal.NewMockScanner(m.t), nil).Maybe()
	return l, nil
}

func TestGetLocalAvailableWAL(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)

	manager := &mockWALManager{t: t}
	RegisterLocalWALManager(manager)

	walInstance, err := GetLocalAvailableWAL(types.PChannelInfo{})
	assert.NoError(t, err)
	assert.NotNil(t, walInstance)
	assert.True(t, IsLocal(walInstance))
	assert.Equal(t, int64(0), manager.walReplicaID)

	msg, _ := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	walInstance.Append(context.Background(), msg)
	walInstance.AppendAsync(context.Background(), msg, func(ar *wal.AppendResult, err error) {})

	s, err := walInstance.Read(context.Background(), wal.ReadOption{})
	assert.NoError(t, err)
	assert.NotNil(t, walInstance)
	assert.True(t, IsLocal(s))
}

func TestGetLocalAvailableWALReplica(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)
	ResetRegisterLocalWALManager()

	manager := &mockWALManager{t: t}
	RegisterLocalWALManager(manager)

	walInstance, err := GetLocalAvailableWALReplica(types.PChannelInfo{}, 2)
	assert.NoError(t, err)
	assert.NotNil(t, walInstance)
	assert.Equal(t, int64(2), manager.walReplicaID)
}
