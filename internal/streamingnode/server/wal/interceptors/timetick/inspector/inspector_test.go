package inspector_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestInsepctor(t *testing.T) {
	paramtable.Init()

	i := inspector.NewTimeTickSyncInspector()
	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	pchannel := types.PChannelInfo{
		Name: "test",
		Term: 1,
	}
	operator.EXPECT().Channel().Return(pchannel)
	operator.EXPECT().Sync(mock.Anything, mock.Anything).Run(func(ctx context.Context, forcePersisted bool) {})

	i.RegisterSyncOperator(operator)
	assert.Panics(t, func() {
		i.RegisterSyncOperator(operator)
	})
	i.TriggerSync(pchannel, false)
	o := i.MustGetOperator(pchannel)
	assert.NotNil(t, o)
	time.Sleep(250 * time.Millisecond)
	i.UnregisterSyncOperator(operator)

	assert.Panics(t, func() {
		i.UnregisterSyncOperator(operator)
	})
	assert.Panics(t, func() {
		i.MustGetOperator(pchannel)
	})
	i.Close()
}

func TestInspectorDoesNotSyncOnBackgroundTicker(t *testing.T) {
	paramtable.Init()
	require.NoError(t, paramtable.Get().Save(paramtable.Get().ProxyCfg.TimeTickInterval.Key, "10"))
	t.Cleanup(func() {
		require.NoError(t, paramtable.Get().Reset(paramtable.Get().ProxyCfg.TimeTickInterval.Key))
	})

	i := inspector.NewTimeTickSyncInspector()
	defer i.Close()

	operator := mock_inspector.NewMockTimeTickSyncOperator(t)
	pchannel := types.PChannelInfo{
		Name: "test-no-background-sync",
		Term: 1,
	}
	operator.EXPECT().Channel().Return(pchannel)
	i.RegisterSyncOperator(operator)

	time.Sleep(50 * time.Millisecond)
	i.UnregisterSyncOperator(operator)
}
