package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
)

type replicaRecoveryStorage struct{ recovery.RecoveryStorage }

func (*replicaRecoveryStorage) Events() *walevent.Source { panic("mockey") }

func TestReplicaSubscriptionRequiresAvailablePrimary(t *testing.T) {
	mockey.PatchConvey("only an available primary exposes its event source", t, func() {
		source := walevent.NewSource(1, 4, 1024, func(context.Context, walevent.Event) error { return nil })
		defer source.Close()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{}, param: &interceptors.InterceptorBuildParam{RecoveryStorage: &replicaRecoveryStorage{}}}
		mockey.Mock((*replicaRecoveryStorage).Events).Return(source).Build()
		available := mockey.Mock((*roWALAdaptorImpl).IsAvailable).Return(true).Build()
		reader, err := w.SubscribeWALEvents(t.Context(), walevent.Cursor{Term: 1})
		require.NoError(t, err)
		defer reader.Close()
		source.Close()
		_, err = w.SubscribeWALEvents(t.Context(), walevent.Cursor{Term: 1})
		require.Error(t, err)
		available.UnPatch()
		mockey.Mock((*roWALAdaptorImpl).IsAvailable).Return(false).Build()
		_, err = w.SubscribeWALEvents(t.Context(), walevent.Cursor{Term: 1})
		require.Error(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = w.SubscribeWALEvents(ctx, walevent.Cursor{Term: 1})
		require.ErrorIs(t, err, context.Canceled)
	})
}
