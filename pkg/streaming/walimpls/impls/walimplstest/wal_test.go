package walimplstest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

func TestWALImplsTest(t *testing.T) {
	enableFenceError.Store(false)
	defer enableFenceError.Store(true)
	walimpls.NewWALImplsTestFramework(t, 100, &openerBuilder{}).Run()
}

func TestRecoveryBarrierAppendBypassesRandomFenceError(t *testing.T) {
	Reset()

	w, err := (&opener{}).Open(context.Background(), &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       "recovery-barrier-random-error",
			AccessMode: types.AccessModeRW,
		},
	})
	require.NoError(t, err)

	for i := 0; i < 2000; i++ {
		msg := message.NewRecoveryBarrierMessageBuilderV2().
			WithHeader(&message.RecoveryBarrierMessageHeader{}).
			WithBody(&message.RecoveryBarrierMessageBody{}).
			WithAllVChannel().
			MustBuildMutable().
			WithTimeTick(uint64(i + 1)).
			WithLastConfirmedUseMessageID()

		_, err := w.Append(context.Background(), msg)
		require.NoError(t, err)
	}
}
