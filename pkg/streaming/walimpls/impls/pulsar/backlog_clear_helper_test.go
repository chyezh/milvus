package pulsar

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type backlogTestClient struct{ pulsar.Client }

func (*backlogTestClient) Subscribe(pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	panic("mockey")
}

type backlogTestConsumer struct{ pulsar.Consumer }

func (*backlogTestConsumer) Seek(pulsar.MessageID) error { panic("mockey") }
func (*backlogTestConsumer) Close()                      { panic("mockey") }

func TestBacklogClearRequiresPublishedCheckpoint(t *testing.T) {
	mockey.PatchConvey("append traffic never authorizes truncation beyond recovery", t, func() {
		channel := types.PChannelInfo{Name: "test", Term: 7, AccessMode: types.AccessModeRW}
		var subscriptions atomic.Int64
		var closes atomic.Int64
		seeks := make(chan pulsar.MessageID, 8)
		mockey.Mock((*backlogTestClient).Subscribe).To(func(_ *backlogTestClient, opts pulsar.ConsumerOptions) (pulsar.Consumer, error) {
			require.Equal(t, "public/default/test", opts.Topic)
			require.Equal(t, pulsar.SubscriptionPositionEarliest, opts.SubscriptionInitialPosition)
			require.Equal(t, backlogClearHelperName, opts.SubscriptionName)
			require.True(t, opts.StartMessageIDInclusive)
			subscriptions.Add(1)
			return &backlogTestConsumer{}, nil
		}).Build()
		mockey.Mock((*backlogTestConsumer).Seek).To(func(_ *backlogTestConsumer, id pulsar.MessageID) error { seeks <- id; return nil }).Build()
		mockey.Mock((*backlogTestConsumer).Close).To(func() { closes.Add(1) }).Build()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		h, err := newBacklogClearHelper(ctx, &backlogTestClient{}, channel, 100, tenant{"public", "default"}, nil)
		require.NoError(t, err)
		defer h.Close()
		require.EqualValues(t, 1, subscriptions.Load(), "durable cursor precedes writer admission")
		h.ObserveAppend(1000)
		select {
		case <-seeks:
			t.Fatal("unpublished WAL was cleared")
		case <-time.After(50 * time.Millisecond):
		}
		first := newMessageIDOfPulsar(1, 2, 0)
		h.AdvanceCheckpoint(first)
		assertSeek := func(expected message.MessageID) {
			t.Helper()
			select {
			case actual := <-seeks:
				require.True(t, expected.EQ(NewPulsarID(actual)))
			case <-time.After(3 * time.Second):
				t.Fatal("published checkpoint was not applied")
			}
		}
		assertSeek(first)
		h.AdvanceCheckpoint(newMessageIDOfPulsar(1, 1, 0))
		h.AdvanceCheckpoint(first)
		h.ObserveAppend(100)
		assertSeek(first)
		// Canceling Open does not stop an installed backend's retention worker.
		cancel()
		w := &walImpl{WALHelper: helper.NewWALHelper(&walimpls.OpenOption{Channel: channel}), backlogClearHelper: h}
		second := newMessageIDOfPulsar(1, 3, 0)
		require.ErrorIs(t, w.Truncate(ctx, second), context.Canceled)
		require.NoError(t, w.Truncate(t.Context(), second))
		// Persisting the last write must eventually release backlog even if
		// there is no next append to cross another traffic threshold.
		assertSeek(second)
		h.Close()
		require.EqualValues(t, 1, closes.Load())
		require.EqualValues(t, 1, subscriptions.Load(), "successful seeks reuse the durable consumer")
	})
}

func TestBacklogClearSeedAndRetry(t *testing.T) {
	mockey.PatchConvey("cold open clears only the persisted restart position", t, func() {
		seed := newMessageIDOfPulsar(1, 5, 0)
		var seeks, subscriptions, closes atomic.Int64
		done := make(chan struct{})
		mockey.Mock((*backlogTestClient).Subscribe).To(func(pulsar.ConsumerOptions) (pulsar.Consumer, error) {
			subscriptions.Add(1)
			return &backlogTestConsumer{}, nil
		}).Build()
		mockey.Mock((*backlogTestConsumer).Close).To(func() { closes.Add(1) }).Build()
		mockey.Mock((*backlogTestConsumer).Seek).To(func(_ *backlogTestConsumer, id pulsar.MessageID) error {
			require.True(t, seed.EQ(NewPulsarID(id)))
			if seeks.Add(1) == 1 {
				return merr.WrapErrServiceUnavailable("retry seek")
			}
			close(done)
			return nil
		}).Build()
		h, err := newBacklogClearHelper(t.Context(), &backlogTestClient{}, types.PChannelInfo{Name: "test", Term: 8}, 100, tenant{"public", "default"}, seed)
		require.NoError(t, err)
		defer h.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("checkpoint seek was not retried")
		}
		h.Close()
		require.EqualValues(t, 2, seeks.Load())
		require.EqualValues(t, 2, subscriptions.Load())
		require.EqualValues(t, 2, closes.Load())
	})
}

func TestBacklogClearOpenFailures(t *testing.T) {
	for _, fail := range []string{"canceled", "wrong-backend", "subscription", "canceled-after-subscription"} {
		t.Run(fail, func(t *testing.T) {
			mockey.PatchConvey("failed construction leaves no consumer or background worker", t, func() {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				closed := mockey.Mock((*backlogTestConsumer).Close).Return().Build()
				mockey.Mock((*backlogTestClient).Subscribe).To(func(pulsar.ConsumerOptions) (pulsar.Consumer, error) {
					if fail == "subscription" {
						return nil, merr.WrapErrServiceUnavailable("subscribe failed")
					}
					cancel()
					return &backlogTestConsumer{}, nil
				}).Build()
				var checkpoint message.MessageID
				if fail == "canceled" {
					cancel()
				}
				if fail == "wrong-backend" {
					checkpoint = walimplstest.NewTestMessageID(1)
				}
				h, err := newBacklogClearHelper(ctx, &backlogTestClient{}, types.PChannelInfo{Name: "test"}, 100, tenant{"public", "default"}, checkpoint)
				require.Error(t, err)
				require.Nil(t, h)
				if fail == "canceled-after-subscription" {
					require.Equal(t, 1, closed.Times())
				} else {
					require.Zero(t, closed.Times())
				}
			})
		})
	}
}

func TestPulsarReadOnlyCannotTruncate(t *testing.T) {
	w := &walImpl{WALHelper: helper.NewWALHelper(&walimpls.OpenOption{Channel: types.PChannelInfo{Name: "test", AccessMode: types.AccessModeRO}})}
	require.Panics(t, func() { _ = w.Truncate(t.Context(), newMessageIDOfPulsar(1, 1, 0)) })
}
