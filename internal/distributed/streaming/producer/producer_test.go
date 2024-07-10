package producer

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
)

func TestResumableProducer(t *testing.T) {
	p := mock_producer.NewMockProducer(t)
	msgID := mock_message.NewMockMessageID(t)
	p.EXPECT().Produce(mock.Anything, mock.Anything).Return(msgID, nil)
	p.EXPECT().Close().Return()
	ch := make(chan struct{})
	p.EXPECT().Available().Return(ch)
	p.EXPECT().IsAvailable().RunAndReturn(func() bool {
		select {
		case <-ch:
			return false
		default:
			return true
		}
	})

	i := 0
	rp := NewResumableProducer(func(ctx context.Context, opts *options.ProducerOptions) (producer.Producer, error) {
		if i == 0 {
			i++
			return p, nil
		} else if i == 1 {
			i++
			return nil, errors.New("test")
		}
		p := mock_producer.NewMockProducer(t)
		msgID := mock_message.NewMockMessageID(t)
		p.EXPECT().Produce(mock.Anything, mock.Anything).Return(msgID, nil)
		p.EXPECT().Close().Return()
		ch := make(chan struct{})
		p.EXPECT().Available().Return(ch)
		p.EXPECT().IsAvailable().RunAndReturn(func() bool {
			select {
			case <-ch:
				return false
			default:
				return true
			}
		})
		return p, nil
	}, &options.ProducerOptions{
		VChannel: "test",
	})

	assert.Equal(t, "test", rp.Channel())
	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().EstimateSize().Return(1)
	id, err := rp.Produce(context.Background(), msg)
	assert.NotNil(t, id)
	assert.NoError(t, err)
	close(ch)
	id, err = rp.Produce(context.Background(), msg)
	assert.NotNil(t, id)
	assert.NoError(t, err)
	rp.Close()
}
