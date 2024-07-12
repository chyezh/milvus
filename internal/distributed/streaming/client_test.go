package streaming_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	_ "github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestStreamingProduce(t *testing.T) {
	paramtable.Init()
	etcd, _ := kvfactory.GetEtcdAndPath()
	c := streaming.NewClient(etcd)
	p := c.CreateProducer(&streaming.ProducerOptions{
		VChannel: "by-dev-rootcoord-dml_0",
	})
	for i := 0; i < 100; i++ {
		msgID, err := p.Produce(context.Background(), message.NewMutableMessageBuilder().
			WithMessageType(message.MessageTypeInsert).
			WithPayload([]byte("test")).
			WithProperties(map[string]string{
				"collectionID": "0",
			}).
			BuildMutable())
		fmt.Printf("msgID = %+v, err = %v\n", msgID, err)
	}
	p.Close()
	c.Close()
}

func TestStreamingConsume(t *testing.T) {
	paramtable.Init()
	etcd, _ := kvfactory.GetEtcdAndPath()
	c := streaming.NewClient(etcd)
	ch := make(message.ChanMessageHandler, 10)
	consumer := c.CreateConsumer(&streaming.ConsumerOptions{
		VChannel:       "by-dev-rootcoord-dml_0",
		DeliverPolicy:  options.DeliverPolicyAll(),
		MessageHandler: ch,
	})
	defer func() {
		consumer.Close()
		c.Close()
	}()

	for {
		select {
		case msg := <-ch:
			fmt.Printf("msgID=%+v, tt=%d, lca=%+v, payload=%s \n",
				msg.MessageID(),
				msg.TimeTick(),
				msg.LastConfirmedMessageID(),
				string(msg.Payload()))
		case <-time.After(time.Second):
			return
		}
	}
}
