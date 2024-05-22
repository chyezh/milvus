package message

import (
	"bytes"
	"testing"

	rawpulsar "github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/kafka"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/nmq"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/pulsar"
)

func TestMessageIDFromPBMessageID(t *testing.T) {
	pbMsgID := &logpb.MessageID{
		Id: &logpb.MessageID_Nmq{
			Nmq: &logpb.MessageIDNmq{
				Offset: 123,
			},
		},
	}
	msgID := NewMessageIDFromPBMessageID(pbMsgID)
	assert.True(t, msgID.EQ(nmq.NewNmqID(123)))

	pbMsgID = &logpb.MessageID{
		Id: &logpb.MessageID_Rmq{
			Rmq: &logpb.MessageIDRmq{
				Offset: 456,
			},
		},
	}
	msgID = NewMessageIDFromPBMessageID(pbMsgID)
	assert.True(t, msgID.EQ(&server.RmqID{MessageID: 456}))

	pbMsgID = &logpb.MessageID{
		Id: &logpb.MessageID_Kafka{
			Kafka: &logpb.MessageIDKafka{
				Offset: 789,
			},
		},
	}
	msgID = NewMessageIDFromPBMessageID(pbMsgID)
	assert.True(t, msgID.EQ(kafka.NewKafkaID(789)))

	msg1 := rawpulsar.EarliestMessageID()
	pbMsgID = &logpb.MessageID{
		Id: &logpb.MessageID_Pulsar{
			Pulsar: &logpb.MessageIDPulsar{
				Serialized: msg1.Serialize(),
			},
		},
	}
	msgID = NewMessageIDFromPBMessageID(pbMsgID)
	assert.True(t, msgID.EQ(pulsar.NewPulsarID(msg1)))
}

func TestPBMessageIDFromMessageID(t *testing.T) {
	msgID := nmq.NewNmqID(123)
	pbMsgID := NewPBMessageIDFromMessageID(msgID)
	assert.Equal(t, uint64(123), pbMsgID.GetNmq().GetOffset())

	msgID = &server.RmqID{MessageID: 456}
	pbMsgID = NewPBMessageIDFromMessageID(msgID)
	assert.Equal(t, int64(456), pbMsgID.GetRmq().GetOffset())

	msgID = kafka.NewKafkaID(789)
	pbMsgID = NewPBMessageIDFromMessageID(msgID)
	assert.Equal(t, int64(789), pbMsgID.GetKafka().GetOffset())

	msg1 := rawpulsar.EarliestMessageID()
	msgID = pulsar.NewPulsarID(msg1)
	pbMsgID = NewPBMessageIDFromMessageID(msgID)
	assert.True(t, bytes.Equal(msg1.Serialize(), pbMsgID.GetPulsar().GetSerialized()))
}
