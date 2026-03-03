package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type Consumer struct {
	c          *kafka.Consumer
	config     *kafka.ConfigMap
	msgChannel chan common.Message
	hasAssign  bool
	skipMsg    bool
	topic      string
	groupID    string
	chanOnce   sync.Once
	closeOnce  sync.Once
	closeCh    chan struct{}
	wg         sync.WaitGroup
}

const timeout = 3000

func newKafkaConsumer(config *kafka.ConfigMap, bufSize int64, topic string, groupID string, position common.SubscriptionInitialPosition) (*Consumer, error) {
	msgChannel := make(chan common.Message, bufSize)
	kc := &Consumer{
		config:     config,
		msgChannel: msgChannel,
		topic:      topic,
		groupID:    groupID,
		closeCh:    make(chan struct{}),
	}

	err := kc.createKafkaConsumer()
	if err != nil {
		return nil, err
	}

	// if it's unknown, we leave the assign to seek
	if position != common.SubscriptionPositionUnknown {
		var offset kafka.Offset
		if position == common.SubscriptionPositionEarliest {
			offset, err = kafka.NewOffset("earliest")
			if err != nil {
				return nil, err
			}
		} else {
			latestMsgID, err := kc.GetLatestMsgID()

			if err != nil {
				switch v := err.(type) {
				case kafka.Error:
					if v.Code() == kafka.ErrUnknownTopic || v.Code() == kafka.ErrUnknownPartition || v.Code() == kafka.ErrUnknownTopicOrPart {
						log.Warn(context.TODO(), "get latest msg ID failed, topic or partition does not exists!",
							log.String("topic", kc.topic),
							log.String("err msg", v.String()))
						offset, err = kafka.NewOffset("earliest")
						if err != nil {
							return nil, err
						}
					}
				default:
					log.Error(context.TODO(), "kafka get latest msg ID failed", log.String("topic", kc.topic), log.Err(err))
					return nil, err
				}
			} else {
				offset = kafka.Offset(latestMsgID.(*KafkaID).MessageID)
				kc.skipMsg = true
			}
		}

		start := time.Now()
		topicPartition := []kafka.TopicPartition{{Topic: &topic, Partition: mqwrapper.DefaultPartitionIdx, Offset: offset}}
		err = kc.c.Assign(topicPartition)
		if err != nil {
			log.Error(context.TODO(), "kafka consumer assign failed ", log.String("topic name", topic), log.Any("Msg position", position), log.Err(err))
			return nil, err
		}
		cost := time.Since(start).Milliseconds()
		if cost > 200 {
			log.Warn(context.TODO(), "kafka consumer assign take too long!", log.String("topic name", topic), log.Any("Msg position", position), log.Int64("time cost(ms)", cost))
		}

		kc.hasAssign = true
	}

	return kc, nil
}

func (kc *Consumer) createKafkaConsumer() error {
	var err error
	kc.c, err = kafka.NewConsumer(kc.config)
	if err != nil {
		log.Error(context.TODO(), "create kafka consumer failed", log.String("topic", kc.topic), log.Err(err))
		return err
	}
	return nil
}

func (kc *Consumer) Subscription() string {
	return kc.groupID
}

// Chan provides a channel to read consumed message.
// confluent-kafka-go recommend us to use function-based consumer,
// channel-based consumer API had already deprecated, see more details
// https://github.com/confluentinc/confluent-kafka-go.
func (kc *Consumer) Chan() <-chan common.Message {
	if !kc.hasAssign {
		log.Error(context.TODO(), "can not chan with not assigned channel", log.String("topic", kc.topic), log.String("groupID", kc.groupID))
		panic("failed to chan a kafka consumer without assign")
	}
	kc.chanOnce.Do(func() {
		kc.wg.Add(1)
		go func() {
			defer kc.wg.Done()
			for {
				select {
				case <-kc.closeCh:
					if kc.msgChannel != nil {
						close(kc.msgChannel)
					}
					return
				default:
					readTimeout := paramtable.Get().KafkaCfg.ReadTimeout.GetAsDuration(time.Second)
					e, err := kc.c.ReadMessage(readTimeout)
					if err != nil {
						// if we failed to read message in 30 Seconds, print out a warn message since there should always be a tt
						log.Warn(context.TODO(), "consume msg failed", log.String("topic", kc.topic), log.String("groupID", kc.groupID), log.Err(err))
					} else {
						if kc.skipMsg {
							kc.skipMsg = false
							continue
						}

						select {
						case kc.msgChannel <- &kafkaMessage{msg: e}:
						case <-kc.closeCh:
						}
					}
				}
			}
		}()
	})

	return kc.msgChannel
}

func (kc *Consumer) Seek(id common.MessageID, inclusive bool) error {
	if kc.hasAssign {
		return errors.New("kafka consumer is already assigned, can not seek again")
	}

	offset := kafka.Offset(id.(*KafkaID).MessageID)
	return kc.internalSeek(offset, inclusive)
}

func (kc *Consumer) internalSeek(offset kafka.Offset, inclusive bool) error {
	log.Info(context.TODO(), "kafka consumer seek start", log.String("topic name", kc.topic),
		log.Any("Msg offset", offset), log.Bool("inclusive", inclusive))

	start := time.Now()
	err := kc.c.Assign([]kafka.TopicPartition{{Topic: &kc.topic, Partition: mqwrapper.DefaultPartitionIdx, Offset: offset}})
	if err != nil {
		log.Warn(context.TODO(), "kafka consumer assign failed ", log.String("topic name", kc.topic), log.Any("Msg offset", offset), log.Err(err))
		return err
	}

	cost := time.Since(start).Milliseconds()
	if cost > 200 {
		log.Warn(context.TODO(), "kafka consumer assign take too long!", log.String("topic name", kc.topic),
			log.Any("Msg offset", offset), log.Bool("inclusive", inclusive), log.Int64("time cost(ms)", cost))
	}

	// If seek timeout is not 0 the call twice will return error isStarted RD_KAFKA_RESP_ERR__STATE.
	// if the timeout is 0 it will initiate the seek  but return immediately without any error reporting
	kc.skipMsg = !inclusive
	if err := kc.c.Seek(kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: mqwrapper.DefaultPartitionIdx,
		Offset:    offset,
	}, timeout); err != nil {
		return err
	}
	cost = time.Since(start).Milliseconds()
	log.Info(context.TODO(), "kafka consumer seek finished", log.String("topic name", kc.topic),
		log.Any("Msg offset", offset), log.Bool("inclusive", inclusive), log.Int64("time cost(ms)", cost))

	kc.hasAssign = true
	return nil
}

func (kc *Consumer) Ack(message common.Message) {
	// Do nothing
	// Kafka retention mechanism only depends on retention configuration,
	// it does not relate to the commit with consumer's offsets.
}

func (kc *Consumer) GetLatestMsgID() (common.MessageID, error) {
	low, high, err := kc.c.QueryWatermarkOffsets(kc.topic, mqwrapper.DefaultPartitionIdx, timeout)
	if err != nil {
		return nil, err
	}

	// Current high value is next offset of the latest message ID, in order to keep
	// semantics consistency with the latest message ID, the high value need to move forward.
	if high > 0 {
		high = high - 1
	}

	log.Info(context.TODO(), "get latest msg ID ", log.String("topic", kc.topic), log.Int64("oldest offset", low), log.Int64("latest offset", high))
	return &KafkaID{MessageID: high}, nil
}

func (kc *Consumer) CheckTopicValid(topic string) error {
	_, err := kc.GetLatestMsgID()
	log.With(log.String("topic", kc.topic))
	// check topic is existed
	if err != nil {
		switch v := err.(type) {
		case kafka.Error:
			if v.Code() == kafka.ErrUnknownTopic || v.Code() == kafka.ErrUnknownTopicOrPart {
				return merr.WrapErrMqTopicNotFound(topic, err.Error())
			}
			return merr.WrapErrMqInternal(err)
		default:
			return err
		}
	}

	return nil
}

func (kc *Consumer) closeInternal() {
	log.Info(context.TODO(), "close consumer ", log.String("topic", kc.topic), log.String("groupID", kc.groupID))
	start := time.Now()
	err := kc.c.Close()
	if err != nil {
		log.Warn(context.TODO(), "failed to close ", log.String("topic", kc.topic), log.Err(err))
	}
	cost := time.Since(start).Milliseconds()
	if cost > 200 {
		log.Warn(context.TODO(), "close consumer costs too long time", log.String("topic", kc.topic), log.String("groupID", kc.groupID), log.Int64("time(ms)", cost))
	}
}

func (kc *Consumer) Close() {
	kc.closeOnce.Do(func() {
		close(kc.closeCh)
		// wait work goroutine exit
		kc.wg.Wait()
		// close the client
		kc.closeInternal()
	})
}
