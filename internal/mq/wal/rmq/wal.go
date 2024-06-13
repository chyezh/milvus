package rmq

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/client"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"go.uber.org/zap"
)

var _ walimpls.WALImpls = (*walImpl)(nil)

// walImpl is the implementation of walimpls.WAL interface.
type walImpl struct {
	*helper.WALHelper
	p client.Producer
	c client.Client
}

// Append appends a message to the wal.
func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	id, err := w.p.Send(&mqwrapper.ProducerMessage{
		Payload:    msg.Payload(),
		Properties: msg.Properties().ToRawMap(),
	})
	if err != nil {
		w.Log().RatedWarn(1, "send message to rmq failed", zap.Error(err))
		return nil, err
	}
	return rmqID(id), nil
}

// Read create a scanner to read the wal.
func (w *walImpl) Read(ctx context.Context, opt walimpls.ReadOption) (s walimpls.ScannerImpls, err error) {
	scannerName := opt.Name
	receiveChannel := make(chan mqwrapper.Message, 1024)
	consumerOption := client.ConsumerOptions{
		Topic:                       w.Channel().GetName(),
		SubscriptionName:            scannerName,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionUnknown,
		MessageChannel:              receiveChannel,
	}
	switch opt.DeliverPolicy.Policy.(type) {
	case *logpb.DeliverPolicy_All:
		consumerOption.SubscriptionInitialPosition = mqwrapper.SubscriptionPositionEarliest
	case *logpb.DeliverPolicy_Latest:
		consumerOption.SubscriptionInitialPosition = mqwrapper.SubscriptionPositionLatest
	}

	// Subscribe the MQ consumer.
	consumer, err := w.c.Subscribe(consumerOption)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			// release the subscriber if following operation is failure.
			// to avoid resource leak.
			consumer.Close()
		}
	}()

	// Seek the MQ consumer.
	var exclude *rmqID
	switch policy := opt.DeliverPolicy.Policy.(type) {
	case *logpb.DeliverPolicy_StartFrom:
		id, err := unmarshalMessageID(policy.StartFrom.Id)
		if err != nil {
			return nil, err
		}
		// Do a inslusive seek.
		if err = consumer.Seek(int64(id)); err != nil {
			return nil, err
		}
	case *logpb.DeliverPolicy_StartAfter:
		id, err := unmarshalMessageID(policy.StartAfter.Id)
		if err != nil {
			return nil, err
		}
		// Do a exclude seek.
		exclude = &id
		if err = consumer.Seek(int64(id)); err != nil {
			return nil, err
		}
	}
	return newScanner(scannerName, exclude, consumer), nil
}

// Close closes the wal.
func (w *walImpl) Close() {
	w.p.Close() // close all producer
}
