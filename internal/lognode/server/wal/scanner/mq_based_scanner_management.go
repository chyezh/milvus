package scanner

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

// mqBasedScannerManagement is the creator for mqBasedScanner.
type mqBasedScannerManagement struct {
	c mqwrapper.Client
}

// Read creates a new scanner.
func (sc *mqBasedScannerManagement) Allocate(ctx context.Context, param AllocateParam) (Scanner, error) {
	consumerOption := mqwrapper.ConsumerOptions{
		Topic:                       param.Channel.Name,
		SubscriptionName:            param.ScannerName,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionUnknown,
		BufSize:                     1024, // TODO: Configurable.
	}
	switch param.DeliverPolicy.Policy.(type) {
	case *logpb.DeliverPolicy_All:
		consumerOption.SubscriptionInitialPosition = mqwrapper.SubscriptionPositionEarliest
	case *logpb.DeliverPolicy_Latest:
		consumerOption.SubscriptionInitialPosition = mqwrapper.SubscriptionPositionLatest
	}

	// Subscribe the MQ consumer.
	sub, err := sc.c.Subscribe(consumerOption)
	if err != nil {
		return nil, err
	}

	// Seek the MQ consumer.
	switch policy := param.DeliverPolicy.Policy.(type) {
	case *logpb.DeliverPolicy_StartFrom:
		messageID := message.NewMessageIDFromPBMessageID(policy.StartFrom)
		// Do a inslusive seek.
		if err := sub.Seek(messageID, true); err != nil {
			return nil, err
		}
	case *logpb.DeliverPolicy_StartAfter:
		messageID := message.NewMessageIDFromPBMessageID(policy.StartAfter)
		// Do a exclude seek.
		if err := sub.Seek(messageID, false); err != nil {
			return nil, err
		}
	}
	return newMQBasedScanner(param.Channel, sub), nil
}

func (sc *mqBasedScannerManagement) Drop(scannerName string) (err error) {
	// TODO: implement unsubcribe here.
	return errors.New("unimplemented")
}
