package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
)

var _ walimpls.WALImpls = (*walImpl)(nil)

type walImpl struct {
	*helper.WALHelper
	c pulsar.Client
	p pulsar.Producer
}

func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	id, err := w.p.Send(ctx, &pulsar.ProducerMessage{
		Payload:    msg.Payload(),
		Properties: msg.Properties().ToRawMap(),
	})
	if err != nil {
		w.Log().RatedWarn(1, "send message to pulsar failed", zap.Error(err))
		return nil, err
	}
	return pulsarID{id}, nil
}

func (w *walImpl) Read(ctx context.Context, opt walimpls.ReadOption) (s walimpls.ScannerImpls, err error) {
	ch := make(chan pulsar.ReaderMessage, 1024)
	readerOpt := pulsar.ReaderOptions{
		Topic:             w.Channel().GetName(),
		Name:              opt.Name,
		MessageChannel:    ch,
		ReceiverQueueSize: opt.ReadAheadBufferSize,
	}

	switch policy := opt.DeliverPolicy.Policy.(type) {
	case *streamingpb.DeliverPolicy_All:
		readerOpt.StartMessageID = pulsar.EarliestMessageID()
	case *streamingpb.DeliverPolicy_Latest:
		readerOpt.StartMessageID = pulsar.LatestMessageID()
	case *streamingpb.DeliverPolicy_StartFrom:
		id, err := unmarshalMessageID(policy.StartFrom.GetId())
		if err != nil {
			return nil, err
		}
		readerOpt.StartMessageID = id.MessageID
		readerOpt.StartMessageIDInclusive = true
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err := unmarshalMessageID(policy.StartAfter.GetId())
		if err != nil {
			return nil, err
		}
		readerOpt.StartMessageID = id.MessageID
		readerOpt.StartMessageIDInclusive = false
	}
	reader, err := w.c.CreateReader(readerOpt)
	if err != nil {
		return nil, err
	}
	return newScanner(opt.Name, reader), nil
}

func (w *walImpl) Close() {
	w.p.Close() // close all producer
}
