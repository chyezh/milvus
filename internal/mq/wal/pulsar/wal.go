package pulsar

import (
	"context"

	gopulsar "github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"go.uber.org/zap"
)

var _ walimpls.WALImpls = (*walImpl)(nil)

type walImpl struct {
	*helper.WALHelper
	c gopulsar.Client
	p gopulsar.Producer
}

func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	id, err := w.p.Send(ctx, &gopulsar.ProducerMessage{
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
	ch := make(chan gopulsar.ReaderMessage, 1024)
	readerOpt := gopulsar.ReaderOptions{
		Topic:          w.Channel().GetName(),
		Name:           opt.Name,
		MessageChannel: ch,
	}

	switch policy := opt.DeliverPolicy.Policy.(type) {
	case *logpb.DeliverPolicy_All:
		readerOpt.StartMessageID = gopulsar.EarliestMessageID()
	case *logpb.DeliverPolicy_Latest:
		readerOpt.StartMessageID = gopulsar.LatestMessageID()
	case *logpb.DeliverPolicy_StartFrom:
		id, err := unmarshalMessageID(policy.StartFrom.GetId())
		if err != nil {
			return nil, err
		}
		readerOpt.StartMessageID = id.MessageID
		readerOpt.StartMessageIDInclusive = true
	case *logpb.DeliverPolicy_StartAfter:
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
