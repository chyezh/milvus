package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/scanner"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/writer"
	smsgstream "github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

// TODO: constructors for all mqBasedOpener, remove mqwrapper layer.
func NewMQBasedOpener() *mqBasedOpener {
	cli, err := newMQClient()
	if err != nil {
		panic(err)
	}
	return &mqBasedOpener{
		c: cli,
	}
}

func newMQClient() (mqwrapper.Client, error) {
	mqType := util.MustSelectMQType()
	log.Info("select mq type", zap.String("mqType", mqType))

	params := paramtable.Get()
	switch mqType {
	case util.MQTypeNatsmq:
		return msgstream.NewNatsmqFactory().NewClient(context.TODO())
	case util.MQTypeRocksmq:
		return smsgstream.NewRocksmqFactory(params.RocksmqCfg.Path.GetValue(), &params.ServiceParam).NewClient(context.TODO())
	case util.MQTypePulsar:
		return msgstream.NewPmsFactory(&params.ServiceParam).NewClient(context.TODO())
	case util.MQTypeKafka:
		return msgstream.NewKmsFactory(&params.ServiceParam).NewClient(context.TODO())
	default:
		panic("unknown mq type")
	}
}

// mqBasedOpener is the opener implementation based on message queue.
type mqBasedOpener struct {
	c mqwrapper.Client
}

// OpenMQBasedWAL creates a new wal instance based on message queue.
func (o *mqBasedOpener) Open(channel logpb.PChannelInfo) (WAL, error) {
	p, err := o.c.CreateProducer(mqwrapper.ProducerOptions{
		Topic: channel.Name,
	})
	if err != nil {
		return nil, status.NewInner(err.Error())
	}

	return &walImpl{
		Writer:         writer.NewMQBasedWriter(p, channel),
		scannerManager: newScannerManager(scanner.NewMQBasedManagement(o.c, channel), channel),
	}, nil
}

// Close closes the opener resources.
func (o *mqBasedOpener) Close() {
	o.c.Close()
}
