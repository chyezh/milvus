package rmq

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/client"
)

var _ walimpls.OpenerImpls = (*openerImpl)(nil)

// openerImpl is the implementation of walimpls.Opener interface.
type openerImpl struct {
	c client.Client
}

// Open opens a new wal.
func (o *openerImpl) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	p, err := o.c.CreateProducer(client.ProducerOptions{
		Topic: opt.Channel.Name,
	})
	if err != nil {
		return nil, err
	}
	return &walImpl{
		WALHelper: helper.NewWALHelper(opt),
		p:         p,
		c:         o.c,
	}, nil
}

// Close closes the opener resources.
func (o *openerImpl) Close() {
	o.c.Close()
}
