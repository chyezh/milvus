package consumer

import (
	"io"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

const (
	consumerStateRunning = iota
	consumerStateClosed
)

// CreateConsumeServer create a new consumer.
// Expected message sequence:
// CreateConsumeServer:
// -> ConsumeResponse 1
// -> ConsumeResponse 2
// -> ConsumeResponse 3
// CloseConsumer:
func CreateConsumeServer(walManager walmanager.Manager, streamServer logpb.LogNodeHandlerService_ConsumeServer) (*ConsumeServer, error) {
	createReq, err := contextutil.GetCreateConsumer(streamServer.Context())
	if err != nil {
		return nil, errors.Wrap(err, "at get create consumer request")
	}

	l, err := walManager.GetAvailableWAL(createReq.ChannelName, createReq.Term)
	if err != nil {
		return nil, errors.Wrap(err, "at get available wal")
	}
	scanner, err := l.Read(streamServer.Context(), wal.ReadOption{
		DeliverPolicy: createReq.DeliverPolicy,
	})
	if err != nil {
		return nil, errors.Wrap(err, "at create scanner")
	}

	consumeServer := &consumeGrpcServerHelper{
		LogNodeHandlerService_ConsumeServer: streamServer,
	}
	if err := consumeServer.SendCreated(&logpb.CreateConsumerResponse{}); err != nil {
		return nil, errors.Wrap(err, "at send created")
	}
	return &ConsumeServer{
		scanner:                scanner,
		grpcStreamServerHelper: consumeServer,
		cancelConsumerCh:       make(chan struct{}),
		logger:                 log.With(zap.String("channel", l.Channel().Name), zap.Int64("term", l.Channel().Term)),
	}, nil
}

// ConsumeServer is a ConsumeServer of log messages.
type ConsumeServer struct {
	scanner wal.Scanner

	grpcStreamServerHelper *consumeGrpcServerHelper
	cancelConsumerCh       chan struct{}
	logger                 *log.MLogger
}

// Execute executes the consumer.
func (c *ConsumeServer) Execute() error {
	// Start a recv arm to handling the control message on background.
	go func() {
		// recv loop will be blocked until the stream is closed.
		// 1. close by client.
		// 2. close by server context cancel by return of outside Execute.
		_ = c.recvLoop()
	}()

	// Start a send loop on current main goroutine.
	// the loop will be blocked until:
	// 1. the stream is broken.
	// 2. recv arm recv close signal.
	// 3. scanner is quit with expected error.
	return c.sendLoop()
}

// sendLoop sends the message to client.
func (c *ConsumeServer) sendLoop() (err error) {
	defer func() {
		if err != nil {
			c.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			c.logger.Info("send arm of stream closed")
		}
		if err := c.scanner.Close(); err != nil {
			c.logger.Warn("close scanner failed", zap.Error(err))
		}
	}()
	// Read ahead buffer is implemented by scanner.
	// Do not add buffer here.
	for {
		select {
		case msg, ok := <-c.scanner.Chan():
			if !ok {
				return errors.Wrap(c.scanner.Error(), "at scanner")
			}
			// Send Consumed message to client and do metrics.
			messageSize := msg.EstimateSize()
			if err := c.grpcStreamServerHelper.SendConsumeMessage(&logpb.ConsumeMessageReponse{
				Id: message.NewPBMessageIDFromMessageID(msg.MessageID()),
				Message: &logpb.Message{
					Payload:    msg.Payload(),
					Properties: msg.Properties().ToRawMap(),
				},
			}); err != nil {
				return errors.Wrap(err, "at send consume message")
			}
			metrics.LogNodeConsumeBytes.WithLabelValues(paramtable.GetNodeIDString()).Observe(float64(messageSize))
		case <-c.cancelConsumerCh:
			return errors.Wrap(c.grpcStreamServerHelper.SendClosed(), "at send close")
		case <-c.grpcStreamServerHelper.Context().Done():
			return errors.Wrap(c.grpcStreamServerHelper.Context().Err(), "at grpc context done")
		}
	}
}

// recvLoop receives messages from client.
func (c *ConsumeServer) recvLoop() (err error) {
	recvState := consumerStateRunning
	defer func() {
		if err != nil {
			c.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
		} else {
			c.logger.Info("recv arm of stream closed")
		}
	}()

	for {
		req, err := c.grpcStreamServerHelper.Recv()
		if err == io.EOF {
			c.logger.Debug("stream closed by client")
			return nil
		}
		if err != nil {
			return err
		}
		switch req := req.Request.(type) {
		case *logpb.ConsumeRequest_Close:
			if recvState != consumerStateRunning {
				return status.NewInvalidRequestSeq("unexpected stream rpc arrive sequence, scanner already closed")
			}
			close(c.cancelConsumerCh)
		default:
			c.logger.Warn("unknown request type", zap.Any("request", req))
		}
	}
}
