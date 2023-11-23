package consumer

import (
	"io"

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
		return nil, err
	}

	l, err := walManager.GetAvailableWAL(createReq.ChannelName, createReq.Term)
	if err != nil {
		return nil, err
	}
	scanner, err := l.Read(streamServer.Context(), wal.ReadOption{
		DeliverPolicy: createReq.DeliverPolicy,
	})
	if err != nil {
		return nil, err
	}

	consumeServer := &consumeGrpcServer{
		LogNodeHandlerService_ConsumeServer: streamServer,
	}
	consumeServer.SendCreated(&logpb.CreateConsumerResponse{})
	return &ConsumeServer{
		scanner:          scanner,
		grpcStreamServer: consumeServer,
		cancelConsumerCh: make(chan struct{}),
		closeCh:          make(chan struct{}),
		logger:           log.With(zap.String("channel", l.Channel().Name), zap.Int64("term", l.Channel().Term)),
	}, nil
}

// ConsumeServer is a ConsumeServer of log messages.
type ConsumeServer struct {
	scanner wal.Scanner

	grpcStreamServer *consumeGrpcServer
	cancelConsumerCh chan struct{}
	closeCh          chan struct{}
	logger           *log.MLogger
}

// Execute executes the consumer.
func (c *ConsumeServer) Execute() (err error) {
	errSendCh := c.startSend()
	errRecvCh := c.startRecv()

	// wait for send or recv loop exit.
	select {
	case <-errSendCh:
	case err = <-errRecvCh:
	}

	// notify close on another arm to exit.
	c.close()

	// wait for another arm exit.
	select {
	case <-errSendCh:
	case err = <-errRecvCh:
	}

	// return recv error at high priority.
	if err != nil {
		return err
	}
	// return scanner error.
	return c.scanner.Error()
}

// startSend starts the send loop.
func (c *ConsumeServer) startSend() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		_ = c.sendLoop()
		close(ch)
	}()
	return ch
}

// startRecv starts the recv loop.
func (c *ConsumeServer) startRecv() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.recvLoop()
	}()
	return errCh
}

// close closes the consumer.
func (c *ConsumeServer) close() {
	close(c.closeCh)
	if err := c.scanner.Close(); err != nil {
		c.logger.Warn("close scanner failed", zap.Error(err))
	}
}

// sendLoop sends the message to client.
func (c *ConsumeServer) sendLoop() (err error) {
	defer func() {
		if err != nil {
			c.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			c.logger.Info("send arm of stream closed")
		}
	}()
	// Read ahead buffer is implemented by scanner.
	// Do not need to add buffer here.
	for {
		select {
		case msg, ok := <-c.scanner.Chan():
			if !ok {
				return nil
			}
			// Send Consumed message to client and do metrics.
			messageSize := msg.EstimateSize()
			if err := c.grpcStreamServer.SendConsumeMessage(&logpb.ConsumeMessageReponse{
				Id: message.NewPBMessageIDFromMessageID(msg.MessageID()),
				Message: &logpb.Message{
					Payload:    msg.Payload(),
					Properties: msg.Properties().ToRawMap(),
				},
			}); err != nil {
				return err
			}
			metrics.LogNodeConsumeBytes.WithLabelValues(paramtable.GetNodeIDString()).Observe(float64(messageSize))
		case <-c.cancelConsumerCh:
			return c.grpcStreamServer.SendClosed()
		case <-c.grpcStreamServer.Context().Done():
			return c.grpcStreamServer.SendClosed()
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
		req, err := c.grpcStreamServer.Recv()
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
