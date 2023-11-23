package producer

import (
	"io"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	producerStateRunning = iota
	producerStateClosed
)

// CreateProduceServer create a new producer.
// Expected message sequence:
// CreateProducer (Header)
// ProduceRequest 1 -> ProduceResponse Or Error 1
// ProduceRequest 2 -> ProduceResponse Or Error 2
// ProduceRequest 3 -> ProduceResponse Or Error 3
// CloseProducer
func CreateProduceServer(walManager walmanager.Manager, streamServer logpb.LogNodeHandlerService_ProduceServer) (*ProduceServer, error) {
	createReq, err := contextutil.GetCreateProducer(streamServer.Context())
	if err != nil {
		return nil, err
	}
	l, err := walManager.GetAvailableWAL(createReq.ChannelName, createReq.Term)
	if err != nil {
		return nil, err
	}

	produceServer := &produceGrpcServer{
		LogNodeHandlerService_ProduceServer: streamServer,
	}
	produceServer.SendCreated()
	return &ProduceServer{
		wal:              l,
		grpcStreamServer: produceServer,
		logger:           log.With(zap.String("channel", l.Channel().Name), zap.Int64("term", l.Channel().Term)),
		produceMessageCh: make(chan *logpb.ProduceMessageResponse),
		appendWG:         sync.WaitGroup{},
		sendExitCh:       make(chan struct{}),
	}, nil
}

// ProduceServer is a ProduceServer of log messages.
type ProduceServer struct {
	wal              wal.WALExtend
	grpcStreamServer *produceGrpcServer
	logger           *log.MLogger
	produceMessageCh chan *logpb.ProduceMessageResponse // All processing messages result should sent from theses channel.
	appendWG         sync.WaitGroup
	sendExitCh       chan struct{}
}

// Execute starts the producer.
func (p *ProduceServer) Execute() error {
	errSendCh := p.startSend()
	errRecvCh := p.startRecv()

	// Wait for send and recv arm to exit.
	err := <-errRecvCh
	<-errSendCh

	// Only return the error of recv arm.
	// error on send loop arm make no sense for client.
	return err
}

// startSend starts the send loop.
func (p *ProduceServer) startSend() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		_ = p.sendLoop()
		close(ch)
	}()
	return ch
}

// startRecv starts the recv loop.
func (p *ProduceServer) startRecv() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.recvLoop()
	}()
	return errCh
}

// sendLoop sends the message to client.
func (p *ProduceServer) sendLoop() (err error) {
	defer func() {
		if err != nil {
			p.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			p.logger.Info("send arm of stream closed")
		}
		close(p.sendExitCh)
	}()
	for {
		select {
		case resp, ok := <-p.produceMessageCh:
			if !ok {
				// all message has been sent, sent close response.
				p.grpcStreamServer.SendClosed()
				return nil
			}
			if err := p.grpcStreamServer.SendProduceMessage(resp); err != nil {
				return err
			}
		case <-p.grpcStreamServer.Context().Done():
			return errors.Wrap(p.grpcStreamServer.Context().Err(), "cancel send loop by stream server")
		}
	}
}

// recvLoop receives the message from client.
func (p *ProduceServer) recvLoop() (err error) {
	recvState := producerStateRunning
	defer func() {
		if err != nil {
			p.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
		} else {
			p.logger.Info("recv arm of stream closed")
		}
		p.appendWG.Wait()
		close(p.produceMessageCh)
	}()
	for {
		req, err := p.grpcStreamServer.Recv()
		if err == io.EOF {
			p.logger.Debug("stream closed by client")
			return nil
		}
		if err != nil {
			return err
		}
		switch req := req.Request.(type) {
		case *logpb.ProduceRequest_Produce:
			if recvState != producerStateRunning {
				return status.NewInvalidRequestSeq("unexpected stream rpc arrive sequence, wal already closed or not ready")
			}
			p.handleProduce(req.Produce)
		case *logpb.ProduceRequest_Close:
			if recvState != producerStateRunning {
				return status.NewInvalidRequestSeq("unexpected stream rpc arrive sequence, close unready wal")
			}
			recvState = producerStateClosed
		default:
			// skip message here.
			p.logger.Error("unknown request type", zap.Any("request", req))
		}
	}
}

// handleProduce handles the produce message request.
func (p *ProduceServer) handleProduce(req *logpb.ProduceMessageRequest) {
	p.logger.Debug("recv produce message from client", zap.Int64("requestID", req.RequestID))
	msg := message.NewMessageFromPBMessage(req.Message)

	// Append message to wal.
	// Concurrent append request can be executed concurrently.
	messageSize := msg.EstimateSize()
	now := time.Now()
	p.appendWG.Add(1)
	p.wal.AppendAsync(p.grpcStreamServer.Context(), msg, func(id message.MessageID, err error) {
		defer func() {
			p.appendWG.Done()
			if err == nil {
				metrics.LogNodeProduceBytes.WithLabelValues(paramtable.GetNodeIDString()).Observe(float64(messageSize))
			}
			metrics.LogNodeProduceDurationSeconds.WithLabelValues(paramtable.GetNodeIDString(), getStatusLabel(err)).Observe(float64(time.Since(now).Seconds()))
		}()
		p.sendProduceResult(req.RequestID, id, err)
	})
}

// sendProduceResult sends the produce result to client.
func (p *ProduceServer) sendProduceResult(reqID int64, id message.MessageID, err error) {
	resp := &logpb.ProduceMessageResponse{
		RequestID: reqID,
	}
	if err != nil {
		resp.Response = &logpb.ProduceMessageResponse_Error{
			Error: status.AsLogError(err).AsPBError(),
		}
	} else {
		resp.Response = &logpb.ProduceMessageResponse_Result{
			Result: &logpb.ProduceMessageResponseResult{
				Id: message.NewPBMessageIDFromMessageID(id),
			},
		}
	}

	// If sendExitCh is closed, it means the stream has been closed.
	// all pending response message should be dropped, client side will handle it.
	select {
	case p.produceMessageCh <- resp:
		p.logger.Debug("send produce message response to client", zap.Int64("requestID", reqID), zap.Any("messageID", id), zap.Error(err))
	case <-p.sendExitCh:
		p.logger.Warn("stream closed before produce message response sent", zap.Int64("requestID", reqID), zap.Any("messageID", id))
		return
	}
}

// getStatusLabel returns the status label of error.
func getStatusLabel(err error) string {
	if status.IsCanceled(err) {
		return metrics.CancelLabel
	}
	if err != nil {
		return metrics.FailLabel
	}
	return metrics.SuccessLabel
}
