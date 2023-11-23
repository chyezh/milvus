package consumer

import "github.com/milvus-io/milvus/internal/proto/logpb"

// consumeGrpcServer is a wrapped consumer server of log messages.
type consumeGrpcServer struct {
	logpb.LogNodeHandlerService_ConsumeServer
}

// SendConsumeMessage sends the consume result to client.
func (p *consumeGrpcServer) SendConsumeMessage(resp *logpb.ConsumeMessageReponse) error {
	return p.Send(&logpb.ConsumeResponse{
		Response: &logpb.ConsumeResponse_Consume{
			Consume: resp,
		},
	})
}

// SendCreated sends the create response to client.
func (p *consumeGrpcServer) SendCreated(resp *logpb.CreateConsumerResponse) error {
	return p.Send(&logpb.ConsumeResponse{
		Response: &logpb.ConsumeResponse_Create{
			Create: resp,
		},
	})
}

// SendClosed sends the close response to client.
// no more message should be sent after sending close response.
func (p *consumeGrpcServer) SendClosed() error {
	// wait for all consume messages are processed.
	return p.Send(&logpb.ConsumeResponse{
		Response: &logpb.ConsumeResponse_Close{
			Close: &logpb.CloseConsumerResponse{},
		},
	})
}
