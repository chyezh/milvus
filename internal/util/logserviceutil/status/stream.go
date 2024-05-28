package status

import (
	"google.golang.org/grpc"
)

// NewClientStreamWrapper returns a grpc.ClientStream that wraps the given stream.
func NewClientStreamWrapper(method string, stream grpc.ClientStream) grpc.ClientStream {
	if stream == nil {
		return nil
	}
	return &clientStreamWrapper{
		method:       method,
		ClientStream: stream,
	}
}

type clientStreamWrapper struct {
	method string
	grpc.ClientStream
}

// Convert the error to a Status and return it.
func (s *clientStreamWrapper) SendMsg(m interface{}) error {
	err := s.ClientStream.SendMsg(m)
	return ConvertLogError(s.method, err)
}

// Convert the error to a Status and return it.
func (s *clientStreamWrapper) RecvMsg(m interface{}) error {
	err := s.ClientStream.RecvMsg(m)
	return ConvertLogError(s.method, err)
}
