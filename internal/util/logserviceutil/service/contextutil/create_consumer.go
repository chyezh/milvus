package contextutil

import (
	"context"
	"encoding/base64"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"google.golang.org/grpc/metadata"
)

const (
	createConsumerKey = "create-consumer"
)

// WithCreateConsumer attaches create consumer request to context.
func WithCreateConsumer(ctx context.Context, req *logpb.CreateConsumerRequest) context.Context {
	bytes, _ := proto.Marshal(req)
	// use base64 encoding to transfer binary to text.
	msg := base64.StdEncoding.EncodeToString(bytes)
	return metadata.AppendToOutgoingContext(ctx, createConsumerKey, msg)
}

// GetCreateConsumer gets create consumer request from context.
func GetCreateConsumer(ctx context.Context) (*logpb.CreateConsumerRequest, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("create consumer metadata not found from incoming context")
	}
	msg := md.Get(createConsumerKey)
	if len(msg) == 0 {
		return nil, errors.New("create consumer metadata not found")
	}

	bytes, err := base64.StdEncoding.DecodeString(msg[0])
	if err != nil {
		return nil, errors.Wrap(err, "decode create consumer metadata failed")
	}

	req := &logpb.CreateConsumerRequest{}
	if err := proto.Unmarshal(bytes, req); err != nil {
		return nil, errors.Wrap(err, "unmarshal create consumer request failed")
	}
	return req, nil
}