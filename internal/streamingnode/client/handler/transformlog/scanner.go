package transformlog

import (
	"context"
	"io"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type ScannerOptions struct {
	Assignment *types.PChannelInfoAssigned
	ReadOption transformlogapi.ReadOption
}

func CreateScanner(
	ctx context.Context,
	opts *ScannerOptions,
	handlerClient streamingpb.StreamingNodeHandlerServiceClient,
) (transformlogapi.Scanner, error) {
	ctx = contextutil.WithCreateTransformStream(ctx, &streamingpb.CreateTransformStreamRequest{
		Pchannel: types.NewProtoFromPChannelInfo(opts.Assignment.Channel),
	})
	streamClient, err := handlerClient.SubscribeTransform(ctx, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		return nil, err
	}
	scanner := &remoteScanner{
		name: opts.ReadOption.Name,
		subscriptionID: 1,
		vchannel: opts.ReadOption.VChannel,
		stream: streamClient,
		ch: make(chan transformlogapi.Event, 16),
		finishErr: syncutil.NewFuture[error](),
		closeOnce: sync.Once{},
	}
	if err := streamClient.Send(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_Create{
			Create: &streamingpb.CreateTransformSubscriptionRequest{
				SubscriptionId: scanner.subscriptionID,
				Vchannel: scanner.vchannel,
				StartAfterTimeTick: opts.ReadOption.StartAfterTimeTick,
			},
		},
	}); err != nil {
		_ = streamClient.CloseSend()
		return nil, err
	}
	go scanner.recvLoop()
	return scanner, nil
}

type remoteScanner struct {
	name string
	subscriptionID int64
	vchannel string
	stream streamingpb.StreamingNodeHandlerService_SubscribeTransformClient
	ch chan transformlogapi.Event
	finishErr *syncutil.Future[error]
	closeOnce sync.Once
}

func (s *remoteScanner) Name() string {
	return s.name
}

func (s *remoteScanner) Chan() <-chan transformlogapi.Event {
	return s.ch
}

func (s *remoteScanner) Error() error {
	return s.finishErr.Get()
}

func (s *remoteScanner) Done() <-chan struct{} {
	return s.finishErr.Done()
}

func (s *remoteScanner) Close() error {
	s.closeOnce.Do(func() {
		_ = s.stream.Send(&streamingpb.TransformRequest{
			Request: &streamingpb.TransformRequest_CloseSubscription{
				CloseSubscription: &streamingpb.CloseTransformSubscriptionRequest{SubscriptionId: s.subscriptionID},
			},
		})
		_ = s.stream.CloseSend()
	})
	return s.finishErr.Get()
}

func (s *remoteScanner) recvLoop() {
	var err error
	defer func() {
		close(s.ch)
		if errors.Is(err, io.EOF) {
			err = nil
		}
		s.finishErr.Set(err)
	}()
	for {
		resp, recvErr := s.stream.Recv()
		if recvErr != nil {
			err = recvErr
			return
		}
		switch resp := resp.GetResponse().(type) {
		case *streamingpb.TransformResponse_Create:
			continue
		case *streamingpb.TransformResponse_MessageBatch:
			if resp.MessageBatch.GetSubscriptionId() != s.subscriptionID {
				continue
			}
			for _, entry := range resp.MessageBatch.GetEntries() {
				s.ch <- transformlogapi.Event{Entry: entry}
			}
		case *streamingpb.TransformResponse_CaughtUp:
			if resp.CaughtUp.GetSubscriptionId() != s.subscriptionID {
				continue
			}
			s.ch <- transformlogapi.Event{
				CaughtUp: &transformlogapi.CaughtUp{
					StartAfterTimeTick: resp.CaughtUp.GetStartAfterTimeTick(),
				},
			}
		case *streamingpb.TransformResponse_SubscriptionError:
			if resp.SubscriptionError.GetSubscriptionId() == s.subscriptionID {
				err = status.AsStreamingError((*status.StreamingError)(resp.SubscriptionError.GetError()))
				return
			}
		case *streamingpb.TransformResponse_CloseStream:
			err = nil
			return
		}
	}
}
