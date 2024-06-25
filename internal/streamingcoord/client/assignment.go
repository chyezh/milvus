package client

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
)

type assignmentService struct {
	*clientImpl
}

func (c *assignmentService) AssignmentDiscover(ctx context.Context, cb func(*streamingpb.AssignmentDiscoverResponse) error) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("assignment client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	assignment, err := c.getAssignmentService(ctx)
	if err != nil {
		return errors.Wrap(err, "at creating assignment service")
	}

	// create stream.
	listener, err := assignment.AssignmentDiscover(ctx, &streamingpb.AssignmentDiscoverRequest{})
	if err != nil {
		return errors.Wrap(err, "at creating stream")
	}

	// receive stream.
	for {
		resp, err := listener.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return errors.Wrap(err, "at receiving the next discovery")
		}
		if err := cb(resp); err != nil {
			return errors.Wrap(err, "at callback")
		}
	}
}

func (c *assignmentService) ReportStreamingError(ctx context.Context, req *streamingpb.ReportStreamingErrorRequest) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("assignment client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	assignment, err := c.getAssignmentService(ctx)
	if err != nil {
		return errors.Wrap(err, "at creating assignment service")
	}

	_, err = assignment.ReportStreamingError(ctx, req)
	return err
}
