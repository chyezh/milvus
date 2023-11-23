package client

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"go.uber.org/zap"
)

type assignmentService struct {
	*clientImpl
}

func (c *assignmentService) AssignmentDiscover(ctx context.Context, watcher chan<- *logpb.AssignmentDiscoverResponse) error {
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
	listener, err := assignment.AssignmentDiscover(ctx, &logpb.AssignmentDiscoverRequest{})
	if err != nil {
		return errors.Wrap(err, "at creating stream")
	}

	go func() {
		for {
			resp, err := listener.Recv()
			if err != nil {
				log.Warn("at receiving the next discovery", zap.Error(err))
				break
			}
			watcher <- resp
		}
	}()
	return nil
}

func (c *assignmentService) ReportLogError(ctx context.Context, req *logpb.ReportLogErrorRequest) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("assignment client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	assignment, err := c.getAssignmentService(ctx)
	if err != nil {
		return errors.Wrap(err, "at creating assignment service")
	}

	_, err = assignment.ReportLogError(ctx, req)
	return err
}
