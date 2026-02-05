package wal

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// OpenerBuilder is the interface for build wal opener.
type OpenerBuilder interface {
	// Name of the wal builder, should be a lowercase string.
	Name() message.WALName

	Build() (Opener, error)
}

// StateProgressReporter is the interface for reporting state progress during WAL opening.
type StateProgressReporter interface {
	// ReportProgress reports the current state and optional progress.
	ReportProgress(state streamingpb.AssignmentState, progress *streamingpb.StreamRecoveringProgress) error
}

// noopStateReporter is a no-op implementation of StateProgressReporter.
type noopStateReporter struct{}

func (n *noopStateReporter) ReportProgress(_ streamingpb.AssignmentState, _ *streamingpb.StreamRecoveringProgress) error {
	return nil
}

// NoopStateReporter returns a no-op state reporter.
func NoopStateReporter() StateProgressReporter {
	return &noopStateReporter{}
}

// OpenOption is the option for allocating wal instance.
type OpenOption struct {
	Channel        types.PChannelInfo
	DisableFlusher bool                  // disable flusher for test, only use in test.
	StateReporter  StateProgressReporter // optional state reporter for progress updates
}

// GetStateReporter returns the state reporter, or a noop reporter if nil.
func (o *OpenOption) GetStateReporter() StateProgressReporter {
	if o.StateReporter != nil {
		return o.StateReporter
	}
	return NoopStateReporter()
}

// Opener is the interface for build wal instance.
type Opener interface {
	// Open open a wal instance.
	Open(ctx context.Context, opt *OpenOption) (WAL, error)

	// Close closes the opener resources.
	Close()
}
