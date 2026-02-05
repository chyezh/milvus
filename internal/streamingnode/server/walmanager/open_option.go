package walmanager

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

// StateProgressReporter is a callback interface for reporting state progress.
type StateProgressReporter interface {
	// ReportProgress reports the current state and optional progress.
	ReportProgress(state streamingpb.AssignmentState, progress *streamingpb.StreamRecoveringProgress) error
}

// OpenOption contains options for opening a WAL.
type OpenOption struct {
	// StateReporter is an optional callback for reporting state progress.
	// If nil, no progress reporting is done.
	StateReporter StateProgressReporter
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
