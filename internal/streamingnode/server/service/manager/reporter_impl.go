package manager

import "github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"

// newReporter creates a new reporter.
func newReporter(closed chan struct{}, assignmentReportCh chan walmanager.SyncResponse) walmanager.Reporter {
	return &reporterImpl{
		closed:             make(chan struct{}),
		assignmentReportCh: make(chan walmanager.SyncResponse),
	}
}

// reporterImpl is the implementation of walmanager.Reporter.
type reporterImpl struct {
	closed             chan struct{}
	assignmentReportCh chan walmanager.SyncResponse
}

func (r *reporterImpl) Report(resp walmanager.SyncResponse) {
	select {
	case r.assignmentReportCh <- resp:
	case r.closed <- struct{}{}:
		// The report result can be lost if the related stream broken.
		// The coordinator will retry the assignment if the stream broken.
	}
}
