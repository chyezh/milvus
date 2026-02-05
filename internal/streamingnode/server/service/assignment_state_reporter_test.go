//go:build test

package service

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

// mockAssignWithStateReportServer is a mock implementation of
// streamingpb.StreamingNodeManagerService_AssignWithStateReportServer for testing.
type mockAssignWithStateReportServer struct {
	ctx         context.Context
	sendFunc    func(*streamingpb.AssignmentStateResponse) error
	sentMsgs    []*streamingpb.AssignmentStateResponse
	headerMD    metadata.MD
	trailerMD   metadata.MD
	recvMsgFunc func(interface{}) error
	sendMsgFunc func(interface{}) error
}

func newMockAssignWithStateReportServer(ctx context.Context) *mockAssignWithStateReportServer {
	return &mockAssignWithStateReportServer{
		ctx:      ctx,
		sentMsgs: make([]*streamingpb.AssignmentStateResponse, 0),
	}
}

func (m *mockAssignWithStateReportServer) Send(resp *streamingpb.AssignmentStateResponse) error {
	m.sentMsgs = append(m.sentMsgs, resp)
	if m.sendFunc != nil {
		return m.sendFunc(resp)
	}
	return nil
}

func (m *mockAssignWithStateReportServer) SetHeader(md metadata.MD) error {
	m.headerMD = md
	return nil
}

func (m *mockAssignWithStateReportServer) SendHeader(md metadata.MD) error {
	m.headerMD = md
	return nil
}

func (m *mockAssignWithStateReportServer) SetTrailer(md metadata.MD) {
	m.trailerMD = md
}

func (m *mockAssignWithStateReportServer) Context() context.Context {
	return m.ctx
}

func (m *mockAssignWithStateReportServer) SendMsg(msg interface{}) error {
	if m.sendMsgFunc != nil {
		return m.sendMsgFunc(msg)
	}
	return nil
}

func (m *mockAssignWithStateReportServer) RecvMsg(msg interface{}) error {
	if m.recvMsgFunc != nil {
		return m.recvMsgFunc(msg)
	}
	return nil
}

func TestNewAssignmentStateReporter(t *testing.T) {
	ctx := context.Background()
	mockStream := newMockAssignWithStateReportServer(ctx)

	reporter := NewAssignmentStateReporter(mockStream)

	assert.NotNil(t, reporter)
	assert.Equal(t, mockStream, reporter.stream)
	assert.Equal(t, defaultStateReportInterval, reporter.interval)
	assert.NotNil(t, reporter.logger)
}

func TestAssignmentStateReporter_ReportProgress(t *testing.T) {
	testCases := []struct {
		name     string
		state    streamingpb.AssignmentState
		progress *streamingpb.StreamRecoveringProgress
	}{
		{
			name:     "FENCING state",
			state:    streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING,
			progress: nil,
		},
		{
			name:     "PERSIST_RECOVERING state",
			state:    streamingpb.AssignmentState_ASSIGNMENT_STATE_PERSIST_RECOVERING,
			progress: nil,
		},
		{
			name:     "STREAM_RECOVERING state",
			state:    streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING,
			progress: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mockStream := newMockAssignWithStateReportServer(ctx)
			reporter := NewAssignmentStateReporter(mockStream)

			err := reporter.ReportProgress(tc.state, tc.progress)

			assert.NoError(t, err)
			assert.Len(t, mockStream.sentMsgs, 1)

			sentResp := mockStream.sentMsgs[0]
			progress, ok := sentResp.Response.(*streamingpb.AssignmentStateResponse_Progress)
			assert.True(t, ok)
			assert.Equal(t, tc.state, progress.Progress.State)
			assert.Nil(t, progress.Progress.StreamRecoveringProgress)
		})
	}
}

func TestAssignmentStateReporter_ReportProgressWithStreamRecoveringProgress(t *testing.T) {
	ctx := context.Background()
	mockStream := newMockAssignWithStateReportServer(ctx)
	reporter := NewAssignmentStateReporter(mockStream)

	streamProgress := &streamingpb.StreamRecoveringProgress{
		RecoveredBytes:    1024,
		TotalBytes:        4096,
		RecoveredMessages: 100,
		TotalMessages:     400,
	}

	err := reporter.ReportProgress(streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING, streamProgress)

	assert.NoError(t, err)
	assert.Len(t, mockStream.sentMsgs, 1)

	sentResp := mockStream.sentMsgs[0]
	progress, ok := sentResp.Response.(*streamingpb.AssignmentStateResponse_Progress)
	assert.True(t, ok)
	assert.Equal(t, streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING, progress.Progress.State)
	assert.NotNil(t, progress.Progress.StreamRecoveringProgress)
	assert.Equal(t, int64(1024), progress.Progress.StreamRecoveringProgress.RecoveredBytes)
	assert.Equal(t, int64(4096), progress.Progress.StreamRecoveringProgress.TotalBytes)
	assert.Equal(t, int64(100), progress.Progress.StreamRecoveringProgress.RecoveredMessages)
	assert.Equal(t, int64(400), progress.Progress.StreamRecoveringProgress.TotalMessages)
}

func TestAssignmentStateReporter_ReportReady(t *testing.T) {
	ctx := context.Background()
	mockStream := newMockAssignWithStateReportServer(ctx)
	reporter := NewAssignmentStateReporter(mockStream)

	err := reporter.ReportReady()

	assert.NoError(t, err)
	assert.Len(t, mockStream.sentMsgs, 1)

	sentResp := mockStream.sentMsgs[0]
	ready, ok := sentResp.Response.(*streamingpb.AssignmentStateResponse_Ready)
	assert.True(t, ok)
	assert.NotNil(t, ready.Ready)
}

func TestAssignmentStateReporter_ReportError(t *testing.T) {
	ctx := context.Background()
	mockStream := newMockAssignWithStateReportServer(ctx)
	reporter := NewAssignmentStateReporter(mockStream)

	testErr := errors.New("test error")
	err := reporter.ReportError(testErr)

	assert.NoError(t, err)
	assert.Len(t, mockStream.sentMsgs, 1)

	sentResp := mockStream.sentMsgs[0]
	errResp, ok := sentResp.Response.(*streamingpb.AssignmentStateResponse_Error)
	assert.True(t, ok)
	assert.NotNil(t, errResp.Error)
	// The error should be converted to a StreamingError
	assert.Contains(t, errResp.Error.Cause, "test error")
}

func TestAssignmentStateReporter_ReportProgress_SendError(t *testing.T) {
	ctx := context.Background()
	mockStream := newMockAssignWithStateReportServer(ctx)
	expectedErr := errors.New("send failed")
	mockStream.sendFunc = func(resp *streamingpb.AssignmentStateResponse) error {
		return expectedErr
	}
	reporter := NewAssignmentStateReporter(mockStream)

	err := reporter.ReportProgress(streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING, nil)

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestAssignmentStateReporter_ReportReady_SendError(t *testing.T) {
	ctx := context.Background()
	mockStream := newMockAssignWithStateReportServer(ctx)
	expectedErr := errors.New("send ready failed")
	mockStream.sendFunc = func(resp *streamingpb.AssignmentStateResponse) error {
		return expectedErr
	}
	reporter := NewAssignmentStateReporter(mockStream)

	err := reporter.ReportReady()

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestAssignmentStateReporter_ReportError_SendError(t *testing.T) {
	ctx := context.Background()
	mockStream := newMockAssignWithStateReportServer(ctx)
	expectedSendErr := errors.New("send error failed")
	mockStream.sendFunc = func(resp *streamingpb.AssignmentStateResponse) error {
		return expectedSendErr
	}
	reporter := NewAssignmentStateReporter(mockStream)

	testErr := errors.New("original error")
	err := reporter.ReportError(testErr)

	assert.Error(t, err)
	assert.Equal(t, expectedSendErr, err)
}

func TestAssignmentStateReporter_Context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStream := newMockAssignWithStateReportServer(ctx)
	reporter := NewAssignmentStateReporter(mockStream)

	reporterCtx := reporter.Context()

	assert.Equal(t, ctx, reporterCtx)
}

func TestAssignmentStateReporter_Interval(t *testing.T) {
	ctx := context.Background()
	mockStream := newMockAssignWithStateReportServer(ctx)
	reporter := NewAssignmentStateReporter(mockStream)

	interval := reporter.Interval()

	assert.Equal(t, 1*time.Second, interval)
	assert.Equal(t, defaultStateReportInterval, interval)
}
