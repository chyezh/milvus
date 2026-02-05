//go:build test

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

// mockAssignWithStateReportClient is a mock implementation of StreamingNodeManagerService_AssignWithStateReportClient
type mockAssignWithStateReportClient struct {
	grpc.ClientStream
	responses []*streamingpb.AssignmentStateResponse
	index     int
	err       error
}

func (m *mockAssignWithStateReportClient) Recv() (*streamingpb.AssignmentStateResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.index >= len(m.responses) {
		return nil, io.EOF
	}
	resp := m.responses[m.index]
	m.index++
	return resp, nil
}

func TestAssignHandler_Success(t *testing.T) {
	client := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	pchannel := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "test-channel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 123},
	}

	// Setup mock stream with Progress -> Ready sequence
	mockStream := &mockAssignWithStateReportClient{
		responses: []*streamingpb.AssignmentStateResponse{
			{
				Response: &streamingpb.AssignmentStateResponse_Progress{
					Progress: &streamingpb.AssignmentProgress{
						State: streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING,
					},
				},
			},
			{
				Response: &streamingpb.AssignmentStateResponse_Progress{
					Progress: &streamingpb.AssignmentProgress{
						State: streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING,
					},
				},
			},
			{
				Response: &streamingpb.AssignmentStateResponse_Ready{
					Ready: &streamingpb.AssignmentReady{},
				},
			},
		},
	}

	client.EXPECT().AssignWithStateReport(mock.Anything, mock.Anything).Return(mockStream, nil)

	handler := newAssignHandler(client, pchannel)
	err := handler.Execute(context.Background())
	assert.NoError(t, err)
}

func TestAssignHandler_Error(t *testing.T) {
	client := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	pchannel := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "test-channel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 123},
	}

	// Setup mock stream that returns Progress -> Error
	mockStream := &mockAssignWithStateReportClient{
		responses: []*streamingpb.AssignmentStateResponse{
			{
				Response: &streamingpb.AssignmentStateResponse_Progress{
					Progress: &streamingpb.AssignmentProgress{
						State: streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING,
					},
				},
			},
			{
				Response: &streamingpb.AssignmentStateResponse_Error{
					Error: &streamingpb.StreamingError{
						Code:  streamingpb.StreamingCode_STREAMING_CODE_INNER,
						Cause: "internal error",
					},
				},
			},
		},
	}

	client.EXPECT().AssignWithStateReport(mock.Anything, mock.Anything).Return(mockStream, nil)

	handler := newAssignHandler(client, pchannel)
	err := handler.Execute(context.Background())
	assert.Error(t, err)

	// Verify it's a StreamingError
	var streamingErr *status.StreamingError
	assert.True(t, errors.As(err, &streamingErr))
}

func TestAssignHandler_Unimplemented(t *testing.T) {
	client := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	pchannel := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "test-channel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 123},
	}

	// Return Unimplemented error on AssignWithStateReport call
	unimplementedErr := grpcstatus.Error(codes.Unimplemented, "method not implemented")
	client.EXPECT().AssignWithStateReport(mock.Anything, mock.Anything).Return(nil, unimplementedErr)

	handler := newAssignHandler(client, pchannel)
	err := handler.Execute(context.Background())
	assert.Error(t, err)

	// Should not retry on Unimplemented error
	assert.True(t, isUnimplemented(err))
}

func TestAssignHandler_ContextCanceled(t *testing.T) {
	client := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	pchannel := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "test-channel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 123},
	}

	// Return context canceled error
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Mock returns network error that would normally trigger retry
	mockStream := &mockAssignWithStateReportClient{
		err: errors.New("network error"),
	}
	client.EXPECT().AssignWithStateReport(mock.Anything, mock.Anything).Return(mockStream, nil).Maybe()

	handler := newAssignHandler(client, pchannel)
	err := handler.Execute(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestAssignHandler_StreamDisconnect(t *testing.T) {
	client := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	pchannel := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "test-channel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 123},
	}

	// First call: stream closes unexpectedly
	disconnectStream := &mockAssignWithStateReportClient{
		responses: []*streamingpb.AssignmentStateResponse{
			{
				Response: &streamingpb.AssignmentStateResponse_Progress{
					Progress: &streamingpb.AssignmentProgress{
						State: streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING,
					},
				},
			},
		},
		// Will return io.EOF after Progress, causing unexpected disconnect
	}

	// Second call: succeeds
	successStream := &mockAssignWithStateReportClient{
		responses: []*streamingpb.AssignmentStateResponse{
			{
				Response: &streamingpb.AssignmentStateResponse_Ready{
					Ready: &streamingpb.AssignmentReady{},
				},
			},
		},
	}

	callCount := 0
	client.EXPECT().AssignWithStateReport(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *streamingpb.StreamingNodeManagerAssignRequest, opts ...grpc.CallOption) (streamingpb.StreamingNodeManagerService_AssignWithStateReportClient, error) {
			callCount++
			if callCount == 1 {
				return disconnectStream, nil
			}
			return successStream, nil
		},
	).Times(2)

	handler := newAssignHandler(client, pchannel)
	err := handler.Execute(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, callCount)
}

func TestIsUnimplemented(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "Unimplemented gRPC error",
			err:      grpcstatus.Error(codes.Unimplemented, "method not implemented"),
			expected: true,
		},
		{
			name:     "wrapped Unimplemented error",
			err:      errors.Wrap(grpcstatus.Error(codes.Unimplemented, "method not implemented"), "wrapper"),
			expected: true,
		},
		{
			name:     "Other gRPC error",
			err:      grpcstatus.Error(codes.Unavailable, "service unavailable"),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("some error"),
			expected: false,
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: false,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		{
			name:     "deeply wrapped Unimplemented error",
			err:      errors.Wrap(errors.Wrap(grpcstatus.Error(codes.Unimplemented, "not implemented"), "level1"), "level2"),
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isUnimplemented(tc.err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestAssignHandler_ShouldRetry(t *testing.T) {
	client := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	pchannel := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "test-channel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 123},
	}
	handler := newAssignHandler(client, pchannel)

	tests := []struct {
		name        string
		err         error
		shouldRetry bool
	}{
		{
			name:        "context canceled",
			err:         context.Canceled,
			shouldRetry: false,
		},
		{
			name:        "context deadline exceeded",
			err:         context.DeadlineExceeded,
			shouldRetry: false,
		},
		{
			name:        "StreamingError",
			err:         status.New(streamingpb.StreamingCode_STREAMING_CODE_INNER, "internal error"),
			shouldRetry: false,
		},
		{
			name:        "Unimplemented gRPC error",
			err:         grpcstatus.Error(codes.Unimplemented, "method not implemented"),
			shouldRetry: false,
		},
		{
			name:        "Unavailable gRPC error",
			err:         grpcstatus.Error(codes.Unavailable, "service unavailable"),
			shouldRetry: true,
		},
		{
			name:        "network error",
			err:         errors.New("network error"),
			shouldRetry: true,
		},
		{
			name:        "wrapped network error",
			err:         errors.Wrap(errors.New("connection reset"), "failed to receive"),
			shouldRetry: true,
		},
		{
			name:        "Internal gRPC error",
			err:         grpcstatus.Error(codes.Internal, "internal"),
			shouldRetry: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := handler.shouldRetry(tc.err)
			assert.Equal(t, tc.shouldRetry, result)
		})
	}
}

func TestAssignHandler_CreateStreamError(t *testing.T) {
	client := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	pchannel := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "test-channel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 123},
	}

	// Return error when creating stream
	networkErr := errors.New("connection refused")
	client.EXPECT().AssignWithStateReport(mock.Anything, mock.Anything).Return(nil, networkErr)

	handler := newAssignHandler(client, pchannel)

	// Use short timeout context to avoid long retry
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel after first attempt
		cancel()
	}()

	err := handler.Execute(ctx)
	assert.Error(t, err)
}

func TestHasProgressChanged(t *testing.T) {
	tests := []struct {
		name     string
		old      progressState
		new      progressState
		expected bool
	}{
		{
			name:     "same state and progress",
			old:      progressState{state: streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING},
			new:      progressState{state: streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING},
			expected: false,
		},
		{
			name:     "state changed",
			old:      progressState{state: streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING},
			new:      progressState{state: streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING},
			expected: true,
		},
		{
			name: "recovered bytes increased",
			old: progressState{
				state:          streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING,
				recoveredBytes: 100,
			},
			new: progressState{
				state:          streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING,
				recoveredBytes: 200,
			},
			expected: true,
		},
		{
			name: "recovered messages increased",
			old: progressState{
				state:             streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING,
				recoveredMessages: 10,
			},
			new: progressState{
				state:             streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING,
				recoveredMessages: 20,
			},
			expected: true,
		},
		{
			name: "bytes decreased - not progress",
			old: progressState{
				state:          streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING,
				recoveredBytes: 200,
			},
			new: progressState{
				state:          streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING,
				recoveredBytes: 100,
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := hasProgressChanged(tc.old, tc.new)
			assert.Equal(t, tc.expected, result)
		})
	}
}
