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

package utility

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestNewStateProgressStore(t *testing.T) {
	store := NewStateProgressStore()
	assert.NotNil(t, store)

	progress := store.Get()
	assert.Equal(t, uint64(0), progress.Version)
	assert.Equal(t, streamingpb.AssignmentState_ASSIGNMENT_STATE_UNKNOWN, progress.State)
	assert.Equal(t, int64(-1), progress.TotalBytes)
	assert.Equal(t, int64(-1), progress.TotalMessages)
	assert.Equal(t, int64(0), progress.RecoveredBytes)
	assert.Equal(t, int64(0), progress.RecoveredMessages)
	assert.False(t, progress.Ready)
	assert.Nil(t, progress.Error)
}

func TestStateProgressStore_UpdateState(t *testing.T) {
	store := NewStateProgressStore()

	store.UpdateState(streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING)
	progress := store.Get()
	assert.Equal(t, uint64(1), progress.Version)
	assert.Equal(t, streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING, progress.State)

	store.UpdateState(streamingpb.AssignmentState_ASSIGNMENT_STATE_PERSIST_RECOVERING)
	progress = store.Get()
	assert.Equal(t, uint64(2), progress.Version)
	assert.Equal(t, streamingpb.AssignmentState_ASSIGNMENT_STATE_PERSIST_RECOVERING, progress.State)

	store.UpdateState(streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING)
	progress = store.Get()
	assert.Equal(t, uint64(3), progress.Version)
	assert.Equal(t, streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING, progress.State)
}

func TestStateProgressStore_SetReady(t *testing.T) {
	store := NewStateProgressStore()
	store.UpdateState(streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING)

	store.SetReady()
	progress := store.Get()
	assert.True(t, progress.Ready)
	assert.True(t, progress.IsTerminal())
}

func TestStateProgressStore_SetError(t *testing.T) {
	store := NewStateProgressStore()
	store.UpdateState(streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING)

	testErr := errors.New("test error")
	store.SetError(testErr)
	progress := store.Get()
	assert.Equal(t, testErr, progress.Error)
	assert.True(t, progress.IsTerminal())
}

func TestStateProgressStore_ObserveRecoveredMessage(t *testing.T) {
	store := NewStateProgressStore()
	store.UpdateState(streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING)

	// Observe first message
	store.ObserveRecoveredMessage(100)
	progress := store.Get()
	assert.Equal(t, int64(100), progress.RecoveredBytes)
	assert.Equal(t, int64(1), progress.RecoveredMessages)

	// Observe second message
	store.ObserveRecoveredMessage(200)
	progress = store.Get()
	assert.Equal(t, int64(300), progress.RecoveredBytes)
	assert.Equal(t, int64(2), progress.RecoveredMessages)

	// Observe third message
	store.ObserveRecoveredMessage(150)
	progress = store.Get()
	assert.Equal(t, int64(450), progress.RecoveredBytes)
	assert.Equal(t, int64(3), progress.RecoveredMessages)
}

func TestStateProgressStore_SetTotalEstimate(t *testing.T) {
	store := NewStateProgressStore()

	store.SetTotalEstimate(1000, 100)
	progress := store.Get()
	assert.Equal(t, int64(1000), progress.TotalBytes)
	assert.Equal(t, int64(100), progress.TotalMessages)

	// Update with different values
	store.SetTotalEstimate(2000, 200)
	progress = store.Get()
	assert.Equal(t, int64(2000), progress.TotalBytes)
	assert.Equal(t, int64(200), progress.TotalMessages)
}

func TestStateProgressStore_GetProtoProgress(t *testing.T) {
	store := NewStateProgressStore()

	// Not in STREAM_RECOVERING state, should return nil
	protoProgress := store.GetProtoProgress()
	assert.Nil(t, protoProgress)

	// Set to STREAM_RECOVERING state
	store.UpdateState(streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING)
	store.SetTotalEstimate(1000, 100)
	store.ObserveRecoveredMessage(100)
	store.ObserveRecoveredMessage(200)

	protoProgress = store.GetProtoProgress()
	assert.NotNil(t, protoProgress)
	assert.Equal(t, int64(300), protoProgress.RecoveredBytes)
	assert.Equal(t, int64(2), protoProgress.RecoveredMessages)
	assert.Equal(t, int64(1000), protoProgress.TotalBytes)
	assert.Equal(t, int64(100), protoProgress.TotalMessages)
}

func TestStateProgressStore_Watch(t *testing.T) {
	store := NewStateProgressStore()

	// Get initial version
	initial := store.Get()
	assert.Equal(t, uint64(0), initial.Version)

	// Start watcher in goroutine
	watchDone := make(chan StateProgress, 1)
	go func() {
		progress, err := store.Watch(context.Background(), initial.Version)
		assert.NoError(t, err)
		watchDone <- progress
	}()

	// Give watcher time to start waiting
	time.Sleep(10 * time.Millisecond)

	// Update state
	store.UpdateState(streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING)

	// Watcher should receive update
	select {
	case progress := <-watchDone:
		assert.Equal(t, uint64(1), progress.Version)
		assert.Equal(t, streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING, progress.State)
	case <-time.After(time.Second):
		t.Fatal("Watcher did not receive update")
	}
}

func TestStateProgressStore_Watch_ContextCanceled(t *testing.T) {
	store := NewStateProgressStore()

	ctx, cancel := context.WithCancel(context.Background())

	// Start watcher
	watchDone := make(chan error, 1)
	go func() {
		_, err := store.Watch(ctx, 0)
		watchDone <- err
	}()

	// Give watcher time to start waiting
	time.Sleep(10 * time.Millisecond)

	// Cancel context
	cancel()

	// Watcher should return error
	select {
	case err := <-watchDone:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Watcher did not return after context cancel")
	}
}

func TestStateProgressStore_Watch_AlreadyChanged(t *testing.T) {
	store := NewStateProgressStore()

	// Update state first
	store.UpdateState(streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING)

	// Watch with old version should return immediately
	progress, err := store.Watch(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), progress.Version)
	assert.Equal(t, streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING, progress.State)
}

func TestStateProgressStore_ConcurrentAccess(t *testing.T) {
	store := NewStateProgressStore()
	var wg sync.WaitGroup

	// Start multiple writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				store.ObserveRecoveredMessage(10)
			}
		}()
	}

	// Start multiple readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = store.Get()
			}
		}()
	}

	wg.Wait()

	// Verify final state
	progress := store.Get()
	assert.Equal(t, int64(10000), progress.RecoveredBytes)   // 10 goroutines * 100 messages * 10 bytes
	assert.Equal(t, int64(1000), progress.RecoveredMessages) // 10 goroutines * 100 messages
}

func TestStateProgress_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		progress StateProgress
		expected bool
	}{
		{
			name:     "not terminal - initial state",
			progress: StateProgress{},
			expected: false,
		},
		{
			name:     "terminal - ready",
			progress: StateProgress{Ready: true},
			expected: true,
		},
		{
			name:     "terminal - error",
			progress: StateProgress{Error: errors.New("error")},
			expected: true,
		},
		{
			name:     "terminal - both ready and error",
			progress: StateProgress{Ready: true, Error: errors.New("error")},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.progress.IsTerminal())
		})
	}
}

func TestStateProgress_GetProtoProgress(t *testing.T) {
	tests := []struct {
		name     string
		progress StateProgress
		expected *streamingpb.StreamRecoveringProgress
	}{
		{
			name: "not STREAM_RECOVERING state",
			progress: StateProgress{
				State:             streamingpb.AssignmentState_ASSIGNMENT_STATE_FENCING,
				RecoveredBytes:    100,
				RecoveredMessages: 10,
			},
			expected: nil,
		},
		{
			name: "STREAM_RECOVERING state with progress",
			progress: StateProgress{
				State:             streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING,
				RecoveredBytes:    500,
				RecoveredMessages: 50,
				TotalBytes:        1000,
				TotalMessages:     100,
			},
			expected: &streamingpb.StreamRecoveringProgress{
				RecoveredBytes:    500,
				RecoveredMessages: 50,
				TotalBytes:        1000,
				TotalMessages:     100,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.progress.GetProtoProgress()
			if tc.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tc.expected.RecoveredBytes, result.RecoveredBytes)
				assert.Equal(t, tc.expected.RecoveredMessages, result.RecoveredMessages)
				assert.Equal(t, tc.expected.TotalBytes, result.TotalBytes)
				assert.Equal(t, tc.expected.TotalMessages, result.TotalMessages)
			}
		})
	}
}
