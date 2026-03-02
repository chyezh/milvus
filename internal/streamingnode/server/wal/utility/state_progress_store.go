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

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// StateProgress represents the current assignment state and recovery progress.
type StateProgress struct {
	Version           uint64                      // Monotonically increasing version for change detection
	State             streamingpb.AssignmentState // Current assignment state
	RecoveredBytes    int64                       // Bytes recovered so far
	RecoveredMessages int64                       // Messages recovered so far
	TotalBytes        int64                       // Total bytes to recover (-1 if unknown)
	TotalMessages     int64                       // Total messages to recover (-1 if unknown)
	Error             error                       // Error if assignment failed
	Ready             bool                        // True if assignment completed successfully
}

// GetProtoProgress converts the progress to proto format.
// Returns nil if not in STREAM_RECOVERING state or no progress data.
func (p *StateProgress) GetProtoProgress() *streamingpb.StreamRecoveringProgress {
	if p.State != streamingpb.AssignmentState_ASSIGNMENT_STATE_STREAM_RECOVERING {
		return nil
	}
	return &streamingpb.StreamRecoveringProgress{
		RecoveredBytes:    p.RecoveredBytes,
		TotalBytes:        p.TotalBytes,
		RecoveredMessages: p.RecoveredMessages,
		TotalMessages:     p.TotalMessages,
	}
}

// StateProgressStore stores assignment state in memory and supports watching for changes.
// It is thread-safe and supports multiple concurrent readers and writers.
// The store uses a version number for efficient change detection in watchers.
type StateProgressStore struct {
	mu      sync.Mutex
	cond    *syncutil.ContextCond
	current StateProgress
}

// NewStateProgressStore creates a new state progress store with initial UNKNOWN state.
func NewStateProgressStore() *StateProgressStore {
	s := &StateProgressStore{
		current: StateProgress{
			Version:       0,
			State:         streamingpb.AssignmentState_ASSIGNMENT_STATE_UNKNOWN,
			TotalBytes:    -1, // Unknown
			TotalMessages: -1, // Unknown
		},
	}
	s.cond = syncutil.NewContextCond(&s.mu)
	return s
}

// UpdateState updates the assignment state.
// This method never fails and is safe to call from recovery process.
func (s *StateProgressStore) UpdateState(state streamingpb.AssignmentState) {
	s.cond.LockAndBroadcast()
	defer s.mu.Unlock()

	s.current.Version++
	s.current.State = state
}

// SetReady marks the assignment as successfully completed.
func (s *StateProgressStore) SetReady() {
	s.cond.LockAndBroadcast()
	defer s.mu.Unlock()

	s.current.Version++
	s.current.Ready = true
}

// SetError marks the assignment as failed with the given error.
func (s *StateProgressStore) SetError(err error) {
	s.cond.LockAndBroadcast()
	defer s.mu.Unlock()

	s.current.Version++
	s.current.Error = err
}

// ObserveRecoveredMessage records a recovered message.
// This should be called for each message during stream recovery.
// The estimatedSize is the estimated size of the message in bytes.
func (s *StateProgressStore) ObserveRecoveredMessage(estimatedSize int) {
	s.cond.LockAndBroadcast()
	defer s.mu.Unlock()

	s.current.Version++
	s.current.RecoveredBytes += int64(estimatedSize)
	s.current.RecoveredMessages++
}

// SetTotalEstimate sets the estimated total bytes and messages to recover.
// Use -1 for unknown values.
func (s *StateProgressStore) SetTotalEstimate(bytes, messages int64) {
	s.cond.LockAndBroadcast()
	defer s.mu.Unlock()

	s.current.Version++
	s.current.TotalBytes = bytes
	s.current.TotalMessages = messages
}

// Get returns the current state progress.
// This is a snapshot and may become stale immediately after returning.
func (s *StateProgressStore) Get() StateProgress {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

// GetProtoProgress returns the current progress in proto format.
// Returns nil if not in STREAM_RECOVERING state.
func (s *StateProgressStore) GetProtoProgress() *streamingpb.StreamRecoveringProgress {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current.GetProtoProgress()
}

// Watch blocks until the state changes from the given version.
// Returns the new state progress and its version, or error if context is canceled.
//
// Usage:
//
//	progress := store.Get()
//	for !progress.Ready && progress.Error == nil {
//	    var err error
//	    progress, err = store.Watch(ctx, progress.Version)
//	    if err != nil {
//	        return err
//	    }
//	    // Handle progress update
//	}
func (s *StateProgressStore) Watch(ctx context.Context, version uint64) (StateProgress, error) {
	s.mu.Lock()
	for s.current.Version == version {
		if err := s.cond.Wait(ctx); err != nil {
			// Note: ContextCond.Wait() releases the lock before returning error
			// so we should NOT unlock here
			return StateProgress{}, err
		}
	}
	progress := s.current
	s.mu.Unlock()
	return progress, nil
}

// IsTerminal returns true if the state is terminal (Ready or Error).
func (p *StateProgress) IsTerminal() bool {
	return p.Ready || p.Error != nil
}

// BlockUntilReady blocks until the state reaches a terminal state (Ready or Error).
// Returns nil if Ready, or the error if Error, or ctx.Err() if context is canceled.
func (s *StateProgressStore) BlockUntilReady(ctx context.Context) error {
	progress := s.Get()
	for !progress.IsTerminal() {
		var err error
		progress, err = s.Watch(ctx, progress.Version)
		if err != nil {
			return err
		}
	}
	return progress.Error
}
