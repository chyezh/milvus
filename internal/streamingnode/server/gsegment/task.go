// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package gsegment

import "context"

// SyncChunkTask persists one sealed chunk's worth of data to object storage.
//
// Contract with SyncScheduler:
//   - Poll runs one stage (e.g., serialize, then upload) and returns either
//     nil (terminal success), ErrContinue (yield for the next stage), a
//     retryable error (transient failure; back off and retry), or any other
//     error (terminal failure).
//   - CPUBound reports whether the next Poll stage is CPU-bound. When Poll
//     returns ErrContinue, the scheduler reads CPUBound to pick the target pool.
//   - OnComplete is invoked exactly once in a terminal state (nil on success,
//     non-nil on failure). The task owner wires downstream state updates here.
//   - Key is used purely for log/metric attribution.
type SyncChunkTask interface {
	CPUBound() bool
	Poll(ctx context.Context) error
	OnComplete(err error)
	Key() string
}

// taskState enumerates the internal stages of the built-in insert/delete tasks.
type taskState int

const (
	taskStateInit taskState = iota
	taskStateSerializing
	taskStateUploading
	taskStateDone
)
