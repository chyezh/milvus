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

package timestamp

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/types"
)

const batchAllocateSize = 1000

var _ Allocator = (*allocatorImpl)(nil)

func NewAllocator(rc types.RootCoordClient) Allocator {
	return &allocatorImpl{
		mu:              sync.Mutex{},
		remoteAllocator: newRemoteAllocator(rc),
		localAllocator:  newLocalAllocator(),
	}
}

type Allocator interface {
	// AllocateOne allocates a timestamp.
	AllocateOne(ctx context.Context) (uint64, error)

	// Sync syncs the local allocator and remote allocator.
	Sync()
}

type allocatorImpl struct {
	mu              sync.Mutex
	remoteAllocator *remoteAllocator
	localAllocator  *localAllocator
}

// AllocateOne allocates a timestamp.
func (ta *allocatorImpl) AllocateOne(ctx context.Context) (uint64, error) {
	// allocate one from local allocator first.
	if id, err := ta.localAllocator.allocateOne(); err == nil {
		return id, nil
	}
	// allocate from remote.
	return ta.allocateRemote(ctx)
}

// Sync syncs the local allocator and remote allocator.
func (ta *allocatorImpl) Sync() {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	ta.localAllocator.expire()
}

// allocateRemote allocates timestamp from remote root coordinator.
func (ta *allocatorImpl) allocateRemote(ctx context.Context) (uint64, error) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// Do allocate at local again, it may already updated by other goroutine.
	if id, err := ta.localAllocator.allocateOne(); err == nil {
		return id, nil
	}

	// Update local allocator from remote.
	start, count, err := ta.remoteAllocator.allocate(ctx, batchAllocateSize)
	if err != nil {
		return 0, err
	}
	ta.localAllocator.update(start, count)

	// Get from local again.
	return ta.localAllocator.allocateOne()
}
