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

package rate

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	SourceRecoveryStorage   = "recovery-storage"
	SourceNodeMemory        = "node-memory"
	SourceFlusherRecovering = "flusher-recovering"
)

type WALRateLimitComponent struct {
	*ratelimit.MuxRateLimitObserverRegistryImpl
	RecoveryStorage   *ratelimit.AdaptiveRateLimitController
	FlusherRecovering *ratelimit.AdaptiveRateLimitController
	NodeMemory        *ratelimit.AdaptiveRateLimitController
	handler           *hardware.SystemMetricsListener
}

// NewWALRateLimitComponent creates a new WAL rate limit component.
func NewWALRateLimitComponent(
	channel types.PChannelInfo,
) *WALRateLimitComponent {
	rateLimitRegistry := ratelimit.NewMuxRateLimitObserverRegistry()
	return &WALRateLimitComponent{
		MuxRateLimitObserverRegistryImpl: rateLimitRegistry,
		RecoveryStorage: ratelimit.NewAdaptiveRateLimitController(channel, SourceRecoveryStorage, rateLimitRegistry,
			newAdaptiveRateLimitControllerConfigFetcher(SourceRecoveryStorage)),
		FlusherRecovering: ratelimit.NewAdaptiveRateLimitController(channel, SourceFlusherRecovering, rateLimitRegistry,
			newAdaptiveRateLimitControllerConfigFetcher(SourceFlusherRecovering)),
		NodeMemory: ratelimit.NewAdaptiveRateLimitController(channel, SourceNodeMemory, rateLimitRegistry,
			newAdaptiveRateLimitControllerConfigFetcher(SourceNodeMemory)),
	}
}

// RegisterMemoryObserver registers the memory observer.
func (c *WALRateLimitComponent) RegisterMemoryObserver() {
	l := &hardware.SystemMetricsListener{
		Cooldown: 0 * time.Second,
		Condition: func(sm hardware.SystemMetrics, _ *hardware.SystemMetricsListener) bool {
			return true
		},
		Callback: c.hardwardCallback,
	}
	hardware.RegisterSystemMetricsListener(l)
	c.handler = l
}

// hardwardCallback is the callback function for the hardware metrics listener.
func (c *WALRateLimitComponent) hardwardCallback(sm hardware.SystemMetrics, _ *hardware.SystemMetricsListener) {
	usedRatio := sm.UsedRatio()
	if usedRatio > paramtable.Get().StreamingCfg.WALRateLimitNodeMemorySlowdownThreshold.GetAsFloat() {
		// Create checker that stops slowdown when memory usage decreases.
		checker := newMemorySlowdownChecker(usedRatio)
		c.NodeMemory.EnterSlowdownMode(checker)
	}
	if usedRatio < paramtable.Get().StreamingCfg.WALRateLimitNodeMemoryRecoverThreshold.GetAsFloat() {
		c.NodeMemory.EnterRecoveryMode()
	}
	if usedRatio > paramtable.Get().StreamingCfg.WALRateLimitNodeMemoryRejectThreshold.GetAsFloat() {
		c.NodeMemory.EnterRejectMode()
	}
}

// memorySlowdownChecker implements ratelimit.SlowdownChecker for memory-based slowdown.
// It returns false when current memory usage is lower than the previous usage,
// indicating that memory pressure is reducing and slowdown should stop.
type memorySlowdownChecker struct {
	mu                sync.Mutex
	previousUsedRatio float64
}

// newMemorySlowdownChecker creates a new memory slowdown checker with the initial used ratio.
func newMemorySlowdownChecker(initialUsedRatio float64) *memorySlowdownChecker {
	return &memorySlowdownChecker{
		previousUsedRatio: initialUsedRatio,
	}
}

// Check returns true if slowdown should continue, false if it should stop.
// Returns false when current memory usage is lower than previous (memory pressure reducing).
func (c *memorySlowdownChecker) Check() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	totalMemory := hardware.GetMemoryCount()
	if totalMemory == 0 {
		return true // Continue slowdown if we can't get memory info
	}
	currentUsedRatio := float64(hardware.GetUsedMemoryCount()) / float64(totalMemory)

	// If current memory usage is lower than previous, stop slowdown
	if currentUsedRatio < c.previousUsedRatio {
		return false
	}
	// Update previous usage for next check
	c.previousUsedRatio = currentUsedRatio
	return true
}

// SlowdownStartupHWM returns 0 to use the default HWM from config.
func (c *memorySlowdownChecker) SlowdownStartupHWM() int64 {
	return 0
}

// Close closes the WAL rate limit component.
// The API of WAL rate limit component is not concurrent-safe.
func (c *WALRateLimitComponent) Close() {
	if c.handler != nil {
		hardware.UnregisterSystemMetricsListener(c.handler)
	}
	c.RecoveryStorage.Close()
	c.FlusherRecovering.Close()
	c.NodeMemory.Close()
}
