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
		c.NodeMemory.EnterSlowdownMode()
	}
	if usedRatio < paramtable.Get().StreamingCfg.WALRateLimitNodeMemoryRecoverThreshold.GetAsFloat() {
		c.NodeMemory.EnterRecoveryMode()
	}
	if usedRatio > paramtable.Get().StreamingCfg.WALRateLimitNodeMemoryRejectThreshold.GetAsFloat() {
		c.NodeMemory.EnterRejectMode()
	}
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
