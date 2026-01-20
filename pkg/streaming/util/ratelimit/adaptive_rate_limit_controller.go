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

package ratelimit

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	adaptiveRateLimitModeNormal adaptiveRateLimitMode = iota
	adaptiveRateLimitModeSlowdown
	adaptiveRateLimitModeReject
	adaptiveRateLimitModeRecovery
)

type adaptiveRateLimitMode int

func (m adaptiveRateLimitMode) String() string {
	switch m {
	case adaptiveRateLimitModeNormal:
		return "normal"
	case adaptiveRateLimitModeSlowdown:
		return "slowdown"
	case adaptiveRateLimitModeReject:
		return "reject"
	case adaptiveRateLimitModeRecovery:
		return "recovery"
	default:
		return ""
	}
}

// AdaptiveRateLimitController manages rate limiting state transitions for the scanner.
// It observes the scanner mode and adjusts rate limits accordingly:
// - When entering slowdown mode: starts at high watermark rate
// - In slowdown mode: decreases rate by ratio periodically until low watermark
// - When exiting slowdown (entering recovery): starts recovery, increasing rate incrementally periodically until high watermark.
// - Recovery completes: when rate reaches high watermark and stays for one more interval
type AdaptiveRateLimitController struct {
	sourceName        string
	wg                *sync.WaitGroup
	rateLimitRegistry *MuxRateLimitObserverRegistryImpl

	mode           *atomic.Int32
	stopBackground chan struct{}
	currentRate    int64
	configFetcher  AdaptiveRateLimitControllerConfigFetcher
	channel        types.PChannelInfo
}

// NewAdaptiveRateLimitController creates a new rate limit controller.
// The API of adaptive rate limit controller is not concurrent-safe.
func NewAdaptiveRateLimitController(
	channel types.PChannelInfo,
	sourceName string,
	rateLimitRegistry *MuxRateLimitObserverRegistryImpl,
	configFetcher AdaptiveRateLimitControllerConfigFetcher,
) *AdaptiveRateLimitController {
	c := &AdaptiveRateLimitController{
		sourceName:        sourceName,
		wg:                &sync.WaitGroup{},
		rateLimitRegistry: rateLimitRegistry,
		mode:              atomic.NewInt32(int32(adaptiveRateLimitModeNormal)),
		currentRate:       0,
		stopBackground:    nil,
		configFetcher:     configFetcher,
		channel:           channel,
	}
	c.notify()
	return c
}

func (c *AdaptiveRateLimitController) getMode() adaptiveRateLimitMode {
	return adaptiveRateLimitMode(c.mode.Load())
}

func (c *AdaptiveRateLimitController) setMode(mode adaptiveRateLimitMode) {
	c.mode.Store(int32(mode))
}

// EnterRejectMode is called when the scanner enters reject mode.
func (c *AdaptiveRateLimitController) EnterRejectMode() {
	if c.getMode() == adaptiveRateLimitModeReject {
		return
	}
	c.stopExecutedBackgroundTask()
	c.enterRejectMode()
}

// EnterSlowdownMode is called when the scanner enters slowdown mode.
// Sets rate to high watermark and starts the decreasing timer.
func (c *AdaptiveRateLimitController) EnterSlowdownMode() {
	if c.getMode() == adaptiveRateLimitModeSlowdown || c.getMode() == adaptiveRateLimitModeReject {
		return
	}
	c.stopExecutedBackgroundTask()

	c.setMode(adaptiveRateLimitModeSlowdown)
	cfg := c.configFetcher.FetchSlowdownConfig()
	c.currentRate = math.MaxInt64
	firstSlowdownDelay := cfg.FirstSlowdownDelay
	cfg.FirstSlowdownDelay = 0

	c.wg.Add(1)
	c.stopBackground = make(chan struct{})
	go func() {
		defer c.wg.Done()
		if firstSlowdownDelay > 0 {
			select {
			case <-time.After(firstSlowdownDelay):
			case <-c.stopBackground:
				return
			}
		}

		if c.currentRate == 0 || c.currentRate > cfg.HWM {
			// Start with slow down mode at high watermark rate.
			c.currentRate = cfg.HWM
		}
		c.notify()

		decreaseTicker := time.NewTicker(cfg.DecreaseInterval)
		defer decreaseTicker.Stop()

		decreaseTickerCh := decreaseTicker.C
		var rejectDelayNotifier <-chan time.Time
		for {
			select {
			case <-decreaseTickerCh:
				if c.tickSlowdown(cfg) {
					decreaseTickerCh = nil
					if cfg.RejectDelayInterval == 0 {
						return
					}
					rejectDelayNotifier = time.After(cfg.RejectDelayInterval)
				}
			case <-rejectDelayNotifier:
				c.enterRejectMode()
				return
			case <-c.stopBackground:
				return
			}
		}
	}()
}

// tickSlowdown should be called periodically while in slowdown mode.
// Decreases rate by half every slowdownRateDecreaseInterval until low watermark.
func (c *AdaptiveRateLimitController) tickSlowdown(cfg SlowdownConfig) (reachedLWM bool) {
	newRate := int64(float64(c.currentRate) * cfg.DecreaseRatio)
	if newRate < cfg.LWM {
		newRate = cfg.LWM
	}
	c.currentRate = newRate
	c.notify()
	return c.currentRate == cfg.LWM
}

// EnterRecoveryMode is called when the scanner exits slowdown mode and enters recovery mode.
// Starts the recovery process to gradually increase rate.
func (c *AdaptiveRateLimitController) EnterRecoveryMode() {
	if c.getMode() == adaptiveRateLimitModeRecovery || c.getMode() == adaptiveRateLimitModeNormal {
		return
	}
	c.stopExecutedBackgroundTask()

	if c.currentRate == math.MaxInt64 {
		// if the current rate is max int64, it means the slowdown mode is not started,
		// so we need enter normal mode directly.
		c.enterNormalMode()
		return
	}

	c.setMode(adaptiveRateLimitModeRecovery)
	cfg := c.configFetcher.FetchRecoveryConfig()
	if c.currentRate < cfg.LWM {
		c.currentRate = cfg.LWM
	}
	c.notify()

	c.wg.Add(1)
	c.stopBackground = make(chan struct{})
	go func() {
		recoveryTicker := time.NewTicker(cfg.IncreaseDelayInterval)
		defer func() {
			recoveryTicker.Stop()
			c.wg.Done()
		}()
		recoveryTickerCh := recoveryTicker.C
		var normalDelayNotifier <-chan time.Time
		for {
			select {
			case <-recoveryTickerCh:
				if c.tickRecovery(cfg) {
					// start a normal delay timer to enter normal mode after recovery reaches high watermark.
					recoveryTickerCh = nil
					normalDelayNotifier = time.After(cfg.NormalInterval)
				}
			case <-normalDelayNotifier:
				c.enterNormalMode()
				return
			case <-c.stopBackground:
				return
			}
		}
	}()
}

// TickRecovery should be called periodically while in recovery mode.
// Increases rate by 1MB/s every recoveryRateIncreaseInterval until reaching high watermark,
// then removes rate limiting after one more interval.
func (c *AdaptiveRateLimitController) tickRecovery(cfg RecoveryConfig) (reachedHWM bool) {
	newRate := c.currentRate + cfg.Incremental
	if newRate > cfg.HWM {
		newRate = cfg.HWM
	}
	c.currentRate = newRate
	c.notify()
	return c.currentRate == cfg.HWM
}

// stopExecutedBackgroundTask stops the background tasks and waits for them to finish.
func (c *AdaptiveRateLimitController) stopExecutedBackgroundTask() {
	if c.stopBackground != nil {
		close(c.stopBackground)
		c.wg.Wait()
		c.stopBackground = nil
	}
}

// notify notifies the observer with SLOWDOWN state.
func (c *AdaptiveRateLimitController) notify() {
	switch c.getMode() {
	case adaptiveRateLimitModeSlowdown, adaptiveRateLimitModeRecovery:
		c.rateLimitRegistry.NotifySourceRateLimitState(c.sourceName, RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  c.currentRate,
		})
	case adaptiveRateLimitModeReject:
		c.rateLimitRegistry.NotifySourceRateLimitState(c.sourceName, RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT,
			Rate:  0,
		})
	case adaptiveRateLimitModeNormal:
		c.rateLimitRegistry.NotifySourceRateLimitState(c.sourceName, RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL,
			Rate:  0,
		})
	}
	c.clearMetrics()
	metrics.WALRateLimitControllerState.WithLabelValues(
		paramtable.GetStringNodeID(),
		c.channel.Name,
		c.sourceName,
		c.mode.String(),
	).Set(float64(c.currentRate))
}

// enterNormalMode is called when the scanner enters normal mode.
func (c *AdaptiveRateLimitController) enterNormalMode() {
	c.setMode(adaptiveRateLimitModeNormal)
	c.currentRate = 0
	c.notify()
}

// enterRejectMode is called when the scanner enters reject mode.
func (c *AdaptiveRateLimitController) enterRejectMode() {
	c.setMode(adaptiveRateLimitModeReject)
	c.currentRate = 0
	c.notify()
}

func (c *AdaptiveRateLimitController) Close() {
	c.stopExecutedBackgroundTask()
}

func (c *AdaptiveRateLimitController) clearMetrics() {
	metrics.WALRateLimitControllerState.DeletePartialMatch(prometheus.Labels{
		metrics.WALRateLimitControllerSourceLabelName: c.sourceName,
		metrics.WALChannelLabelName:                   c.channel.Name,
	})
}
