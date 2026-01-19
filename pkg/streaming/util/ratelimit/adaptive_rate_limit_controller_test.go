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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type MockConfigFetcher struct {
	mock.Mock
}

func (m *MockConfigFetcher) FetchRecoveryConfig() RecoveryConfig {
	args := m.Called()
	return args.Get(0).(RecoveryConfig)
}

func (m *MockConfigFetcher) FetchSlowdownConfig() SlowdownConfig {
	args := m.Called()
	return args.Get(0).(SlowdownConfig)
}

func setupTest(_ *testing.T) (types.PChannelInfo, string, *MuxRateLimitObserverRegistryImpl, *MockRateLimitObserver, *MockConfigFetcher) {
	channel := types.PChannelInfo{Name: "test-channel"}
	sourceName := "test-source"
	mux := NewMuxRateLimitObserverRegistry()
	observer := new(MockRateLimitObserver)
	observer.On("UpdateRateLimitState", NewNormalRateLimitState()).Once()
	mux.Register(observer)
	fetcher := new(MockConfigFetcher)
	return channel, sourceName, mux, observer, fetcher
}

func TestAdaptiveRateLimitController_ModeString(t *testing.T) {
	assert.Equal(t, "normal", adaptiveRateLimitModeNormal.String())
	assert.Equal(t, "slowdown", adaptiveRateLimitModeSlowdown.String())
	assert.Equal(t, "reject", adaptiveRateLimitModeReject.String())
	assert.Equal(t, "recovery", adaptiveRateLimitModeRecovery.String())
	assert.Equal(t, "", adaptiveRateLimitMode(99).String())
}

func TestAdaptiveRateLimitController_ModeTransition(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	observer.On("UpdateRateLimitState", mock.Anything).Maybe()
	slowdownCfg := SlowdownConfig{
		HWM:                 100,
		LWM:                 50,
		DecreaseInterval:    10 * time.Millisecond,
		DecreaseRatio:       0.8,
		RejectDelayInterval: 50 * time.Millisecond,
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)
	recoveryCfg := RecoveryConfig{
		HWM:                   100,
		LWM:                   60,
		NormalInterval:        20 * time.Millisecond,
		Incremental:           15,
		IncreaseDelayInterval: 10 * time.Millisecond,
	}
	fetcher.On("FetchRecoveryConfig").Return(recoveryCfg)

	for i := 0; i < 100; i++ {
		time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
		switch rand.Intn(3) {
		case 0:
			controller.EnterRejectMode()
		case 1:
			controller.EnterSlowdownMode(0)
		case 2:
			controller.EnterRecoveryMode()
		}
	}
	controller.EnterRecoveryMode()
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeNormal
	}, 2*time.Second, 10*time.Millisecond)
	observer.AssertExpectations(t)
}

func TestAdaptiveRateLimitController_EnterRejectMode(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	rejectState := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT, Rate: 0}
	observer.On("UpdateRateLimitState", rejectState).Once()

	controller.EnterRejectMode()
	assert.Equal(t, adaptiveRateLimitModeReject, controller.getMode())
	observer.AssertExpectations(t)

	// Enter again should do nothing
	controller.EnterRejectMode()
}

func TestAdaptiveRateLimitController_EnterSlowdownMode(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	slowdownCfg := SlowdownConfig{
		HWM:                 100,
		LWM:                 50,
		DecreaseInterval:    10 * time.Millisecond,
		DecreaseRatio:       0.8,
		RejectDelayInterval: 50 * time.Millisecond,
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)

	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 100}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 80}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 64}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 51}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 50}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT, Rate: 0}).Once()

	controller.EnterSlowdownMode(0)
	controller.wg.Wait()
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeReject
	}, 2*time.Second, 10*time.Millisecond)

	observer.AssertExpectations(t)
}

func TestAdaptiveRateLimitController_EnterRecoveryMode(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	// Move to slowdown state manually
	controller.setMode(adaptiveRateLimitModeSlowdown)
	controller.currentRate = 50

	recoveryCfg := RecoveryConfig{
		HWM:                   100,
		LWM:                   60,
		NormalInterval:        20 * time.Millisecond,
		Incremental:           15,
		IncreaseDelayInterval: 10 * time.Millisecond,
	}
	fetcher.On("FetchRecoveryConfig").Return(recoveryCfg)

	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 60}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 75}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 90}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 100}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL, Rate: 0}).Once()

	controller.EnterRecoveryMode()
	controller.EnterRecoveryMode()

	controller.wg.Wait()
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeNormal
	}, 2*time.Second, 10*time.Millisecond)

	observer.AssertExpectations(t)
}

func TestAdaptiveRateLimitController_EnterRecoveryFromMaxInt64(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	controller.setMode(adaptiveRateLimitModeSlowdown)
	controller.currentRate = 9223372036854775807 // math.MaxInt64

	// Notify the mux so it's in a non-normal state
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: controller.currentRate}).Once()
	controller.notify()
	observer.AssertExpectations(t)

	observer.On("UpdateRateLimitState", NewNormalRateLimitState()).Once()
	controller.EnterRecoveryMode()
	assert.Equal(t, adaptiveRateLimitModeNormal, controller.getMode())
	observer.AssertExpectations(t)
}

func TestAdaptiveRateLimitController_SlowdownWithStartupDelay(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	slowdownCfg := SlowdownConfig{
		HWM:                 200,
		LWM:                 100,
		DecreaseInterval:    100 * time.Millisecond,
		DecreaseRatio:       0.5,
		RejectDelayInterval: 0,
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)

	delay := 100 * time.Millisecond
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 200}).Once()
	observer.On("UpdateRateLimitState", mock.Anything).Maybe()

	start := time.Now()
	controller.EnterSlowdownMode(delay)
	controller.EnterSlowdownMode(delay)

	controller.wg.Wait()
	assert.Eventually(t, func() bool {
		return controller.currentRate == 100
	}, 2*time.Second, 10*time.Millisecond)

	assert.True(t, time.Since(start) >= delay)
	observer.AssertExpectations(t)
}
