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

package delegator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestTSafeWaiterManagerNotifySatisfiedOnly(t *testing.T) {
	latest := atomic.NewUint64(10)
	manager := newTSafeWaiterManager(latest)

	_, waiter15, closed := manager.addWaiter(15)
	require.False(t, closed)
	require.NotNil(t, waiter15)

	_, waiter20, closed := manager.addWaiter(20)
	require.False(t, closed)
	require.NotNil(t, waiter20)

	woken, avoided := manager.update(15)

	assertClosed(t, waiter15.done)
	assertNotClosed(t, waiter20.done)
	assert.EqualValues(t, 15, latest.Load())
	assert.Equal(t, 1, woken)
	assert.Equal(t, 1, avoided)
}

func TestTSafeWaiterManagerAddWaiterAlreadySatisfied(t *testing.T) {
	latest := atomic.NewUint64(20)
	manager := newTSafeWaiterManager(latest)

	current, waiter, closed := manager.addWaiter(15)

	assert.False(t, closed)
	assert.Nil(t, waiter)
	assert.EqualValues(t, 20, current)
}

func TestTSafeWaiterManagerCancelWaiter(t *testing.T) {
	latest := atomic.NewUint64(10)
	manager := newTSafeWaiterManager(latest)

	_, waiter, closed := manager.addWaiter(15)
	require.False(t, closed)
	require.NotNil(t, waiter)

	manager.cancel(waiter)
	manager.update(15)

	assertNotClosed(t, waiter.done)
	assert.EqualValues(t, 15, latest.Load())
}

func TestTSafeWaiterManagerCloseAll(t *testing.T) {
	latest := atomic.NewUint64(10)
	manager := newTSafeWaiterManager(latest)

	_, waiter15, closed := manager.addWaiter(15)
	require.False(t, closed)
	require.NotNil(t, waiter15)
	_, waiter20, closed := manager.addWaiter(20)
	require.False(t, closed)
	require.NotNil(t, waiter20)

	manager.closeAll()

	assertClosed(t, waiter15.done)
	assertClosed(t, waiter20.done)

	_, waiter, closed := manager.addWaiter(25)
	assert.True(t, closed)
	assert.Nil(t, waiter)
}

func assertClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		t.Fatal("expected channel to be closed")
	}
}

func assertNotClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("expected channel to remain open")
	default:
	}
}
