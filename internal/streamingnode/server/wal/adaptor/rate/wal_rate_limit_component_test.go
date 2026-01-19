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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestWALRateLimitComponent(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "test-channel"}
	component := NewWALRateLimitComponent(channel)
	defer component.Close()

	assert.NotNil(t, component.RecoveryStorage)
	assert.NotNil(t, component.FlusherRecovering)
	assert.NotNil(t, component.NodeMemory)

	assert.False(t, component.IsRejected())
	component.RecoveryStorage.EnterRejectMode()
	assert.True(t, component.IsRejected())

	component.RecoveryStorage.EnterRecoveryMode()
	assert.False(t, component.IsRejected())

	component.hardwardCallback(hardware.SystemMetrics{UsedMemoryBytes: 91, TotalMemoryBytes: 100}, nil)
	assert.False(t, component.IsRejected())

	component.hardwardCallback(hardware.SystemMetrics{UsedMemoryBytes: 100, TotalMemoryBytes: 100}, nil)
	assert.True(t, component.IsRejected())

	component.hardwardCallback(hardware.SystemMetrics{UsedMemoryBytes: 91, TotalMemoryBytes: 100}, nil)
	assert.True(t, component.IsRejected())

	component.hardwardCallback(hardware.SystemMetrics{UsedMemoryBytes: 84, TotalMemoryBytes: 100}, nil)
	assert.False(t, component.IsRejected())

	component.RegisterMemoryObserver()
	assert.NotNil(t, component.handler)
}
