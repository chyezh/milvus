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

package broadcast

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestIsReplicationSkippable(t *testing.T) {
	paramtable.Init()

	t.Run("property_based_skippable_resource_group", func(t *testing.T) {
		assert.True(t, isReplicationSkippable(message.MessageTypeAlterResourceGroup))
		assert.True(t, isReplicationSkippable(message.MessageTypeDropResourceGroup))
	})

	t.Run("property_based_skippable_rbac", func(t *testing.T) {
		assert.True(t, isReplicationSkippable(message.MessageTypeAlterUser))
		assert.True(t, isReplicationSkippable(message.MessageTypeDropUser))
		assert.True(t, isReplicationSkippable(message.MessageTypeAlterRole))
		assert.True(t, isReplicationSkippable(message.MessageTypeDropRole))
		assert.True(t, isReplicationSkippable(message.MessageTypeAlterUserRole))
		assert.True(t, isReplicationSkippable(message.MessageTypeDropUserRole))
		assert.True(t, isReplicationSkippable(message.MessageTypeAlterPrivilege))
		assert.True(t, isReplicationSkippable(message.MessageTypeDropPrivilege))
		assert.True(t, isReplicationSkippable(message.MessageTypeAlterPrivilegeGroup))
		assert.True(t, isReplicationSkippable(message.MessageTypeDropPrivilegeGroup))
		assert.True(t, isReplicationSkippable(message.MessageTypeRestoreRBAC))
	})

	t.Run("not_skippable_by_default", func(t *testing.T) {
		assert.False(t, isReplicationSkippable(message.MessageTypeCreateCollection))
		assert.False(t, isReplicationSkippable(message.MessageTypeDropCollection))
	})

	t.Run("config_override_makes_skippable", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().StreamingCfg.ReplicationSkipMessageTypes.Key, "CreateCollection")
		defer paramtable.Get().Reset(paramtable.Get().StreamingCfg.ReplicationSkipMessageTypes.Key)

		assert.True(t, isReplicationSkippable(message.MessageTypeCreateCollection))
		// Property-based ones still work
		assert.True(t, isReplicationSkippable(message.MessageTypeAlterResourceGroup))
	})

	t.Run("empty_config_still_uses_property", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().StreamingCfg.ReplicationSkipMessageTypes.Key, "")
		defer paramtable.Get().Reset(paramtable.Get().StreamingCfg.ReplicationSkipMessageTypes.Key)

		// Property-based ones still work even with empty config
		assert.True(t, isReplicationSkippable(message.MessageTypeAlterResourceGroup))
		assert.True(t, isReplicationSkippable(message.MessageTypeDropResourceGroup))
	})
}

func TestLocalApplyBroadcastAPI_Close(t *testing.T) {
	api := &localApplyBroadcastAPI{}
	// Close should be a no-op and not panic
	api.Close()
}
