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
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var _ broadcaster.BroadcastAPI = (*localApplyBroadcastAPI)(nil)

// localApplyBroadcastAPI is a BroadcastAPI implementation that bypasses the broadcaster system
// and directly calls the registered ack callback. Used on non-primary clusters for message types
// that are configured to be skipped during replication.
type localApplyBroadcastAPI struct{}

// Broadcast checks whether the message type is allowed for local apply (via property or config),
// and if so, directly calls the registered ack callback instead of going through the broadcaster.
func (l *localApplyBroadcastAPI) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	msgType := msg.MessageType()
	if !isReplicationSkippable(msgType) {
		return nil, broadcaster.ErrNotPrimary
	}
	if err := registry.CallMessageAckCallback(ctx, msg, nil); err != nil {
		return nil, err
	}
	return &types.BroadcastAppendResult{}, nil
}

// Close is a no-op since localApplyBroadcastAPI does not hold any resources.
func (l *localApplyBroadcastAPI) Close() {}

// isReplicationSkippable checks whether the message type can be skipped during replication.
// A message type is skippable if its property is set OR it appears in the config list.
func isReplicationSkippable(msgType message.MessageType) bool {
	if msgType.IsReplicationSkippable() {
		return true
	}
	for _, t := range paramtable.Get().StreamingCfg.ReplicationSkipMessageTypes.GetAsStrings() {
		if t == msgType.String() {
			return true
		}
	}
	return false
}
