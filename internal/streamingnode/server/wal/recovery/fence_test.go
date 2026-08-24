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

package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	walimplstest "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// TestFenceConsumeCheckpointStampsOwnTerm proves the takeover fence writes
// the checkpoint back with this recovery's term through the checkpoint CAS of
// SaveRecoverySnapshot, preserving the published position.
func TestFenceConsumeCheckpointStampsOwnTerm(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
	})
	storage.channel.Term = 3

	var received *streamingpb.WALCheckpoint
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().SaveRecoverySnapshot(mock.Anything, "test-pchannel",
		mock.MatchedBy(func(snapshot *metastore.WALRecoverySnapshot) bool {
			require.NotNil(t, snapshot)
			require.NotNil(t, snapshot.ConsumeCheckpoint)
			received = snapshot.ConsumeCheckpoint
			return true
		})).Return(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	require.NoError(t, storage.fenceConsumeCheckpoint(context.Background()))
	require.NotNil(t, received)
	assert.Equal(t, int64(3), received.GetTerm(), "fence must stamp the own term")
	assert.Equal(t, uint64(10), received.GetTimeTick(), "fence must preserve the published position")
	assert.Equal(t, "10", received.GetMessageId().GetId(), "fence must preserve the published message id")
}

// TestFenceConsumeCheckpointNilCheckpointSkips proves the fence is a no-op
// when no checkpoint was ever published: nothing is written, the open simply
// starts the checkpoint chain on its first persistence.
func TestFenceConsumeCheckpointNilCheckpointSkips(t *testing.T) {
	storage := newTestRecoveryStorage(t, nil)
	// No catalog expectation at all: any catalog call would fail the test.
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(mock_metastore.NewMockStreamingNodeCataLog(t)))

	require.NoError(t, storage.fenceConsumeCheckpoint(context.Background()))
}
