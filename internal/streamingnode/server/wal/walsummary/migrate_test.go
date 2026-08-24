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

package walsummary

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestManagerMigrateLegacyTransformLogs(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	require.NoError(t, manager.Recover(ctx))

	records := map[string][]*streamingpb.TransformLogEntry{
		"v1": {newTestTransformEntry(100), newTestTransformEntry(200)},
		"v2": {newTestTransformEntry(150)},
	}
	require.NoError(t, manager.MigrateLegacyTransformLogs(ctx, records))

	// The migrated data is readable through the transform reader.
	entries, err := manager.ReadTransformEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, uint64(100), entries[0].GetTimeTick())
	assert.Equal(t, uint64(200), entries[1].GetTimeTick())

	entries, err = manager.ReadTransformEntries(ctx, "v2", 0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(150), entries[0].GetTimeTick())

	// The migration is idempotent: a second call writes nothing.
	before := len(manager.Manifest().GetChunks())
	require.NoError(t, manager.MigrateLegacyTransformLogs(ctx, records))
	assert.Len(t, manager.Manifest().GetChunks(), before)

	// A fresh manager over the same store recovers the migrated chunk.
	recovered := newTestManager(t, store, 1<<20, 1<<30)
	require.NoError(t, recovered.Recover(ctx))
	assert.Len(t, recovered.Manifest().GetChunks(), before)
}

func TestManagerMigrateLegacyTransformLogsRejectsUnsorted(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Recover(ctx))

	err := manager.MigrateLegacyTransformLogs(ctx, map[string][]*streamingpb.TransformLogEntry{
		"v1": {newTestTransformEntry(200), newTestTransformEntry(100)},
	})
	assert.Error(t, err)
	assert.Empty(t, manager.Manifest().GetChunks())
}

func TestManagerMigrateLegacyTransformLogsSkipsWhenStoreOwnsChunks(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Recover(ctx))

	// A normal flush already owns a chunk: migration must not run.
	finalized := false
	observeDelete(t, manager.View("v1"), 10, &finalized)
	manager.requestFlush()
	require.NoError(t, manager.flushOnce(ctx, &summaryFlushTask{log: manager}))

	require.NoError(t, manager.MigrateLegacyTransformLogs(ctx, map[string][]*streamingpb.TransformLogEntry{
		"v2": {newTestTransformEntry(150)},
	}))
	// Only the flushed chunk is recorded; the migration was skipped.
	assert.Len(t, manager.Manifest().GetChunks(), 1)
	entries, err := manager.ReadTransformEntries(ctx, "v2", 0, 1000)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func newTestTransformEntry(timetick uint64) *streamingpb.TransformLogEntry {
	return &streamingpb.TransformLogEntry{
		TimeTick: timetick,
		Entry: &streamingpb.TransformLogEntry_Delete{
			Delete: &streamingpb.TransformDeleteEntry{
				Blocks: []*streamingpb.TransformDeleteBlock{
					{
						PartitionId: 10,
						PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{Data: []int64{int64(timetick)}},
						}},
					},
				},
			},
		},
	}
}
