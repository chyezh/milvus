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

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// TestRecoverInheritsPreviousTermChunks covers the term-handoff path: a chunk
// the previous term persisted (its handles released, the WAL checkpoint may
// have passed it) but whose records were never materialized must stay visible
// to the next term. Without the inheritance the new term's empty manifest
// would hide the delete records forever, resurrecting deleted data.
func TestRecoverInheritsPreviousTermChunks(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	// Term 1 flushes one delete into chunk 0 and publishes its manifest.
	store1 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager1 := newTestManager(t, store1, 1, 1<<30)
	require.NoError(t, manager1.Recover(ctx))
	var unused bool
	observeDelete(t, manager1.View("v1"), 100, &unused)
	require.NoError(t, manager1.flushOnce(ctx))
	assert.Len(t, manager1.manifest.GetChunks(), 1)

	// Term 2 takes over the pchannel (term handoff) and recovers. It must see
	// term 1's chunk through its own manifest.
	store2 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 2)
	manager2 := newTestManager(t, store2, 1, 1<<30)
	require.NoError(t, manager2.Recover(ctx))
	require.Len(t, manager2.manifest.GetChunks(), 1)
	assert.Equal(t, uint64(0), manager2.manifest.GetChunks()[0].GetGeneration())
	assert.Equal(t, int64(1), manager2.manifest.GetChunks()[0].GetTerm())
	assert.Equal(t, uint64(1), manager2.nextGeneration, "generations continue past the inherited set")

	// The durable backlog through the inherited chunk is readable: recovery of
	// the transform consumer loads the un-materialized delete from it.
	entries, err := manager2.ReadTransformEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(100), entries[0].GetTimeTick())

	// Term 2 sealed the inheritance into its own manifest, so the chain never
	// grows beyond one hop: a term 3 recovery reads term 2's manifest.
	loaded, found, err := store2.ReadManifest(ctx)
	require.NoError(t, err)
	require.True(t, found)
	assert.Len(t, loaded.GetChunks(), 1)
	assert.Equal(t, int64(1), loaded.GetChunks()[0].GetTerm())
}

// TestRecoverInheritsPreviousTermProbedTail covers the crash window of the
// previous term: a chunk written but not yet recorded in the manifest before
// the handoff. The new term's recovery must probe the previous term's objects
// and seal them, exactly as it probes its own.
func TestRecoverInheritsPreviousTermProbedTail(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	// Term 1 writes chunk 0 but the manifest publish "crashes" (we simply do
	// not publish).
	store1 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager1 := newTestManager(t, store1, 1, 1<<30)
	require.NoError(t, manager1.Recover(ctx))
	var unused bool
	observeDelete(t, manager1.View("v1"), 100, &unused)
	require.NoError(t, manager1.flushOnce(ctx))
	require.Len(t, manager1.manifest.GetChunks(), 1)

	// Term 2 recovers: it finds no manifest of its own, then probes term 1.
	store2 := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 2)
	manager2 := newTestManager(t, store2, 1, 1<<30)
	require.NoError(t, manager2.Recover(ctx))
	require.Len(t, manager2.manifest.GetChunks(), 1)
	assert.Equal(t, int64(1), manager2.manifest.GetChunks()[0].GetTerm())
}

// TestRecoverFencesOlderTerm ensures the catalog fencing still rejects an
// owner whose term is older than the recorded one after the inheritance
// change.
func TestRecoverFencesOlderTerm(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	catalog := &catalogRecorder{term: 2}
	older := NewManager(ManagerConfig{
		PChannel:    "by-dev-rootcoord-dml_0_40451v0",
		Term:        1,
		Store:       store,
		MetaCatalog: catalog,
	})
	err := older.Recover(ctx)
	require.Error(t, err)
}

type catalogRecorder struct {
	term int64
}

func (c *catalogRecorder) GetPChannelSummaryMeta(ctx context.Context, pchannel string) (*streamingpb.PChannelSummaryMeta, error) {
	return &streamingpb.PChannelSummaryMeta{Pchannel: pchannel, Term: c.term}, nil
}

func (c *catalogRecorder) SavePChannelSummaryMeta(ctx context.Context, pchannel string, meta *streamingpb.PChannelSummaryMeta) error {
	c.term = meta.GetTerm()
	return nil
}
