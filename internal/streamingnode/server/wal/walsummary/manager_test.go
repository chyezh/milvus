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
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type recordingScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return recordingTaskHandle{}
}

type recordingTaskHandle struct{}

func (recordingTaskHandle) Cancel() {}

func (recordingTaskHandle) Wait(context.Context) error { return nil }

func newTestManager(t *testing.T, store *Store, flushMaxBytes, retentionMaxBytes uint64) *Manager {
	t.Helper()
	return NewManager(ManagerConfig{
		PChannel:          store.PChannel(),
		Term:              store.Term(),
		Store:             store,
		Runtime:           moduleapi.Runtime{},
		FlushMaxBytes:     flushMaxBytes,
		RetentionMaxBytes: retentionMaxBytes,
	})
}

func newTestManagerWithStore(t *testing.T) (*Manager, *Store) {
	t.Helper()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	return newTestManager(t, store, 1<<20, 1<<30), store
}

// newTestDeleteMessage builds a delete message of the given vchannel.
func newTestDeleteMessage(t *testing.T, vchannel string, timetick uint64, partitionID int64, pks ...int64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  partitionID,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

// observeDelete observes one delete message and releases the owner, setting
// *finalized when the message is fully released (no retained handle survives).
func observeDelete(t *testing.T, view *SummaryView, timetick uint64, finalized *bool) {
	t.Helper()
	msg := newTestDeleteMessage(t, view.VChannel(), timetick, 10, int64(timetick))
	owner := message.NewOwnedImmutableMessage(msg, func() { *finalized = true })
	retained := owner.Clone()
	view.ObserveMessage(context.Background(), retained)
	retained.Release()
	owner.Release()
}

func TestViewObserveAndFlushReleasesHandles(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	view := manager.View("v1")
	ctx := context.Background()

	// A payload-free message (insert) is not retained at all.
	insert := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{CollectionID: 1}).
		MustBuildMutable().
		WithTimeTick(1).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	insertFinalized := false
	insertOwner := message.NewOwnedImmutableMessage(insert, func() { insertFinalized = true })
	insertRetained := insertOwner.Clone()
	view.ObserveMessage(ctx, insertRetained)
	insertRetained.Release()
	insertOwner.Release()
	// Not retained: the owner release finalized the message immediately.
	assert.True(t, insertFinalized)

	// A delete message is retained until the flush is durable.
	finalized := false
	observeDelete(t, view, 100, &finalized)
	assert.False(t, finalized, "message must stay alive while staging")

	require.NoError(t, manager.flushOnce(ctx))
	assert.True(t, finalized, "message handle must be released after a durable flush")
	assert.Equal(t, uint64(100), view.DurableTimeTick())
	assert.Equal(t, uint64(100), manager.LatestCoveredTimeTick())

	// The chunk is readable and carries the record.
	decoded, footer, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(0), footer.GetGeneration())
	require.Len(t, decoded["v1"], 1)
	assert.Equal(t, uint64(100), decoded["v1"][0].GetTimeTick())
	assert.Equal(t, int64(10), decoded["v1"][0].GetDelete().GetBlocks()[0].GetPartitionId())
}

func TestViewFlushFailureKeepsHandles(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	view := manager.View("v1")
	ctx := context.Background()

	finalized := false
	observeDelete(t, view, 100, &finalized)

	// Break the chunk manager so the chunk write fails.
	original := manager.cfg.Store.chunkManager
	manager.cfg.Store.chunkManager = &failingChunkManager{ChunkManager: original}
	err := manager.flushOnce(ctx)
	assert.Error(t, err)
	manager.cfg.Store.chunkManager = original
	assert.False(t, finalized, "handles must survive a failed flush")
	// The staging must still be there for the retry.
	require.NoError(t, manager.flushOnce(ctx))
	assert.True(t, finalized)
}

type failingChunkManager struct {
	storage.ChunkManager
}

func (f *failingChunkManager) Exist(context.Context, string) (bool, error) {
	return false, errors.New("injected failure")
}

func TestManagerRecover(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	view := manager.View("v1")
	ctx := context.Background()

	// Two flushes produce two chunks.
	unused := false
	observeDelete(t, view, 100, &unused)
	require.NoError(t, manager.flushOnce(ctx))
	observeDelete(t, view, 200, &unused)
	require.NoError(t, manager.flushOnce(ctx))

	// A new manager over the same store recovers both chunks and continues
	// generations after them.
	recovered := newTestManager(t, manager.cfg.Store, 1<<20, 1<<30)
	require.NoError(t, recovered.Recover(ctx))
	assert.Equal(t, uint64(2), recovered.nextGeneration)
	assert.Equal(t, uint64(200), recovered.LatestCoveredTimeTick())
	assert.Len(t, recovered.Manifest().GetChunks(), 2)
}

func TestManagerRecoverProbesOrphanChunk(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	view := manager.View("v1")
	ctx := context.Background()

	unused := false
	observeDelete(t, view, 100, &unused)
	require.NoError(t, manager.flushOnce(ctx))

	// Simulate a crash between chunk write and manifest publish: write a chunk
	// directly without recording it.
	orphanRecords := map[string][]*streamingpb.VChannelSummaryTransformRecord{
		"v1": {{TimeTick: 300, Delete: &streamingpb.TransformDeleteEntry{}}},
	}
	_, _, err := manager.cfg.Store.WriteChunk(ctx, 2, orphanRecords)
	require.NoError(t, err)

	recovered := newTestManager(t, manager.cfg.Store, 1<<20, 1<<30)
	require.NoError(t, recovered.Recover(ctx))
	assert.Equal(t, uint64(3), recovered.nextGeneration)
	assert.Equal(t, uint64(300), recovered.LatestCoveredTimeTick())
	require.Len(t, recovered.Manifest().GetChunks(), 2)
	// The probed tail is sealed into a published manifest: a third recovery
	// sees it without probing again.
	again := newTestManager(t, manager.cfg.Store, 1<<20, 1<<30)
	require.NoError(t, again.Recover(ctx))
	assert.Equal(t, uint64(3), again.nextGeneration)
}

func TestManagerGCReleaseAndMaterializationFloor(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	view := manager.View("v1")
	ctx := context.Background()

	flush := func(tt uint64) {
		unused := false
		observeDelete(t, view, tt, &unused)
		require.NoError(t, manager.flushOnce(ctx))
	}
	flush(100)
	flush(200)
	flush(300)
	require.Len(t, manager.Manifest().GetChunks(), 3)

	// Without a materialization frontier nothing is eligible.
	manager.cfg.RetentionMaxBytes = 0
	require.NoError(t, manager.GCOnce(ctx))
	assert.Len(t, manager.Manifest().GetChunks(), 3)

	// Materialize through 200 (a completed frontier): chunks 0 (end 100) and
	// 1 (end 200) are fully consumed and released; chunk 2 (end 300) still
	// holds un-materialized records and stays.
	manager.SetMaterializedTimeTick("v1", 200)
	require.NoError(t, manager.GCOnce(ctx))
	chunks := manager.Manifest().GetChunks()
	require.Len(t, chunks, 1)
	assert.Equal(t, uint64(2), chunks[0].GetGeneration())
	// The released object is gone.
	_, _, err := manager.cfg.Store.ReadChunk(ctx, 0, 1)
	assert.Error(t, err)
	// pending_gc drained.
	assert.Empty(t, manager.Manifest().GetPendingGc())

	// Materialize everything: all chunks are released.
	manager.SetMaterializedTimeTick("v1", 400)
	require.NoError(t, manager.GCOnce(ctx))
	assert.Empty(t, manager.Manifest().GetChunks())
}

func TestViewRequestPersistThroughSchedulesFlush(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	scheduler := &recordingScheduler{}
	manager.cfg.Runtime.Scheduler = scheduler
	view := manager.View("v1")

	unused := false
	observeDelete(t, view, 100, &unused)
	// Observe below the byte threshold schedules nothing.
	assert.Empty(t, scheduler.tasks)
	observeDelete(t, view, 150, &unused)
	view.RequestPersistThrough(160)
	assert.Len(t, scheduler.tasks, 1)
}

func TestViewSkipsRecordsAtOrBelowDurableFrontier(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()
	require.NoError(t, manager.Recover(ctx))
	manager.SetDurableTimeTick("v1", 150)
	view := manager.View("v1")
	manager.cfg.FlushMaxBytes = 1 // any retained record triggers.

	// Records at or below the restored durable frontier are skipped entirely:
	// recovery replay re-observes them, and staging them again would rewrite
	// the same records into a new chunk.
	unused := false
	observeDelete(t, view, 100, &unused)
	observeDelete(t, view, 150, &unused)
	assert.Empty(t, manager.flushTasks)
	assert.Empty(t, view.staging)

	// A record past the frontier is staged as usual.
	observeDelete(t, view, 200, &unused)
	assert.Len(t, manager.flushTasks, 1)
	require.NoError(t, manager.flushTasks[0].Execute(ctx))
	entries, err := manager.ReadTransformEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(200), entries[0].GetTimeTick())
}

func TestManagerDurableTimeTickDerivedFromManifest(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Recover(ctx))
	assert.Zero(t, manager.DurableTimeTick("v1"))

	var unused bool
	observeDelete(t, manager.View("v1"), 100, &unused)
	observeDelete(t, manager.View("v1"), 200, &unused)
	manager.requestFlush()
	require.NoError(t, manager.flushOnce(ctx))
	assert.Equal(t, uint64(200), manager.DurableTimeTick("v1"))

	// A vchannel with no records has no frontier.
	assert.Zero(t, manager.DurableTimeTick("v2"))
}

func TestManagerSetDurableTimeTickAppliesToExistingView(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	view := manager.View("v1")
	manager.SetDurableTimeTick("v1", 250)
	assert.Equal(t, uint64(250), view.DurableTimeTick())
}

func TestViewObserveAboveThresholdSchedulesFlush(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	scheduler := &recordingScheduler{}
	manager.cfg.Runtime.Scheduler = scheduler
	view := manager.View("v1")

	manager.cfg.FlushMaxBytes = 1 // any record triggers.
	unused := false
	observeDelete(t, view, 100, &unused)
	assert.Len(t, scheduler.tasks, 1)
}

func TestManagerHasPendingWork(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	view := manager.View("v1")
	ctx := context.Background()

	assert.False(t, manager.HasPendingWork())
	unused := false
	observeDelete(t, view, 100, &unused)
	// Staging alone is not pending work (no flush scheduled).
	assert.False(t, manager.HasPendingWork())
	manager.requestFlush()
	assert.True(t, manager.HasPendingWork())
	// Executing the scheduled task completes it and drains the queue.
	require.Len(t, manager.flushTasks, 1)
	require.NoError(t, manager.flushTasks[0].Execute(ctx))
	assert.False(t, manager.HasPendingWork())
}

func TestFlushTaskMergesConcurrentRequests(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	ctx := context.Background()

	manager.requestFlush()
	manager.requestFlush()
	manager.requestFlush()
	// At most one flush task exists at a time: the executing task collects
	// every view's staging, so concurrent requests merge into it instead of
	// queueing one task per request.
	require.Len(t, manager.flushTasks, 1)
	require.NoError(t, manager.flushTasks[0].Execute(ctx))
	assert.False(t, manager.HasPendingWork())
	// The completed task is compacted away; a later request schedules the
	// successor.
	manager.requestFlush()
	require.Len(t, manager.flushTasks, 1)
	require.NoError(t, manager.flushTasks[0].Execute(ctx))
}

func TestManagerFlushReleasesHandles(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Recover(ctx))
	view := manager.View("v1")

	finalized := false
	observeDelete(t, view, 100, &finalized)
	manager.requestFlush()
	require.NoError(t, manager.flushOnce(ctx))
	assert.True(t, finalized, "handles released after the flush is durable end to end")
}

func TestManagerReadTransformEntriesAcrossChunks(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.NoError(t, manager.Recover(ctx))
	manager.View("v1")

	// Two flushes produce two chunks; recovery-style reads span them.
	var unused bool
	for _, tt := range []uint64{100, 200} {
		observeDelete(t, manager.View("v1"), tt, &unused)
		manager.requestFlush()
		require.NoError(t, manager.flushOnce(ctx))
	}
	entries, err := manager.ReadTransformEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, uint64(100), entries[0].GetTimeTick())
	assert.Equal(t, uint64(200), entries[1].GetTimeTick())

	// The from-boundary is exclusive.
	entries, err = manager.ReadTransformEntries(ctx, "v1", 100, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(200), entries[0].GetTimeTick())
}

type failInjectingChunkManager struct {
	storage.ChunkManager
	failManifest atomic.Bool
}

func (c *failInjectingChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	if c.failManifest.Load() && strings.Contains(filePath, manifestObjectExt) {
		return errors.New("injected manifest write failure")
	}
	return c.ChunkManager.Write(ctx, filePath, content)
}

// TestFlushPublishFailureRollsBackAndRetriesSameGeneration covers the retry
// path: the chunk is written, the manifest publish fails, and the retry must
// rewrite the SAME generation — never a second chunk object for the same
// batch — so a reader can never observe the batch twice.
func TestFlushPublishFailureRollsBackAndRetriesSameGeneration(t *testing.T) {
	ctx := context.Background()
	cm := &failInjectingChunkManager{
		ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())),
	}
	store := NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
	manager := newTestManager(t, store, 1, 1<<30)
	require.NoError(t, manager.Recover(ctx))
	view := manager.View("v1")
	finalized := false

	cm.failManifest.Store(true)
	observeDelete(t, view, 100, &finalized)
	// Chunk write succeeds, manifest publish fails.
	require.Error(t, manager.flushOnce(ctx))
	// The amendment and the claimed generation were rolled back; the handles
	// stay retained because the batch is not durable end to end.
	assert.Len(t, manager.manifest.GetChunks(), 0)
	assert.Equal(t, uint64(0), manager.nextGeneration)
	assert.False(t, finalized)

	// The retry succeeds and rewrites the same generation: exactly one chunk
	// object and one manifest entry.
	cm.failManifest.Store(false)
	require.NoError(t, manager.flushOnce(ctx))
	assert.True(t, finalized)
	require.Len(t, manager.manifest.GetChunks(), 1)
	assert.Equal(t, uint64(0), manager.manifest.GetChunks()[0].GetGeneration())
	keys, _, err := storage.ListAllChunkWithPrefix(ctx, cm, buildChunkPrefix(cm, store.PChannel()), false)
	require.NoError(t, err)
	assert.Len(t, keys, 1, "the retry must not duplicate the chunk object")
}

// TestGCOnceRemovesAllPendingCovers the snapshot iteration of the pending GC
// queue: removePendingGC compacts the live slice in place, and a naive range
// over the live slice would skip entries once the indexes shift.
func TestGCOnceRemovesAllPending(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	require.NoError(t, manager.Recover(ctx))

	// Seed three chunks and three pending refs, and write the objects.
	manager.mu.Lock()
	for gen := uint64(0); gen < 3; gen++ {
		manager.manifest.Chunks = append(manager.manifest.Chunks, &streamingpb.PChannelSummaryChunkIndexEntry{
			Generation:    gen,
			Term:          1,
			StartTimetick: gen * 100,
			EndTimetick:   gen*100 + 50,
		})
		manager.manifest.PendingGc = append(manager.manifest.PendingGc, &streamingpb.PChannelSummaryChunkRef{
			Generation: gen,
			Term:       1,
		})
	}
	manager.mu.Unlock()
	for gen := uint64(0); gen < 3; gen++ {
		_, _, err := store.WriteChunk(ctx, gen, nil)
		require.NoError(t, err)
	}

	require.NoError(t, manager.GCOnce(ctx))
	assert.Empty(t, manager.manifest.GetPendingGc())
	for gen := uint64(0); gen < 3; gen++ {
		_, _, err := store.ReadChunk(ctx, gen, 1)
		require.Error(t, err, "chunk %d must be deleted", gen)
	}
}

// TestRestoreStagingAfterViewRemovedReleasesHandles covers the failure path of
// a flush racing a vchannel cleanup: restoring a batch into a view that was
// already removed must release the handles, never leak them.
func TestRestoreStagingAfterViewRemovedReleasesHandles(t *testing.T) {
	manager, _ := newTestManagerWithStore(t)
	view := manager.View("v1")
	finalized := false
	observeDelete(t, view, 100, &finalized)

	batch := manager.collectStaging()
	require.Len(t, batch.recordsByVChannel["v1"], 1)
	manager.RemoveView("v1")
	manager.restoreStaging(batch)
	assert.True(t, finalized, "restoring into a removed view releases the handles")
}

// TestConcurrentFlushAndGCRelease exercises the manifest publish paths
// concurrently. Run with -race: a torn publish (a marshal racing an in-place
// edit of the shared manifest) would surface here as a data race, and the
// serialization through manifestMu is what keeps flush and GC publishes from
// interleaving.
func TestConcurrentFlushAndGCRelease(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	manager.cfg.RetentionMaxBytes = 1 // any chunk becomes releasable once materialized
	require.NoError(t, manager.Recover(ctx))
	view := manager.View("v1")
	manager.SetMaterializedTimeTick("v1", 1<<30)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			unused := false
			observeDelete(t, view, uint64(100+i), &unused)
			require.NoError(t, manager.flushOnce(ctx))
		}
		close(stop)
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := manager.GCOnce(ctx); err != nil {
					t.Errorf("GCOnce: %v", err)
					return
				}
			}
		}
	}()
	wg.Wait()
}
