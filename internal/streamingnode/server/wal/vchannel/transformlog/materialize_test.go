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

package transformlog

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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

// recordingMaterializer records every Materialize call.
type recordingMaterializer struct {
	mu      sync.Mutex
	batches []MaterializeRequest
}

func (m *recordingMaterializer) Materialize(_ context.Context, req MaterializeRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, req)
	return nil
}

func (m *recordingMaterializer) calls() []MaterializeRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]MaterializeRequest(nil), m.batches...)
}

// failingMaterializer fails until released.
type failingMaterializer struct {
	mu    sync.Mutex
	fail  bool
	calls int
}

func (m *failingMaterializer) Materialize(_ context.Context, _ MaterializeRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.fail {
		return context.DeadlineExceeded
	}
	return nil
}

func (m *failingMaterializer) setFail(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = fail
}

func (m *failingMaterializer) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func newTestDeleteEntry(timetick uint64) *streamingpb.TransformLogEntry {
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

// flushed delivers a flush event of the vchannel with the given entries.
func flushed(t *testing.T, log *TransformLog, entries ...*streamingpb.TransformLogEntry) {
	t.Helper()
	log.OnSummaryFlushed(&walsummary.FlushedBatch{
		RecordsByVChannel: map[string][]*streamingpb.TransformLogEntry{"v1": entries},
		CoveredTimeTick:   entries[len(entries)-1].GetTimeTick(),
	})
}

func newTestTransformLog(t *testing.T, materializer Materializer, initialMaterialized uint64) (*TransformLog, *recordingScheduler, *recordingMaterializer) {
	t.Helper()
	scheduler := &recordingScheduler{}
	rec := &recordingMaterializer{}
	m := materializer
	if m == nil {
		m = rec
	}
	log := New(Config{
		VChannel:             "v1",
		MaterializedTimeTick: initialMaterialized,
		MaterializeMaxRows:   500000,
		MaterializeMaxBytes:  32 * 1024 * 1024,
		Materializer:         m,
		Runtime:              moduleapi.Runtime{Scheduler: scheduler},
	})
	return log, scheduler, rec
}

func TestTransformLogMaterializeThroughRequest(t *testing.T) {
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	flushed(t, log, newTestDeleteEntry(100), newTestDeleteEntry(200), newTestDeleteEntry(300))
	onMaterialized := []uint64{}
	log.onMaterialized = func(tt uint64) { onMaterialized = append(onMaterialized, tt) }

	require.True(t, log.RequestMaterializeThrough(300))
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(300), log.MaterializedTimeTick())
	assert.Equal(t, []uint64{300}, onMaterialized)

	calls := rec.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, uint64(300), calls[0].TargetTimeTick)
	assert.Len(t, calls[0].Entries, 3)

	// A repeated request for the same frontier is a no-op.
	require.False(t, log.RequestMaterializeThrough(300))
}

func TestTransformLogMaterializeCapsBatchAndContinues(t *testing.T) {
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	flushed(t, log, newTestDeleteEntry(100), newTestDeleteEntry(200), newTestDeleteEntry(300))
	log.materializeMaxRows = 1

	require.True(t, log.RequestMaterializeThrough(300))
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(100), log.MaterializedTimeTick())
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())
	require.Len(t, scheduler.tasks, 3)
	require.NoError(t, scheduler.tasks[2].Execute(context.Background()))
	assert.Equal(t, uint64(300), log.MaterializedTimeTick())

	calls := rec.calls()
	require.Len(t, calls, 3)
	assert.Equal(t, uint64(100), calls[0].TargetTimeTick)
	assert.Equal(t, uint64(200), calls[1].TargetTimeTick)
	assert.Equal(t, uint64(300), calls[2].TargetTimeTick)
}

func TestTransformLogMaterializeRetriesOnFailure(t *testing.T) {
	fail := &failingMaterializer{}
	log, scheduler, _ := newTestTransformLog(t, fail, 0)
	flushed(t, log, newTestDeleteEntry(100))
	fail.setFail(true)

	require.True(t, log.RequestMaterializeThrough(100))
	err := scheduler.tasks[0].Execute(context.Background())
	assert.Error(t, err)
	assert.False(t, scheduler.tasks[0].(*transformMaterializeTask).Done())
	assert.Equal(t, uint64(0), log.MaterializedTimeTick())

	fail.setFail(false)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, scheduler.tasks[0].(*transformMaterializeTask).Done())
	assert.Equal(t, uint64(100), log.MaterializedTimeTick())
}

func TestTransformLogMaterializeUpperBoundLimitsTarget(t *testing.T) {
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	flushed(t, log, newTestDeleteEntry(100), newTestDeleteEntry(200), newTestDeleteEntry(300))

	require.True(t, log.RequestMaterializeThrough(300))
	require.Len(t, scheduler.tasks, 1)

	// The upper bound retracts the frontier before the task runs.
	log.SetMaterializeUpperBound(200)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())

	// Advancing the bound continues the retained request without a new WAL
	// trigger.
	log.SetMaterializeUpperBound(300)
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.Equal(t, uint64(300), log.MaterializedTimeTick())
	assert.Len(t, rec.calls(), 2)
}

func TestTransformLogMaterializeTargetCappedByDurableTimeTick(t *testing.T) {
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	// Only 100 is durable: requesting 300 must not materialize records the
	// summary has not persisted yet.
	flushed(t, log, newTestDeleteEntry(100))

	require.True(t, log.RequestMaterializeThrough(300))
	require.Error(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(0), log.MaterializedTimeTick())
	require.Empty(t, rec.calls())

	// The next flush event delivers the rest; the pending task completes.
	flushed(t, log, newTestDeleteEntry(300))
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(300), log.MaterializedTimeTick())
	require.Len(t, rec.calls(), 1)
}

func TestTransformLogMaterializeWaitsForFlushEvent(t *testing.T) {
	log, scheduler, rec := newTestTransformLog(t, nil, 0)

	// Nothing is durable: the request is recorded, the task stays delayed,
	// and nothing is emitted.
	require.True(t, log.RequestMaterializeThrough(100))
	require.Len(t, scheduler.tasks, 1)
	require.Error(t, scheduler.tasks[0].Execute(context.Background()))
	require.Empty(t, rec.calls())
	assert.Equal(t, uint64(0), log.MaterializedTimeTick())

	// The flush event replaces the window; the pending task completes.
	flushed(t, log, newTestDeleteEntry(100))
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(100), log.MaterializedTimeTick())
	require.Len(t, rec.calls(), 1)
}

func TestTransformLogFlushEventUnblocksPendingTask(t *testing.T) {
	log, scheduler, rec := newTestTransformLog(t, nil, 0)
	require.True(t, log.RequestMaterializeThrough(200))
	require.Len(t, scheduler.tasks, 1)

	// The pending task stays delayed until the flush event moves the window;
	// the event does not schedule a duplicate (pending-target dedup).
	flushed(t, log, newTestDeleteEntry(100), newTestDeleteEntry(200))
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())
	require.Len(t, rec.calls(), 1)
}

func TestTransformLogRecoveryWindow(t *testing.T) {
	// Recovery loads the durable backlog into the initial window: records
	// after the restored frontier are materializable without any flush event.
	log, scheduler, rec := newTestTransformLog(t, nil, 100)
	log.pending = []*streamingpb.TransformLogEntry{newTestDeleteEntry(200)}
	log.durableTimeTick = 200

	require.True(t, log.RequestMaterializeThrough(200))
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, uint64(200), log.MaterializedTimeTick())
	assert.Empty(t, log.pending)
	require.Len(t, rec.calls(), 1)
}
