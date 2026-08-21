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

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type transformTask interface {
	nodescheduler.Task
	Done() bool
}

type transformTaskBase struct {
	log          *TransformLog
	timetick     uint64
	predecessors []transformTask
	done         atomic.Bool
}

func (t *transformTaskBase) Done() bool {
	return t.done.Load()
}

func (t *transformTaskBase) predecessorsDone() bool {
	for _, predecessor := range t.predecessors {
		if predecessor != nil && !predecessor.Done() {
			return false
		}
	}
	return true
}

func (t *transformTaskBase) execute(ctx context.Context, ready bool, fn func(context.Context) error) error {
	if !ready {
		return nodescheduler.ErrDelay
	}
	err := fn(ctx)
	if err == nil {
		t.done.Store(true)
		return nil
	}
	return errors.Mark(err, nodescheduler.ErrDelay)
}

type transformMaterializeTask struct {
	transformTaskBase
}

func (t *transformMaterializeTask) Execute(ctx context.Context) error {
	// The task runs only once the summary has made the requested frontier
	// durable: a flush event moves the window, and only then may
	// materialization consume it.
	ready := t.predecessorsDone() && t.log.DurableTimeTick() >= t.timetick
	return t.execute(ctx, ready, func(ctx context.Context) error {
		if _, err := t.log.materialize(ctx, materializeOption{TargetTimeTick: t.timetick}); err != nil {
			return err
		}
		return nil
	})
}

// newRequestedMaterializeTaskLocked returns a task for the current requested
// frontier, or nil when there is nothing to do or a task is already pending.
func (t *TransformLog) newRequestedMaterializeTaskLocked() *transformMaterializeTask {
	target := t.materializeTargetLocked()
	if target <= t.materializedTimeTick || t.pendingMaterializeTargetLocked() >= target {
		return nil
	}
	return t.newMaterializeTaskLocked(target)
}

// newMaterializeTaskLocked appends a materialize task for target without the
// pending-target dedup of newRequestedMaterializeTaskLocked. It continues a
// capped batch: the current task is still pending (and becomes a predecessor of
// the new one), so execution order keeps the batches sequential.
func (t *TransformLog) newMaterializeTaskLocked(target uint64) *transformMaterializeTask {
	task := &transformMaterializeTask{
		transformTaskBase: transformTaskBase{
			log:          t,
			timetick:     target,
			predecessors: t.taskPredecessorsLocked(),
		},
	}
	t.materializeTasks = append(t.materializeTasks, task)
	return task
}

func (t *TransformLog) taskPredecessorsLocked() []transformTask {
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	predecessors := make([]transformTask, 0, len(t.materializeTasks))
	for _, task := range t.materializeTasks {
		predecessors = append(predecessors, task)
	}
	return predecessors
}

func (t *TransformLog) HasPendingMaterializeTask() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	return len(t.materializeTasks) > 0
}

func (t *TransformLog) pendingMaterializeTargetLocked() uint64 {
	t.materializeTasks = compactTransformMaterializeTasks(t.materializeTasks)
	var target uint64
	for _, task := range t.materializeTasks {
		if task.timetick > target {
			target = task.timetick
		}
	}
	return target
}

func compactTransformMaterializeTasks(tasks []*transformMaterializeTask) []*transformMaterializeTask {
	pending := tasks[:0]
	for _, task := range tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
	}
	clear(pending[len(pending):])
	return pending
}

var (
	_ nodescheduler.Task = (*transformMaterializeTask)(nil)
)
