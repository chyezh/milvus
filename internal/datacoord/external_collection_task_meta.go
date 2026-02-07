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

package datacoord

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type externalCollectionTaskMeta struct {
	ctx     context.Context
	catalog metastore.DataCoordCatalog

	keyLock *lock.KeyLock[UniqueID]
	// taskID -> UpdateExternalCollectionTask
	tasks *typeutil.ConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask]

	// collectionID -> UpdateExternalCollectionTask
	collectionID2Tasks *typeutil.ConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask]
}

func newExternalCollectionTaskMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*externalCollectionTaskMeta, error) {
	ectm := &externalCollectionTaskMeta{
		ctx:                ctx,
		catalog:            catalog,
		keyLock:            lock.NewKeyLock[UniqueID](),
		tasks:              typeutil.NewConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask](),
		collectionID2Tasks: typeutil.NewConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask](),
	}
	if err := ectm.reloadFromKV(); err != nil {
		return nil, err
	}
	return ectm, nil
}

func (ectm *externalCollectionTaskMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("externalCollectionTaskMeta-reloadFromKV")
	tasks, err := ectm.catalog.ListUpdateExternalCollectionTasks(ectm.ctx)
	if err != nil {
		mlog.Error(context.TODO(), "externalCollectionTaskMeta reloadFromKV load tasks failed", mlog.Err(err))
		return err
	}
	for _, t := range tasks {
		ectm.tasks.Insert(t.GetTaskID(), t)
		ectm.collectionID2Tasks.Insert(t.GetCollectionID(), t)
	}

	mlog.Info(context.TODO(), "externalCollectionTaskMeta reloadFromKV done", mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (ectm *externalCollectionTaskMeta) AddTask(t *indexpb.UpdateExternalCollectionTask) error {
	// Lock on collectionID to prevent concurrent tasks for the same collection
	ectm.keyLock.Lock(t.GetCollectionID())
	defer ectm.keyLock.Unlock(t.GetCollectionID())

	mlog.Info(ectm.ctx, "add update external collection task",
		mlog.Int64("taskID", t.GetTaskID()),
		mlog.Int64("collectionID", t.GetCollectionID()))

	// Check if a task already exists for this collection
	if existingTask, ok := ectm.collectionID2Tasks.Get(t.GetCollectionID()); ok {
		mlog.Warn(context.TODO(), "update external collection task already exists for collection",
			mlog.Int64("existingTaskID", existingTask.GetTaskID()),
			mlog.Int64("newTaskID", t.GetTaskID()),
			mlog.Int64("collectionID", t.GetCollectionID()))
		return merr.WrapErrTaskDuplicate(strconv.FormatInt(t.GetCollectionID(), 10))
	}

	if err := ectm.catalog.SaveUpdateExternalCollectionTask(ectm.ctx, t); err != nil {
		mlog.Warn(context.TODO(), "save update external collection task failed",
			mlog.Int64("taskID", t.GetTaskID()),
			mlog.Int64("collectionID", t.GetCollectionID()),
			mlog.Err(err))
		return err
	}

	ectm.tasks.Insert(t.GetTaskID(), t)
	ectm.collectionID2Tasks.Insert(t.GetCollectionID(), t)

	mlog.Info(context.TODO(), "add update external collection task success",
		mlog.Int64("taskID", t.GetTaskID()),
		mlog.Int64("collectionID", t.GetCollectionID()))
	return nil
}

func (ectm *externalCollectionTaskMeta) DropTask(ctx context.Context, taskID int64) error {
	// First get the task to find its collectionID
	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		mlog.Info(ctx, "remove update external collection task success, task already not exist", mlog.Int64("taskID", taskID))
		return nil
	}

	// Lock on collectionID to serialize with AddTask operations
	ectm.keyLock.Lock(t.GetCollectionID())
	defer ectm.keyLock.Unlock(t.GetCollectionID())

	mlog.Info(ctx, "drop update external collection task by taskID",
		mlog.Int64("taskID", taskID),
		mlog.Int64("collectionID", t.GetCollectionID()))

	// Double-check task still exists after acquiring lock
	t, ok = ectm.tasks.Get(taskID)
	if !ok {
		mlog.Info(ctx, "remove update external collection task success, task already not exist", mlog.Int64("taskID", taskID))
		return nil
	}

	if err := ectm.catalog.DropUpdateExternalCollectionTask(ctx, taskID); err != nil {
		mlog.Warn(ctx, "drop update external collection task failed",
			mlog.Int64("taskID", taskID),
			mlog.Int64("collectionID", t.GetCollectionID()),
			mlog.Err(err))
		return err
	}

	ectm.tasks.Remove(taskID)
	ectm.collectionID2Tasks.Remove(t.GetCollectionID())

	mlog.Info(ctx, "remove update external collection task success",
		mlog.Int64("taskID", taskID),
		mlog.Int64("collectionID", t.GetCollectionID()))
	return nil
}

func (ectm *externalCollectionTaskMeta) UpdateVersion(taskID, nodeID int64) error {
	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	// Lock on collectionID for consistency with Add/Drop operations
	ectm.keyLock.Lock(t.GetCollectionID())
	defer ectm.keyLock.Unlock(t.GetCollectionID())

	// Double-check task still exists after acquiring lock
	t, ok = ectm.tasks.Get(taskID)
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.UpdateExternalCollectionTask)
	cloneT.Version++
	cloneT.NodeID = nodeID

	if err := ectm.catalog.SaveUpdateExternalCollectionTask(ectm.ctx, cloneT); err != nil {
		mlog.Warn(context.TODO(), "update external collection task version failed",
			mlog.Int64("taskID", t.GetTaskID()),
			mlog.Int64("collectionID", t.GetCollectionID()),
			mlog.Int64("nodeID", nodeID),
			mlog.Err(err))
		return err
	}

	ectm.tasks.Insert(taskID, cloneT)
	ectm.collectionID2Tasks.Insert(t.GetCollectionID(), cloneT)
	mlog.Info(context.TODO(), "update external collection task version success", mlog.Int64("taskID", taskID), mlog.Int64("nodeID", nodeID),
		mlog.Int64("newVersion", cloneT.GetVersion()))
	return nil
}

func (ectm *externalCollectionTaskMeta) UpdateTaskState(taskID int64, state indexpb.JobState, failReason string) error {
	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	// Lock on collectionID for consistency with Add/Drop operations
	ectm.keyLock.Lock(t.GetCollectionID())
	defer ectm.keyLock.Unlock(t.GetCollectionID())

	// Double-check task still exists after acquiring lock
	t, ok = ectm.tasks.Get(taskID)
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.UpdateExternalCollectionTask)
	cloneT.State = state
	cloneT.FailReason = failReason

	if err := ectm.catalog.SaveUpdateExternalCollectionTask(ectm.ctx, cloneT); err != nil {
		mlog.Warn(context.TODO(), "update external collection task state failed",
			mlog.Int64("taskID", t.GetTaskID()),
			mlog.Err(err))
		return err
	}

	ectm.tasks.Insert(taskID, cloneT)
	ectm.collectionID2Tasks.Insert(t.GetCollectionID(), cloneT)

	return nil
}

func (ectm *externalCollectionTaskMeta) GetTask(taskID int64) *indexpb.UpdateExternalCollectionTask {
	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		return nil
	}
	return proto.Clone(t).(*indexpb.UpdateExternalCollectionTask)
}

func (ectm *externalCollectionTaskMeta) GetTaskState(taskID int64) indexpb.JobState {
	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		return indexpb.JobState_JobStateNone
	}
	return t.State
}

func (ectm *externalCollectionTaskMeta) GetTaskByCollectionID(collectionID int64) *indexpb.UpdateExternalCollectionTask {
	t, ok := ectm.collectionID2Tasks.Get(collectionID)
	if !ok {
		return nil
	}
	return proto.Clone(t).(*indexpb.UpdateExternalCollectionTask)
}

func (ectm *externalCollectionTaskMeta) GetAllTasks() map[int64]*indexpb.UpdateExternalCollectionTask {
	tasks := make(map[int64]*indexpb.UpdateExternalCollectionTask)
	ectm.tasks.Range(func(taskID int64, task *indexpb.UpdateExternalCollectionTask) bool {
		tasks[taskID] = proto.Clone(task).(*indexpb.UpdateExternalCollectionTask)
		return true
	})
	return tasks
}
