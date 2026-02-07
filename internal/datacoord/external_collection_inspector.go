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
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type ExternalCollectionInspector interface {
	Start()
	Stop()
	SubmitUpdateTask(collectionID int64) error
}

var _ ExternalCollectionInspector = (*externalCollectionInspector)(nil)

type externalCollectionInspector struct {
	ctx    context.Context
	cancel context.CancelFunc

	loopWg sync.WaitGroup

	mt *meta

	scheduler task.GlobalScheduler
	allocator allocator.Allocator
}

func newExternalCollectionInspector(ctx context.Context,
	mt *meta,
	scheduler task.GlobalScheduler,
	allocator allocator.Allocator,
) *externalCollectionInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &externalCollectionInspector{
		ctx:       ctx,
		cancel:    cancel,
		loopWg:    sync.WaitGroup{},
		mt:        mt,
		scheduler: scheduler,
		allocator: allocator,
	}
}

func (i *externalCollectionInspector) Start() {
	i.reloadFromMeta()
	i.loopWg.Add(1)
	go i.triggerUpdateTaskLoop()
}

func (i *externalCollectionInspector) Stop() {
	i.cancel()
	i.loopWg.Wait()
}

func (i *externalCollectionInspector) reloadFromMeta() {
	tasks := i.mt.externalCollectionTaskMeta.GetAllTasks()
	for _, t := range tasks {
		if t.GetState() != indexpb.JobState_JobStateInit &&
			t.GetState() != indexpb.JobState_JobStateRetry &&
			t.GetState() != indexpb.JobState_JobStateInProgress {
			continue
		}
		// Enqueue active tasks for processing
		updateTask := newUpdateExternalCollectionTask(t, i.mt, i.allocator)
		i.scheduler.Enqueue(updateTask)
	}
}

func (i *externalCollectionInspector) triggerUpdateTaskLoop() {
	mlog.Info(context.TODO(), "start external collection inspector loop...")
	defer i.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-i.ctx.Done():
			mlog.Warn(context.TODO(), "DataCoord context done, exit external collection inspector loop...")
			return
		case <-ticker.C:
			i.triggerUpdateTasks()
		}
	}
}

func (i *externalCollectionInspector) triggerUpdateTasks() {
	collections := i.mt.GetCollections()

	for _, collection := range collections {
		if !i.isExternalCollection(collection) {
			continue
		}

		// Check if we should trigger a task based on source changes
		if shouldTrigger := i.shouldTriggerTask(collection); shouldTrigger {
			if err := i.SubmitUpdateTask(collection.ID); err != nil {
				mlog.Warn(context.TODO(), "failed to submit update task for external collection",
					mlog.Int64("collectionID", collection.ID),
					mlog.Err(err))
			}
		}
	}
}

func (i *externalCollectionInspector) isExternalCollection(collection *collectionInfo) bool {
	if collection.Schema == nil {
		return false
	}
	return collection.Schema.GetExternalSource() != ""
}

// shouldTriggerTask checks if we should create/replace a task based on external source changes
// Returns shouldTrigger
func (i *externalCollectionInspector) shouldTriggerTask(collection *collectionInfo) bool {
	externalSource := collection.Schema.GetExternalSource()
	externalSpec := collection.Schema.GetExternalSpec()

	existingTask := i.mt.externalCollectionTaskMeta.GetTaskByCollectionID(collection.ID)

	if existingTask == nil {
		// No task exists, create one
		return true
	}

	taskSource := existingTask.GetExternalSource()
	taskSpec := existingTask.GetExternalSpec()

	// Check if source or spec changed
	sourceChanged := externalSource != taskSource || externalSpec != taskSpec

	switch existingTask.GetState() {
	case indexpb.JobState_JobStateInit, indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateInProgress:
		// Task is running
		if sourceChanged {
			// Source changed while task running - abort and replace
			mlog.Info(context.TODO(), "External source changed, replacing running task",
				mlog.Int64("collectionID", collection.ID),
				mlog.Int64("taskID", existingTask.GetTaskID()),
				mlog.String("oldSource", taskSource),
				mlog.String("newSource", externalSource))

			// Abort old task from scheduler
			i.scheduler.AbortAndRemoveTask(existingTask.GetTaskID())
			// Drop from meta
			i.mt.externalCollectionTaskMeta.DropTask(context.Background(), existingTask.GetTaskID())

			return true
		}
		// Same source, task already running
		return false

	case indexpb.JobState_JobStateFinished:
		// Task completed
		if sourceChanged {
			// Source changed after completion, create new task
			mlog.Info(context.TODO(), "External source changed after completion, creating new task",
				mlog.Int64("collectionID", collection.ID),
				mlog.String("oldSource", taskSource),
				mlog.String("newSource", externalSource))

			// Drop old completed task, create new one
			i.mt.externalCollectionTaskMeta.DropTask(context.Background(), existingTask.GetTaskID())
			return true
		}
		// Up to date
		return false

	case indexpb.JobState_JobStateFailed:
		if sourceChanged {
			// Source changed after failure - worth retrying with new source
			mlog.Info(context.TODO(), "External source changed after failure, creating new task",
				mlog.Int64("collectionID", collection.ID),
				mlog.String("oldSource", taskSource),
				mlog.String("newSource", externalSource))

			i.mt.externalCollectionTaskMeta.DropTask(context.Background(), existingTask.GetTaskID())
			return true
		}
		// Same source failed - don't auto-retry
		return false

	default:
		return false
	}
}

func (i *externalCollectionInspector) SubmitUpdateTask(collectionID int64) error {
	i.ctx = mlog.WithFields(i.ctx, mlog.Int64("collectionID", collectionID))

	// Get collection info to retrieve external source and spec
	collection := i.mt.GetCollection(collectionID)
	if collection == nil {
		mlog.Warn(context.TODO(), "collection not found")
		return fmt.Errorf("collection %d not found", collectionID)
	}

	// Allocate task ID
	taskID, err := i.allocator.AllocID(context.Background())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to allocate task ID", mlog.Err(err))
		return err
	}

	// Create task
	t := &indexpb.UpdateExternalCollectionTask{
		CollectionID:   collectionID,
		TaskID:         taskID,
		Version:        0,
		NodeID:         0,
		State:          indexpb.JobState_JobStateInit,
		FailReason:     "",
		ExternalSource: collection.Schema.GetExternalSource(),
		ExternalSpec:   collection.Schema.GetExternalSpec(),
	}

	// Add task to meta
	if err = i.mt.externalCollectionTaskMeta.AddTask(t); err != nil {
		if errors.Is(err, merr.ErrTaskDuplicate) {
			mlog.Info(context.TODO(), "external collection update task already exists",
				mlog.Int64("collectionID", collectionID))
			return nil
		}
		mlog.Warn(context.TODO(), "failed to add task to meta", mlog.Err(err))
		return err
	}

	// Create and enqueue task
	updateTask := newUpdateExternalCollectionTask(t, i.mt, i.allocator)
	i.scheduler.Enqueue(updateTask)

	mlog.Info(context.TODO(), "external collection update task submitted",
		mlog.Int64("taskID", taskID),
		mlog.Int64("collectionID", collectionID),
		mlog.String("externalSource", collection.Schema.GetExternalSource()))

	return nil
}
