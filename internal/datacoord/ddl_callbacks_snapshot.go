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


	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// createSnapshotV2AckCallback handles the callback for CreateSnapshot DDL message.
// ID allocation happens inside SnapshotManager.CreateSnapshot.
func (s *DDLCallbacks) createSnapshotV2AckCallback(ctx context.Context, result message.BroadcastResultCreateSnapshotMessageV2) error {
	header := result.Message.Header()
	log.Info(context.TODO(), "createSnapshotV2AckCallback received")

	// Create snapshot - ID is allocated inside CreateSnapshot
	snapshotID, err := s.snapshotManager.CreateSnapshot(ctx, header.CollectionId, header.Name, header.Description)
	if err != nil {
		log.Error(context.TODO(), "failed to create snapshot via DDL callback", log.Err(err))
		return err
	}

	log.Info(context.TODO(), "snapshot created successfully via DDL callback", log.Int64("snapshotID", snapshotID))
	return nil
}

// dropSnapshotV2AckCallback handles the callback for DropSnapshot DDL message.
func (s *DDLCallbacks) dropSnapshotV2AckCallback(ctx context.Context, result message.BroadcastResultDropSnapshotMessageV2) error {
	header := result.Message.Header()
	log.Info(context.TODO(), "dropSnapshotV2AckCallback received")

	// Delete snapshot using SnapshotManager interface (idempotent)
	if err := s.snapshotManager.DropSnapshot(ctx, header.Name); err != nil {
		log.Error(context.TODO(), "failed to drop snapshot via DDL callback", log.Err(err))
		return err
	}

	log.Info(context.TODO(), "snapshot dropped successfully via DDL callback")
	return nil
}

// restoreSnapshotV2AckCallback handles the callback for RestoreSnapshot DDL message.
// It creates copy segment jobs for data restoration.
// NOTE: RestoreIndexes is now called synchronously in services.go before broadcast.
// NOTE: jobID is pre-allocated in RestoreSnapshot and passed via WAL message for idempotency.
func (s *DDLCallbacks) restoreSnapshotV2AckCallback(ctx context.Context, result message.BroadcastResultRestoreSnapshotMessageV2) error {
	header := result.Message.Header()
	log.Info(context.TODO(), "restoreSnapshotV2AckCallback received")

	// Restore data (create copy segment job)
	// Use the pre-allocated jobID from the WAL message for idempotency
	jobID, err := s.snapshotManager.RestoreData(ctx, header.SnapshotName, header.CollectionId, header.JobId)
	if err != nil {
		log.Error(context.TODO(), "failed to restore data", log.Err(err))
		return err
	}

	log.Info(context.TODO(), "restore snapshot callback completed, job created for async execution", log.Int64("jobID", jobID))
	return nil
}
