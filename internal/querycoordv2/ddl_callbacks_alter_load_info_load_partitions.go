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

package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (s *Server) broadcastAlterLoadConfigCollectionV2ForLoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) error {
	broadcaster, err := s.startBroadcastWithCollectionIDLock(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// double check if the collection is already dropped
	coll, err := s.broker.DescribeCollection(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}

	replicaNumber, resourceGroups, userSpecifiedReplicaMode, err := s.getLoadReplicaConfigForRequest(
		ctx,
		req.GetReplicaNumber(),
		req.GetResourceGroups(),
		req.GetCollectionID(),
	)
	if err != nil {
		return err
	}

	expectedReplicasNumber, err := utils.AssignReplica(ctx, s.meta, resourceGroups, replicaNumber, true)
	if err != nil {
		return err
	}

	currentLoadConfig := s.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[req.GetCollectionID()]
	if err := checkQViewsLoadPartitionsReplicaNumber(currentLoadConfig, expectedReplicasNumber); err != nil {
		return err
	}
	partitionIDsSet := typeutil.NewSet[int64]()
	if currentLoadConfig != nil {
		partitionIDsSet.Insert(currentLoadConfig.PartitionIDs...)
	}
	// add new incoming partitionIDs.
	for _, partition := range req.PartitionIDs {
		partitionIDsSet.Insert(partition)
	}
	msg, err := s.generateAlterLoadConfigMessageForLoadCollection(ctx, coll, currentLoadConfig, qviewsExpectedLoadConfig{
		PartitionIDs:             partitionIDsSet.Collect(),
		LoadType:                 querypb.LoadType_LoadPartition,
		ReplicaNumber:            expectedReplicasNumber,
		FieldIndexID:             req.GetFieldIndexID(),
		LoadFields:               req.GetLoadFields(),
		Priority:                 req.GetPriority(),
		UserSpecifiedReplicaMode: userSpecifiedReplicaMode,
	})
	if err != nil {
		return err
	}
	if msg == nil {
		// load config unchanged, the partitions are already loaded as requested.
		mlog.Info(ctx, "load partitions ignored, load config is unchanged",
			mlog.Int64("collectionID", req.GetCollectionID()),
			mlog.Int64s("partitionIDs", req.GetPartitionIDs()))
		return nil
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func checkQViewsLoadPartitionsReplicaNumber(current *loadmgr.LoadConfig, expected map[string]int) error {
	if current == nil {
		return nil
	}
	expectedReplicaNumber := 0
	for _, num := range expected {
		expectedReplicaNumber += num
	}
	if len(current.Replicas) != expectedReplicaNumber {
		return merr.WrapErrParameterInvalid(len(current.Replicas), expectedReplicaNumber, "can't change the replica number for loaded partitions")
	}
	return nil
}
