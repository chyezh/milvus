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

package rootcoord

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/log"
	pb "github.com/milvus-io/milvus/pkg/proto/etcdpb"
)

type dropPartitionTask struct {
	baseTask
	Req      *milvuspb.DropPartitionRequest
	collMeta *model.Collection
}

func (t *dropPartitionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_DropPartition); err != nil {
		return err
	}
	if t.Req.GetPartitionName() == Params.CommonCfg.DefaultPartitionName.GetValue() {
		return fmt.Errorf("default partition cannot be deleted")
	}
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.GetTs())
	if err != nil {
		// Is this idempotent?
		return err
	}
	t.collMeta = collMeta
	return nil
}

func (t *dropPartitionTask) Execute(ctx context.Context) error {
	var partition *model.Partition
	for _, p := range t.collMeta.Partitions {
		if p.PartitionName == t.Req.GetPartitionName() {
			partition = p
			break
		}
	}
	if partition == nil || partition.State == pb.PartitionState_PartitionDropping || partition.State == pb.PartitionState_PartitionDropped {
		log.Ctx(ctx).Warn("drop an non-existent or dropping/dropped partition", zap.String("collection", t.Req.GetCollectionName()), zap.String("partition", t.Req.GetPartitionName()))
		// make dropping partition idempotent.
		return nil
	}

	return executeDropPartitionTaskSteps(ctx, t.core,
		t.Req.GetPartitionName(), partID,
		t.collMeta, t.Req.GetDbName(),
		t.Req.GetBase().GetReplicateInfo().GetIsReplicate(), t.GetTs())
}

func (t *dropPartitionTask) GetLockerKey() LockerKey {
	collection := t.core.getCollectionIDStr(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), 0)
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

func executeDropPartitionTaskSteps(ctx context.Context,
	core *Core,
	partitionName string,
	partitionID UniqueID,
	col *model.Collection,
	dbName string,
	isReplicate bool,
	ts Timestamp,
) error {
	redoTask := newBaseRedoTask(core.stepExecutor)
	if !isRecover {
		// if the task is recoverred from meta, the state has been changed to dropping, so skip it.
		redoTask.AddSyncStep(&changePartitionStateStep{
			baseStep:     baseStep{core: core},
			collectionID: collMeta.CollectionID,
			partitionID:  partition.PartitionID,
			state:        pb.PartitionState_PartitionDropping,
			ts:           ts,
		})
	}

	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: core},
		dbName:          dbName,
		collectionNames: []string{col.Name},
		collectionID:    col.CollectionID,
		partitionName:   partitionName,
		ts:              ts,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_DropPartition)},
	})
	redoTask.AddAsyncStep(&deletePartitionDataStep{
		baseStep: baseStep{core: core},
		pchans:   col.PhysicalChannelNames,
		vchans:   col.VirtualChannelNames,
		partition: &model.Partition{
			PartitionID:   partitionID,
			PartitionName: partitionName,
			CollectionID:  col.CollectionID,
		},
		isSkip: isReplicate,
	})
	redoTask.AddAsyncStep(newDropPartitionAtDataCoordStep(core, col.CollectionID, partition.PartitionID))
	redoTask.AddAsyncStep(newConfirmGCStep(core, col.CollectionID, partitionID))
	redoTask.AddAsyncStep(&removePartitionMetaStep{
		baseStep:     baseStep{core: core},
		dbID:         col.DBID,
		collectionID: col.CollectionID,
		partitionID:  partitionID,
		// This ts is less than the ts when we notify data nodes to drop partition, but it's OK since we have already
		// marked this partition as deleted. If we want to make this ts greater than the notification's ts, we should
		// wrap a step who will have these children and connect them with ts.
		ts: ts,
	})

	return redoTask.Execute(ctx)
}
