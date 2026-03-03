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

package datanode

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/index"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// CreateJob is CreateIndex
func (node *DataNode) CreateJob(ctx context.Context, req *workerpb.CreateJobRequest) (*commonpb.Status, error) {

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn(context.TODO(), "index node not ready",
			log.Err(err),
		)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()
	log.Info(context.TODO(), "DataNode building index ...",
		log.Int64("collectionID", req.GetCollectionID()),
		log.Int64("partitionID", req.GetPartitionID()),
		log.Int64("segmentID", req.GetSegmentID()),
		log.Int64("indexID", req.GetIndexID()),
		log.String("indexName", req.GetIndexName()),
		log.String("indexFilePrefix", req.GetIndexFilePrefix()),
		log.Int64("indexVersion", req.GetIndexVersion()),
		log.Strings("dataPaths", req.GetDataPaths()),
		log.Any("typeParams", req.GetTypeParams()),
		log.Any("indexParams", req.GetIndexParams()),
		log.Int64("numRows", req.GetNumRows()),
		log.Int32("current_index_version", req.GetCurrentIndexVersion()),
		log.Any("storepath", req.GetStorePath()),
		log.Any("storeversion", req.GetStoreVersion()),
		log.Any("indexstorepath", req.GetIndexStorePath()),
		log.Any("dim", req.GetDim()),
	)
	ctx, sp := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "DataNode-CreateIndex", trace.WithAttributes(
		attribute.Int64("indexBuildID", req.GetBuildID()),
		attribute.String("clusterID", req.GetClusterID()),
	))
	defer sp.End()
	metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.TotalLabel).Inc()

	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreIndexTask(req.GetClusterID(), req.GetBuildID(), &index.IndexTaskInfo{
		Cancel: taskCancel,
		State:  commonpb.IndexState_InProgress,
	}); oldInfo != nil {
		err := merr.WrapErrIndexDuplicate(req.GetIndexName(), "building index task existed")
		log.Warn(context.TODO(), "duplicated index build task", log.Err(err))
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		log.Error(context.TODO(), "create chunk manager failed", log.String("bucket", req.GetStorageConfig().GetBucketName()),
			log.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			log.Err(err),
		)
		node.taskManager.DeleteIndexTaskInfos(ctx, []index.Key{{ClusterID: req.GetClusterID(), TaskID: req.GetBuildID()}})
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), req.GetCollectionID())
	if err != nil {
		return merr.Status(err), nil
	}
	task := index.NewIndexBuildTask(taskCtx, taskCancel, req, cm, node.taskManager, pluginContext)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(task); err != nil {
		log.Warn(context.TODO(), "DataNode failed to schedule",
			log.Err(err))
		ret = merr.Status(err)
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel).Inc()
	log.Info(context.TODO(), "DataNode successfully scheduled",
		log.String("indexName", req.GetIndexName()))
	return ret, nil
}

func (node *DataNode) QueryJobs(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn(context.TODO(), "index node not ready", log.Err(err))
		return &workerpb.QueryJobsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()
	infos := make(map[typeutil.UniqueID]*index.IndexTaskInfo)
	node.taskManager.ForeachIndexTaskInfo(func(ClusterID string, buildID typeutil.UniqueID, info *index.IndexTaskInfo) {
		if ClusterID == req.GetClusterID() {
			infos[buildID] = info.Clone()
		}
	})
	ret := &workerpb.QueryJobsResponse{
		Status:     merr.Success(),
		ClusterID:  req.GetClusterID(),
		IndexInfos: make([]*workerpb.IndexTaskInfo, 0, len(req.GetTaskIDs())),
	}
	for i, buildID := range req.GetTaskIDs() {
		ret.IndexInfos = append(ret.IndexInfos, &workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_IndexStateNone,
			IndexFileKeys:  nil,
			SerializedSize: 0,
		})
		if info, ok := infos[buildID]; ok {
			ret.IndexInfos[i].State = info.State
			ret.IndexInfos[i].IndexFileKeys = info.FileKeys
			ret.IndexInfos[i].SerializedSize = info.SerializedSize
			ret.IndexInfos[i].MemSize = info.MemSize
			ret.IndexInfos[i].FailReason = info.FailReason
			ret.IndexInfos[i].CurrentIndexVersion = info.CurrentIndexVersion
			ret.IndexInfos[i].CurrentScalarIndexVersion = info.CurrentScalarIndexVersion
			log.RatedDebug(context.TODO(), log.RateDefault, "querying index build task",
				log.Int64("indexBuildID", buildID),
				log.String("state", info.State.String()),
				log.String("reason", info.FailReason),
			)
		}
	}
	return ret, nil
}

func (node *DataNode) DropJobs(ctx context.Context, req *workerpb.DropJobsRequest) (*commonpb.Status, error) {
	log.Info(ctx, "drop index build jobs",
		log.String("clusterID", req.ClusterID),
		log.Int64s("indexBuildIDs", req.GetTaskIDs()),
	)
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn(ctx, "index node not ready", log.Err(err), log.String("clusterID", req.ClusterID))
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()
	keys := make([]index.Key, 0, len(req.GetTaskIDs()))
	for _, taskID := range req.GetTaskIDs() {
		keys = append(keys, index.Key{ClusterID: req.GetClusterID(), TaskID: taskID})
	}
	infos := node.taskManager.DeleteIndexTaskInfos(ctx, keys)
	for _, info := range infos {
		if info.Cancel != nil {
			info.Cancel()
		}
	}
	log.Info(ctx, "drop index build jobs success", log.String("clusterID", req.GetClusterID()),
		log.Int64s("indexBuildIDs", req.GetTaskIDs()))
	return merr.Success(), nil
}

// GetJobStats should be GetSlots
func (node *DataNode) GetJobStats(ctx context.Context, req *workerpb.GetJobStatsRequest) (*workerpb.GetJobStatsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn(ctx, "index node not ready", log.Err(err))
		return &workerpb.GetJobStatsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	var (
		totalSlots     = index.CalculateNodeSlots()
		indexStatsUsed = node.taskScheduler.TaskQueue.GetUsingSlot()
		compactionUsed = node.compactionExecutor.Slots()
		importUsed     = node.importScheduler.Slots()
	)

	availableSlots := totalSlots - indexStatsUsed - compactionUsed - importUsed
	if availableSlots < 0 {
		availableSlots = 0
	}

	log.Info(ctx, "query slots done",
		log.Int64("totalSlots", totalSlots),
		log.Int64("availableSlots", availableSlots),
		log.Int64("indexStatsUsed", indexStatsUsed),
		log.Int64("compactionUsed", compactionUsed),
		log.Int64("importUsed", importUsed),
	)

	return &workerpb.GetJobStatsResponse{
		Status:         merr.Success(),
		TotalSlots:     totalSlots,
		AvailableSlots: availableSlots,
	}, nil
}

// Deprecated: use CreateTask instead, keep for compatibility
func (node *DataNode) CreateJobV2(ctx context.Context, req *workerpb.CreateJobV2Request) (*commonpb.Status, error) {

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn(context.TODO(), "index node not ready",
			log.Err(err),
		)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	log.Info(context.TODO(), "DataNode receive CreateJob request...")

	switch req.GetJobType() {
	case indexpb.JobType_JobTypeIndexJob:
		indexRequest := req.GetIndexRequest()
		return node.createIndexTask(ctx, indexRequest)
	case indexpb.JobType_JobTypeAnalyzeJob:
		analyzeRequest := req.GetAnalyzeRequest()
		return node.createAnalyzeTask(ctx, analyzeRequest)
	case indexpb.JobType_JobTypeStatsJob:
		statsRequest := req.GetStatsRequest()
		return node.createStatsTask(ctx, statsRequest)
	default:
		log.Warn(context.TODO(), "DataNode receive unknown type job")
		return merr.Status(fmt.Errorf("DataNode receive unknown type job with TaskID: %d", req.GetTaskID())), nil
	}
}

func (node *DataNode) createIndexTask(ctx context.Context, req *workerpb.CreateJobRequest) (*commonpb.Status, error) {
	log.Info(ctx, "DataNode building index ...",
		log.String("clusterID", req.GetClusterID()),
		log.Int64("taskID", req.GetBuildID()),
		log.Int64("collectionID", req.GetCollectionID()),
		log.Int64("partitionID", req.GetPartitionID()),
		log.Int64("segmentID", req.GetSegmentID()),
		log.String("indexFilePrefix", req.GetIndexFilePrefix()),
		log.Int64("indexVersion", req.GetIndexVersion()),
		log.Strings("dataPaths", req.GetDataPaths()),
		log.Any("typeParams", req.GetTypeParams()),
		log.Any("indexParams", req.GetIndexParams()),
		log.Int64("numRows", req.GetNumRows()),
		log.Int32("current_index_version", req.GetCurrentIndexVersion()),
		log.String("storePath", req.GetStorePath()),
		log.Int64("storeVersion", req.GetStoreVersion()),
		log.String("indexStorePath", req.GetIndexStorePath()),
		log.Int64("dim", req.GetDim()),
		log.Int64("fieldID", req.GetFieldID()),
		log.String("fieldType", req.GetFieldType().String()),
		log.Any("field", req.GetField()),
		log.Int64("taskSlot", req.GetTaskSlot()),
		log.Int64("lackBinlogRows", req.GetLackBinlogRows()),
	)
	if req.GetTaskSlot() <= 0 {
		log.Warn(ctx, "receive index task with invalid slot, set to 64", log.Int64("taskSlot", req.GetTaskSlot()))
		req.TaskSlot = 64
	}
	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreIndexTask(req.GetClusterID(), req.GetBuildID(), &index.IndexTaskInfo{
		Cancel: taskCancel,
		State:  commonpb.IndexState_InProgress,
	}); oldInfo != nil {
		err := merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeIndexJob.String(),
			fmt.Sprintf("building index task existed with %s-%d", req.GetClusterID(), req.GetBuildID()))
		log.Warn(context.TODO(), "duplicated index build task", log.Err(err))
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		log.Error(context.TODO(), "create chunk manager failed", log.String("bucket", req.GetStorageConfig().GetBucketName()),
			log.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			log.Err(err),
		)
		node.taskManager.DeleteIndexTaskInfos(ctx, []index.Key{{ClusterID: req.GetClusterID(), TaskID: req.GetBuildID()}})
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), req.GetCollectionID())
	if err != nil {
		return merr.Status(err), nil
	}

	task := index.NewIndexBuildTask(taskCtx, taskCancel, req, cm, node.taskManager, pluginContext)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(task); err != nil {
		log.Warn(context.TODO(), "DataNode failed to schedule",
			log.Err(err))
		ret = merr.Status(err)
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel).Inc()
	log.Info(context.TODO(), "DataNode index job enqueued successfully",
		log.String("indexName", req.GetIndexName()))
	return ret, nil
}

func (node *DataNode) createAnalyzeTask(ctx context.Context, req *workerpb.AnalyzeRequest) (*commonpb.Status, error) {
	log.Info(ctx, "receive analyze job",
		log.String("clusterID", req.GetClusterID()),
		log.Int64("taskID", req.GetTaskID()),
		log.Int64("collectionID", req.GetCollectionID()),
		log.Int64("partitionID", req.GetPartitionID()),
		log.Int64("fieldID", req.GetFieldID()),
		log.String("fieldName", req.GetFieldName()),
		log.String("dataType", req.GetFieldType().String()),
		log.Int64("version", req.GetVersion()),
		log.Int64("dim", req.GetDim()),
		log.Float64("trainSizeRatio", req.GetMaxTrainSizeRatio()),
		log.Int64("numClusters", req.GetNumClusters()),
		log.Int64("taskSlot", req.GetTaskSlot()),
	)

	if req.GetTaskSlot() <= 0 {
		log.Warn(ctx, "receive analyze task with invalid slot, set to 65535", log.Int64("taskSlot", req.GetTaskSlot()))
		req.TaskSlot = 65535
	}

	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreAnalyzeTask(req.GetClusterID(), req.GetTaskID(), &index.AnalyzeTaskInfo{
		Cancel: taskCancel,
		State:  indexpb.JobState_JobStateInProgress,
	}); oldInfo != nil {
		err := merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeAnalyzeJob.String(),
			fmt.Sprintf("analyze task already existed with %s-%d", req.GetClusterID(), req.GetTaskID()))
		log.Warn(context.TODO(), "duplicated analyze task", log.Err(err))
		return merr.Status(err), nil
	}
	t := index.NewAnalyzeTask(taskCtx, taskCancel, req, node.taskManager)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(t); err != nil {
		log.Warn(context.TODO(), "DataNode failed to schedule", log.Err(err))
		ret = merr.Status(err)
		return ret, nil
	}
	log.Info(context.TODO(), "DataNode analyze job enqueued successfully")
	return ret, nil
}

func (node *DataNode) createStatsTask(ctx context.Context, req *workerpb.CreateStatsRequest) (*commonpb.Status, error) {
	log.Info(ctx, "receive stats job",
		log.String("clusterID", req.GetClusterID()),
		log.Int64("taskID", req.GetTaskID()),
		log.Int64("collectionID", req.GetCollectionID()),
		log.Int64("partitionID", req.GetPartitionID()),
		log.Int64("segmentID", req.GetSegmentID()),
		log.Int64("numRows", req.GetNumRows()),
		log.Int64("targetSegmentID", req.GetTargetSegmentID()),
		log.String("subJobType", req.GetSubJobType().String()),
		log.Int64("startLogID", req.GetStartLogID()),
		log.Int64("endLogID", req.GetEndLogID()),
		log.Int64("taskSlot", req.GetTaskSlot()),
	)

	if req.GetTaskSlot() <= 0 {
		log.Warn(ctx, "receive stats task with invalid slot, set to 64", log.Int64("taskSlot", req.GetTaskSlot()))
		req.TaskSlot = 64
	}

	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreStatsTask(req.GetClusterID(), req.GetTaskID(), &index.StatsTaskInfo{
		Cancel: taskCancel,
		State:  indexpb.JobState_JobStateInProgress,
	}); oldInfo != nil {
		err := merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeStatsJob.String(),
			fmt.Sprintf("stats task already existed with %s-%d", req.GetClusterID(), req.GetTaskID()))
		log.Warn(context.TODO(), "duplicated stats task", log.Err(err))
		return merr.Status(err), nil
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		log.Error(context.TODO(), "create chunk manager failed", log.String("bucket", req.GetStorageConfig().GetBucketName()),
			log.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			log.Err(err),
		)
		node.taskManager.DeleteStatsTaskInfos(ctx, []index.Key{{ClusterID: req.GetClusterID(), TaskID: req.GetTaskID()}})
		return merr.Status(err), nil
	}

	t := index.NewStatsTask(taskCtx, taskCancel, req, node.taskManager, cm)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(t); err != nil {
		log.Warn(context.TODO(), "DataNode failed to schedule", log.Err(err))
		ret = merr.Status(err)
		return ret, nil
	}
	log.Info(context.TODO(), "DataNode stats job enqueued successfully")
	return ret, nil
}

// Deprecated: use QueryTask instead, keep for compatibility
func (node *DataNode) QueryJobsV2(ctx context.Context, req *workerpb.QueryJobsV2Request) (*workerpb.QueryJobsV2Response, error) {

	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn(context.TODO(), "DataNode not ready", log.Err(err))
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	switch req.GetJobType() {
	case indexpb.JobType_JobTypeIndexJob:
		return node.queryIndexTask(ctx, &workerpb.QueryJobsRequest{
			ClusterID: req.GetClusterID(),
			TaskIDs:   req.GetTaskIDs(),
		})
	case indexpb.JobType_JobTypeAnalyzeJob:
		return node.queryAnalyzeTask(ctx, &workerpb.QueryJobsRequest{
			ClusterID: req.GetClusterID(),
			TaskIDs:   req.GetTaskIDs(),
		})
	case indexpb.JobType_JobTypeStatsJob:
		return node.queryStatsTask(ctx, &workerpb.QueryJobsRequest{
			ClusterID: req.GetClusterID(),
			TaskIDs:   req.GetTaskIDs(),
		})
	default:
		log.Warn(context.TODO(), "DataNode receive querying unknown type jobs")
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(errors.New("DataNode receive querying unknown type jobs")),
		}, nil
	}
}

func (node *DataNode) queryIndexTask(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsV2Response, error) {

	infos := make(map[typeutil.UniqueID]*index.IndexTaskInfo)
	node.taskManager.ForeachIndexTaskInfo(func(ClusterID string, buildID typeutil.UniqueID, info *index.IndexTaskInfo) {
		if ClusterID == req.GetClusterID() {
			infos[buildID] = info.Clone()
		}
	})
	results := make([]*workerpb.IndexTaskInfo, 0, len(req.GetTaskIDs()))
	for _, buildID := range req.GetTaskIDs() {
		if info, ok := infos[buildID]; ok {
			results = append(results, info.ToIndexTaskInfo(buildID))
		}
	}
	log.Debug(context.TODO(), "query index jobs result success", log.Any("results", results))
	if len(results) == 0 {
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(fmt.Errorf("tasks '%v' not found", req.GetTaskIDs())),
		}, nil
	}
	return &workerpb.QueryJobsV2Response{
		Status:    merr.Success(),
		ClusterID: req.GetClusterID(),
		Result: &workerpb.QueryJobsV2Response_IndexJobResults{
			IndexJobResults: &workerpb.IndexJobResults{
				Results: results,
			},
		},
	}, nil
}

func (node *DataNode) queryStatsTask(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsV2Response, error) {

	results := make([]*workerpb.StatsResult, 0, len(req.GetTaskIDs()))
	for _, taskID := range req.GetTaskIDs() {
		info := node.taskManager.GetStatsTaskInfo(req.GetClusterID(), taskID)
		if info != nil {
			results = append(results, info.ToStatsResult(taskID))
		}
	}
	log.Debug(context.TODO(), "query stats job result success", log.Any("results", results))
	if len(results) == 0 {
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(fmt.Errorf("tasks '%v' not found", req.GetTaskIDs())),
		}, nil
	}
	return &workerpb.QueryJobsV2Response{
		Status:    merr.Success(),
		ClusterID: req.GetClusterID(),
		Result: &workerpb.QueryJobsV2Response_StatsJobResults{
			StatsJobResults: &workerpb.StatsResults{
				Results: results,
			},
		},
	}, nil
}

func (node *DataNode) queryAnalyzeTask(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsV2Response, error) {

	results := make([]*workerpb.AnalyzeResult, 0, len(req.GetTaskIDs()))
	for _, taskID := range req.GetTaskIDs() {
		info := node.taskManager.GetAnalyzeTaskInfo(req.GetClusterID(), taskID)
		if info != nil {
			results = append(results, &workerpb.AnalyzeResult{
				TaskID:        taskID,
				State:         info.State,
				FailReason:    info.FailReason,
				CentroidsFile: info.CentroidsFile,
			})
		}
	}
	log.Debug(context.TODO(), "query analyze jobs result success", log.Any("results", results))
	if len(results) == 0 {
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(fmt.Errorf("tasks '%v' not found", req.GetTaskIDs())),
		}, nil
	}
	return &workerpb.QueryJobsV2Response{
		Status:    merr.Success(),
		ClusterID: req.GetClusterID(),
		Result: &workerpb.QueryJobsV2Response_AnalyzeJobResults{
			AnalyzeJobResults: &workerpb.AnalyzeResults{
				Results: results,
			},
		},
	}, nil
}

// Deprecated: use DropTask instead, keep for compatibility
func (node *DataNode) DropJobsV2(ctx context.Context, req *workerpb.DropJobsV2Request) (*commonpb.Status, error) {

	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn(context.TODO(), "DataNode not ready", log.Err(err))
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	log.Info(context.TODO(), "DataNode receive DropJobs request")

	switch req.GetJobType() {
	case indexpb.JobType_JobTypeIndexJob:
		keys := make([]index.Key, 0, len(req.GetTaskIDs()))
		for _, buildID := range req.GetTaskIDs() {
			keys = append(keys, index.Key{ClusterID: req.GetClusterID(), TaskID: buildID})
		}
		infos := node.taskManager.DeleteIndexTaskInfos(ctx, keys)
		for _, info := range infos {
			if info.Cancel != nil {
				info.Cancel()
			}
		}
		log.Info(context.TODO(), "drop index build jobs success")
		return merr.Success(), nil
	case indexpb.JobType_JobTypeAnalyzeJob:
		keys := make([]index.Key, 0, len(req.GetTaskIDs()))
		for _, taskID := range req.GetTaskIDs() {
			keys = append(keys, index.Key{ClusterID: req.GetClusterID(), TaskID: taskID})
		}
		infos := node.taskManager.DeleteAnalyzeTaskInfos(ctx, keys)
		for _, info := range infos {
			if info.Cancel != nil {
				info.Cancel()
			}
		}
		log.Info(context.TODO(), "drop analyze jobs success")
		return merr.Success(), nil
	case indexpb.JobType_JobTypeStatsJob:
		keys := make([]index.Key, 0, len(req.GetTaskIDs()))
		for _, taskID := range req.GetTaskIDs() {
			keys = append(keys, index.Key{ClusterID: req.GetClusterID(), TaskID: taskID})
		}
		infos := node.taskManager.DeleteStatsTaskInfos(ctx, keys)
		for _, info := range infos {
			if info.Cancel != nil {
				info.Cancel()
			}
		}
		log.Info(context.TODO(), "drop stats jobs success")
		return merr.Success(), nil
	default:
		log.Warn(context.TODO(), "DataNode receive dropping unknown type jobs")
		return merr.Status(errors.New("DataNode receive dropping unknown type jobs")), nil
	}
}
