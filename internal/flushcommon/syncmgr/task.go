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

package syncmgr

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type SyncTask struct {
	chunkManager storage.ChunkManager
	allocator    allocator.Interface

	collectionID  int64
	partitionID   int64
	segmentID     int64
	channelName   string
	startPosition *msgpb.MsgPosition
	checkpoint    *msgpb.MsgPosition
	dataSource    string
	// batchRows is the row number of this sync task,
	// not the total num of rows of segemnt
	batchRows int64
	level     datapb.SegmentLevel

	tsFrom typeutil.Timestamp
	tsTo   typeutil.Timestamp

	metacache  metacache.MetaCache
	metaWriter MetaWriter
	schema     *schemapb.CollectionSchema // schema for when buffer created, could be different from current on in metacache

	pack *SyncPack

	insertBinlogs map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	statsBinlogs  map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	bm25Binlogs   map[int64]*datapb.FieldBinlog
	deltaBinlog   *datapb.FieldBinlog

	manifestPath string

	// stats is the writer-built Statistics for SegmentInfo.Stats: insert /
	// delta counts and sizes, bloom-filter / BM25 stats_binlog_size,
	// timestamp_from/to/quantiles. DataCoord persists it directly on
	// SaveBinlogPathsRequest.Stats; for V2 (or any flush that returns nil
	// here) the handler falls back to computing from FieldBinlog arrays.
	stats *datapb.Statistics

	writeRetryOpts []retry.Option

	failureCallback func(err error)

	tr *timerecord.TimeRecorder

	flushedSize int64
	execTime    time.Duration

	// writtenColumnGroups and preparedStats capture Write's outputs for the
	// WriteMeta step, which runs after the DataCoord registration. The stats
	// object is the exact one whose Publish() DataCoord just persisted.
	writtenColumnGroups []storagecommon.ColumnGroup
	preparedStats       *metacache.SegmentStats

	// writeSkipped marks a Write that found the segment gone (dropped or
	// already synced): WriteMeta must then no-op as well, mirroring the
	// single-check behavior of Run.
	writeSkipped bool

	// storage config used in pooled tasks, optional
	// use singleton config for non-pooled tasks
	storageConfig *indexpb.StorageConfig
}

func (t *SyncTask) getLogger() *mlog.Logger {
	return mlog.With(
		mlog.FieldCollectionID(t.collectionID),
		mlog.FieldPartitionID(t.partitionID),
		mlog.FieldSegmentID(t.segmentID),
		mlog.String("channel", t.channelName),
		mlog.String("level", t.level.String()),
	)
}

func (t *SyncTask) HandleError(err error) {
	if t.failureCallback != nil {
		t.failureCallback(err)
	}

	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel, t.level.String()).Inc()
	if !t.pack.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel, t.level.String()).Inc()
	}
}

func (t *SyncTask) Run(ctx context.Context) (err error) {
	t.tr = timerecord.NewTimeRecorder("syncTask")
	defer func() {
		if err != nil {
			t.HandleError(err)
		}
	}()
	if err = t.Write(ctx); err != nil {
		return err
	}
	return t.WriteMeta(ctx)
}

// Write writes the sync pack's data into object storage. It is the first half
// of Run: after a successful Write the produced binlogs (see Binlogs) are
// durable and ready to be registered. WriteMeta performs the DataCoord
// registration, so a caller can persist an outbox record between the two to
// make the registration crash-replayable.
func (t *SyncTask) Write(ctx context.Context) (err error) {
	if t.tr == nil {
		t.tr = timerecord.NewTimeRecorder("syncTask")
	}
	logger := t.getLogger()

	segmentInfo, has := t.metacache.GetSegmentByID(t.segmentID)
	if !has {
		t.writeSkipped = true
		if t.pack.isDrop {
			logger.Info(ctx, "segment dropped, discard sync task")
			return nil
		}
		logger.Warn(ctx, "segment not found in metacache, may be already synced")
		return nil
	}

	columnGroups := t.getColumnGroups(segmentInfo)
	t.writtenColumnGroups = columnGroups

	// statsWriter, when set (V2 / V3), exposes this sync's prepared cumulative
	// stats. SyncTask.WriteMeta installs it on the metaCache only after the
	// DataCoord ack below, so a failed/retried sync never double-counts.
	var statsWriter interface {
		PreparedStats() *metacache.SegmentStats
	}

	switch segmentInfo.GetStorageVersion() {
	case storage.StorageV2:
		// New sync task means needs to flush data immediately, so do not need to buffer data in writer again.
		writer := NewBulkPackWriterV2(t.metacache, t.schema, t.chunkManager, t.allocator, 0,
			packed.DefaultMultiPartUploadSize, t.storageConfig, columnGroups, t.writeRetryOpts...)
		t.insertBinlogs, t.deltaBinlog, t.statsBinlogs, t.bm25Binlogs, t.manifestPath, t.flushedSize, t.stats, err = writer.Write(ctx, t.pack)
		statsWriter = writer
	case storage.StorageV3:
		writer := NewBulkPackWriterV3(t.metacache, t.schema, t.chunkManager, t.allocator, 0,
			packed.DefaultMultiPartUploadSize, t.storageConfig, columnGroups, segmentInfo.ManifestPath(), t.writeRetryOpts...)
		t.insertBinlogs, t.deltaBinlog, t.statsBinlogs, t.bm25Binlogs, t.manifestPath, t.flushedSize, t.stats, err = writer.Write(ctx, t.pack)
		statsWriter = writer
	default:
		writer, writerErr := NewBulkPackWriter(t.metacache, t.schema, t.chunkManager, t.allocator, t.writeRetryOpts...)
		if writerErr != nil {
			return writerErr
		}
		t.insertBinlogs, t.deltaBinlog, t.statsBinlogs, t.bm25Binlogs, t.flushedSize, err = writer.Write(ctx, t.pack)
	}

	if err != nil {
		logger.Warn(ctx, "failed to write sync data with storage v2 format", mlog.Err(err))
		return err
	}

	getDataCount := func(binlogs ...*datapb.FieldBinlog) int64 {
		count := int64(0)
		for _, binlog := range binlogs {
			for _, fbinlog := range binlog.GetBinlogs() {
				count += fbinlog.GetEntriesNum()
			}
		}
		return count
	}
	metrics.DataNodeWriteDataCount.WithLabelValues(paramtable.GetStringNodeID(), t.dataSource, metrics.InsertLabel, fmt.Sprint(t.collectionID)).Add(float64(t.batchRows))
	metrics.DataNodeWriteDataCount.WithLabelValues(paramtable.GetStringNodeID(), t.dataSource, metrics.DeleteLabel, fmt.Sprint(t.collectionID)).Add(float64(getDataCount(t.deltaBinlog)))
	metrics.DataNodeFlushedSize.WithLabelValues(paramtable.GetStringNodeID(), t.dataSource, t.level.String()).Add(float64(t.flushedSize))

	metrics.DataNodeFlushedRows.WithLabelValues(paramtable.GetStringNodeID(), t.dataSource).Add(float64(t.batchRows))

	metrics.DataNodeSave2StorageLatency.WithLabelValues(paramtable.GetStringNodeID(), t.level.String()).Observe(float64(t.tr.RecordSpan().Milliseconds()))
	if statsWriter != nil {
		t.preparedStats = statsWriter.PreparedStats()
	}
	return nil
}

// WriteMeta registers the written data at DataCoord and finalizes the meta
// cache. It is the second half of Run; Write must have succeeded before. When
// the task has no MetaWriter (e.g. a purely local materialization) the
// registration step is skipped.
func (t *SyncTask) WriteMeta(ctx context.Context) (err error) {
	logger := t.getLogger()
	if t.writeSkipped {
		return nil
	}

	if t.metaWriter != nil {
		err = t.writeMeta(ctx)
		if err != nil {
			logger.Warn(ctx, "failed to save serialized data into storage", mlog.Err(err))
			return err
		}
	}

	t.pack.ReleaseData()

	actions := []metacache.SegmentAction{metacache.FinishSyncing(t.batchRows), metacache.UpdateManifestPath(t.manifestPath)}
	if len(t.writtenColumnGroups) > 0 {
		actions = append(actions, metacache.UpdateCurrentSplit(t.writtenColumnGroups))
	}
	if t.pack.isFlush {
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Flushed))
	}
	// Install the prepared cumulative stats directly in the commit transaction:
	// no digest work, the exact object whose Publish() DataCoord just persisted.
	if t.preparedStats != nil {
		actions = append(actions, metacache.SetStatistics(t.preparedStats))
	}
	t.metacache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(t.segmentID))

	if t.pack.isDrop {
		t.metacache.RemoveSegments(metacache.WithSegmentIDs(t.segmentID))
		logger.Info(ctx, "segment removed", mlog.FieldSegmentID(t.segmentID), mlog.String("channel", t.channelName))
	}

	t.execTime = t.tr.ElapseSpan()
	logger.Info(ctx, "task done", mlog.Int64("flushedSize", t.flushedSize), mlog.Duration("timeTaken", t.execTime))

	if !t.pack.isFlush {
		metrics.DataNodeAutoFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel, t.level.String()).Inc()
	}
	metrics.DataNodeFlushBufferCount.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel, t.level.String()).Inc()

	// Publish filesystem metrics after sync task completion
	storagev2.PublishFilesystemMetricsWithConfig(t.storageConfig)

	return nil
}

func (t *SyncTask) getColumnGroups(segmentInfo *metacache.SegmentInfo) []storagecommon.ColumnGroup {
	return resolveColumnGroups(segmentInfo, t.schema, t.segmentID, t.calcColumnStats)
}

func resolveColumnGroups(segmentInfo *metacache.SegmentInfo, schema *schemapb.CollectionSchema, segmentID int64, calcColumnStats func() map[int64]storagecommon.ColumnStats) []storagecommon.ColumnGroup {
	// column group only needed for storage v2/v3 segments
	if segmentInfo.GetStorageVersion() != storage.StorageV2 && segmentInfo.GetStorageVersion() != storage.StorageV3 {
		return nil
	}

	// empty pack
	if schema == nil {
		return nil
	}

	allFields := typeutil.GetAllFieldSchemas(schema)

	// use previous split if already exists
	if currentSplit := segmentInfo.GetCurrentSplit(); currentSplit != nil {
		for _, cg := range currentSplit {
			// legacy split found, use legacy policy
			if len(cg.Fields) == 0 {
				result := storagecommon.SplitColumns(allFields, map[int64]storagecommon.ColumnStats{}, storagecommon.NewLocalFormatPolicy(), storagecommon.NewSelectedDataTypePolicy(), storagecommon.NewRemanentShortPolicy(-1))
				result = storagecommon.FillColumnGroupFormats(result, paramtable.Get().DataNodeCfg.StorageFormat.GetValue())
				mlog.Info(context.TODO(), "use legacy split policy", mlog.FieldSegmentID(segmentID), mlog.Stringers("columnGroups", result))
				return result
			}
		}
		field2idx := make(map[int64]int)
		for idx, field := range allFields {
			field2idx[field.GetFieldID()] = idx
		}
		for idx, cg := range currentSplit {
			cg.Columns = lo.Map(cg.Fields, func(fieldID int64, _ int) int {
				return field2idx[fieldID]
			})
			currentSplit[idx] = cg
		}
		if segmentInfo.GetStorageVersion() == storage.StorageV3 && segmentInfo.ManifestPath() != "" {
			return currentSplit
		}
		return storagecommon.FillColumnGroupFormats(currentSplit, paramtable.Get().DataNodeCfg.StorageFormat.GetValue())
	}

	policies := storagecommon.DefaultPolicies()
	stats := map[int64]storagecommon.ColumnStats{}
	if calcColumnStats != nil {
		stats = calcColumnStats()
	}
	result := storagecommon.SplitColumns(allFields, stats, policies...)
	result = storagecommon.FillColumnGroupFormats(result, paramtable.Get().DataNodeCfg.StorageFormat.GetValue())
	mlog.Info(context.TODO(), "sync new split columns", mlog.FieldSegmentID(segmentID), mlog.Stringers("columnGroups", result))
	return result
}

func (t *SyncTask) calcColumnStats() map[int64]storagecommon.ColumnStats {
	result := make(map[int64]storagecommon.ColumnStats)

	memorySizes := make(map[int64]int64)
	rowNums := make(map[int64]int64)
	for _, data := range t.pack.insertData {
		for fieldID, fieldData := range data.Data {
			memorySizes[fieldID] += int64(fieldData.GetMemorySize())
			rowNums[fieldID] += int64(fieldData.RowNum())
		}
	}
	for fieldID, rowNum := range rowNums {
		if rowNum > 0 {
			result[fieldID] = storagecommon.ColumnStats{
				AvgSize: memorySizes[fieldID] / rowNum,
			}
		}
	}
	return result
}

// writeMeta updates segments via meta writer in option.
func (t *SyncTask) writeMeta(ctx context.Context) error {
	return t.metaWriter.UpdateSync(ctx, t)
}

func (t *SyncTask) SegmentID() int64 {
	return t.segmentID
}

// SaveBinlogPathsRequest assembles the DataCoord registration request for the
// written data: the complete binlog set (segment accumulations plus this
// sync's writes), the checkpoint and start positions, and the storage
// metadata. The request is full-replacement (WithFullBinlogs): DataCoord
// replaces the segment's binlog arrays, checkpoint and stats with the request
// content, so re-sending the identical request after a crash is idempotent.
// It returns nil when the segment is missing from the meta cache.
func (t *SyncTask) SaveBinlogPathsRequest(serverID int64) *datapb.SaveBinlogPathsRequest {
	segment, ok := t.metacache.GetSegmentByID(t.segmentID)
	if !ok {
		return nil
	}
	insertFieldBinlogs := append(segment.Binlogs(), storage.SortFieldBinlogs(t.insertBinlogs)...)
	statsFieldBinlogs := append(segment.Statslogs(), lo.MapToSlice(t.statsBinlogs, func(_ int64, fieldBinlog *datapb.FieldBinlog) *datapb.FieldBinlog { return fieldBinlog })...)
	deltaFieldBinlogs := segment.Deltalogs()
	if t.deltaBinlog != nil && len(t.deltaBinlog.Binlogs) > 0 {
		deltaFieldBinlogs = append(deltaFieldBinlogs, t.deltaBinlog)
	}
	deltaBm25StatsBinlogs := segment.Bm25logs()
	if len(t.bm25Binlogs) > 0 {
		deltaBm25StatsBinlogs = append(segment.Bm25logs(), lo.MapToSlice(t.bm25Binlogs, func(_ int64, fieldBinlog *datapb.FieldBinlog) *datapb.FieldBinlog { return fieldBinlog })...)
	}
	checkPoints := []*datapb.CheckPoint{{
		SegmentID: t.segmentID,
		NumOfRows: segment.FlushedRows() + t.batchRows,
		Position:  t.checkpoint,
	}}
	// Get not reported L1's start positions
	startPos := lo.Map(t.metacache.GetSegmentsBy(
		metacache.WithSegmentState(commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing),
		metacache.WithLevel(datapb.SegmentLevel_L1), metacache.WithStartPosNotRecorded()),
		func(info *metacache.SegmentInfo, _ int) *datapb.SegmentStartPosition {
			return &datapb.SegmentStartPosition{
				SegmentID:     info.SegmentID(),
				StartPosition: info.StartPosition(),
			}
		})
	// L0 brings its own start position
	if t.level == datapb.SegmentLevel_L0 {
		startPos = append(startPos, &datapb.SegmentStartPosition{SegmentID: t.segmentID, StartPosition: t.StartPosition()})
	}
	return &datapb.SaveBinlogPathsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(serverID),
		),
		SegmentID:           t.segmentID,
		CollectionID:        t.collectionID,
		PartitionID:         t.partitionID,
		Field2BinlogPaths:   insertFieldBinlogs,
		Field2StatslogPaths: statsFieldBinlogs,
		Field2Bm25LogPaths:  deltaBm25StatsBinlogs,
		Deltalogs:           deltaFieldBinlogs,
		CheckPoints:         checkPoints,
		StartPositions:      startPos,
		Flushed:             t.pack.isFlush,
		Dropped:             t.pack.isDrop,
		Channel:             t.channelName,
		SegLevel:            t.level,
		StorageVersion:      segment.GetStorageVersion(),
		WithFullBinlogs:     true,
		ManifestPath:        t.manifestPath,
		// Stats carries the complete cumulative Statistics for the segment,
		// published from the growing-segment collector (all fields, both V2
		// and V3).
		Stats: t.stats,
	}
}

func (t *SyncTask) Checkpoint() *msgpb.MsgPosition {
	return t.checkpoint
}

func (t *SyncTask) StartPosition() *msgpb.MsgPosition {
	return t.startPosition
}

func (t *SyncTask) ChannelName() string {
	return t.channelName
}

func (t *SyncTask) IsFlush() bool {
	return t.pack.isFlush
}

func (t *SyncTask) IsDrop() bool {
	return t.pack.isDrop
}

func (t *SyncTask) Binlogs() (map[int64]*datapb.FieldBinlog, map[int64]*datapb.FieldBinlog, *datapb.FieldBinlog, map[int64]*datapb.FieldBinlog) {
	return t.insertBinlogs, t.statsBinlogs, t.deltaBinlog, t.bm25Binlogs
}

func (t *SyncTask) MarshalJSON() ([]byte, error) {
	deltaRowCount := int64(0)
	if t.pack != nil && t.pack.deltaData != nil {
		deltaRowCount = t.pack.deltaData.RowCount
	}
	return json.Marshal(&metricsinfo.SyncTask{
		SegmentID:     t.segmentID,
		BatchRows:     t.batchRows,
		SegmentLevel:  t.level.String(),
		TSFrom:        tsoutil.PhysicalTimeFormat(t.tsFrom),
		TSTo:          tsoutil.PhysicalTimeFormat(t.tsTo),
		DeltaRowCount: deltaRowCount,
		FlushSize:     t.flushedSize,
		RunningTime:   t.execTime.String(),
		NodeID:        paramtable.GetNodeID(),
	})
}
