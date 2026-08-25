package syncmgr

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)
// MetaWriter is the interface for SyncManager to write segment sync meta.
type MetaWriter interface {
	UpdateSync(context.Context, *SyncTask) error
	UpdateGrowingSourceSync(context.Context, *GrowingSourceSyncTask) error
	DropChannel(context.Context, string) error
}

type brokerMetaWriter struct {
	broker   broker.Broker
	opts     []retry.Option
	serverID int64
}

func BrokerMetaWriter(broker broker.Broker, serverID int64, opts ...retry.Option) MetaWriter {
	return &brokerMetaWriter{
		broker:   broker,
		serverID: serverID,
		opts:     opts,
	}
}

func (b *brokerMetaWriter) UpdateSync(ctx context.Context, pack *SyncTask) error {
	req := pack.SaveBinlogPathsRequest(b.serverID)
	if req == nil {
		return merr.WrapErrSegmentNotFound(pack.segmentID)
	}

	getBinlogNum := func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) }
	mlog.Info(ctx, "SaveBinlogPath",
		mlog.Int64("SegmentID", pack.segmentID),
		mlog.Int64("CollectionID", pack.collectionID),
		mlog.Int64("ParitionID", pack.partitionID),
		mlog.Any("startPos", req.GetStartPositions()),
		mlog.Any("checkPoints", req.GetCheckPoints()),
		mlog.Int("binlogNum", lo.SumBy(req.GetField2BinlogPaths(), getBinlogNum)),
		mlog.Int("statslogNum", lo.SumBy(req.GetField2StatslogPaths(), getBinlogNum)),
		mlog.Int("deltalogNum", lo.SumBy(req.GetDeltalogs(), getBinlogNum)),
		mlog.Int("bm25logNum", lo.SumBy(req.GetField2Bm25LogPaths(), getBinlogNum)),
		mlog.String("manifestPath", pack.manifestPath),
		mlog.String("vChannelName", pack.channelName),
	)

	err := retry.Handle(ctx, func() (bool, error) {
		err := b.broker.SaveBinlogPaths(ctx, req)
		// Segment not found during stale segment flush. Segment might get compacted already.
		// Stop retry and still proceed to the end, ignoring this error.
		if !pack.pack.isFlush && errors.Is(err, merr.ErrSegmentNotFound) {
			mlog.Warn(ctx, "stale segment not found, could be compacted",
				mlog.FieldSegmentID(pack.segmentID))
			mlog.Warn(ctx, "failed to SaveBinlogPaths",
				mlog.FieldSegmentID(pack.segmentID),
				mlog.Err(err))
			return false, nil
		}
		// meta error, datanode handles a virtual channel does not belong here
		if errors.IsAny(err, merr.ErrSegmentNotFound, merr.ErrChannelNotFound) {
			mlog.Warn(ctx, "meta error found, skip sync and start to drop virtual channel", mlog.String("channel", pack.channelName))
			return false, nil
		}

		if err != nil {
			return !merr.IsCanceledOrTimeout(err), err
		}

		return false, nil
	}, b.opts...)
	if err != nil {
		mlog.Warn(ctx, "failed to SaveBinlogPaths",
			mlog.FieldSegmentID(pack.segmentID),
			mlog.Err(err))
		return err
	}

	pack.metacache.UpdateSegments(metacache.SetStartPosRecorded(true), metacache.WithSegmentIDs(lo.Map(req.GetStartPositions(), func(pos *datapb.SegmentStartPosition, _ int) int64 { return pos.GetSegmentID() })...))
	pack.metacache.UpdateSegments(metacache.MergeSegmentAction(
		metacache.UpdateBinlogs(req.GetField2BinlogPaths()),
		metacache.UpdateStatslogs(req.GetField2StatslogPaths()),
		metacache.UpdateDeltalogs(req.GetDeltalogs()),
		metacache.UpdateBm25logs(req.GetField2Bm25LogPaths()),
	), metacache.WithSegmentIDs(pack.segmentID))
	return nil
}

func (b *brokerMetaWriter) UpdateGrowingSourceSync(ctx context.Context, task *GrowingSourceSyncTask) error {
	segment, ok := task.metacache.GetSegmentByID(task.segmentID)
	if !ok {
		return merr.WrapErrSegmentNotFound(task.segmentID)
	}
	if segment.GetStorageVersion() != storage.StorageV3 {
		return merr.WrapErrDataIntegrityMsg("growing source sync requires StorageV3 segment, segmentID=%d storageVersion=%d",
			task.segmentID, segment.GetStorageVersion())
	}

	insertFieldBinlogs := segment.Binlogs()
	if len(task.insertBinlogs) > 0 {
		insertFieldBinlogs = append(segment.Binlogs(), storage.SortFieldBinlogs(task.insertBinlogs)...)
	}
	statsFieldBinlogs := segment.Statslogs()
	deltaFieldBinlogs := segment.Deltalogs()
	bm25FieldBinlogs := segment.Bm25logs()
	startPos := task.startPositions()
	checkPoints := []*datapb.CheckPoint{{
		SegmentID: task.segmentID,
		NumOfRows: segment.FlushedRows() + task.batchRows,
		Position:  task.checkpoint,
	}}

	mlog.Info(ctx, "SaveBinlogPath for growing source sync",
		mlog.Int64("SegmentID", task.segmentID),
		mlog.Int64("CollectionID", task.collectionID),
		mlog.Int64("ParitionID", task.partitionID),
		mlog.Any("startPos", startPos),
		mlog.Any("checkPoints", checkPoints),
		mlog.Int("binlogNum", lo.SumBy(insertFieldBinlogs, func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) })),
		mlog.Int("statslogNum", lo.SumBy(statsFieldBinlogs, func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) })),
		mlog.Int("deltalogNum", lo.SumBy(deltaFieldBinlogs, func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) })),
		mlog.Int("bm25logNum", lo.SumBy(bm25FieldBinlogs, func(fBinlog *datapb.FieldBinlog) int { return len(fBinlog.GetBinlogs()) })),
		mlog.String("manifestPath", task.manifestPath),
		mlog.String("vChannelName", task.channelName),
	)

	// Insert/delta aggregates live on the cumulative collector, not the in-memory
	// binlog arrays — after a V3 recovery those arrays are empty (their per-field
	// KVs are skipped), so rebuilding from them would ship a Statistics
	// reflecting only post-recovery batches and undercount everything else.
	// Mirror the SyncTask finalizeStats path: Digest this batch onto a clone of
	// the restored cumulative collector, then install the clone back on success
	// so the next batch keeps accumulating. Digest does not read insert-binlog
	// timestamps, so pass the batch's range explicitly.
	statsClone := segment.Statistics().Clone()
	tsFrom, tsTo := insertBinlogTimestampRange(task.insertBinlogs)
	statsClone.Digest(task.insertBinlogs, nil, 0, task.batchRows, tsFrom, tsTo)
	stats := statsClone.Publish()
	// V3 stats (bloom-filter / BM25 footprint) live in the manifest, not in
	// statslog KV arrays; source StatsBinlogSize from the just-committed manifest.
	if stats != nil && task.storageConfig != nil && task.manifestPath != "" {
		if statsBlobSize, err := packed.StatsBinlogSizeFromManifest(task.manifestPath, task.storageConfig); err != nil {
			// Degrade gracefully: keep the collector's StatsBinlogSize rather than
			// block the flush commit on a transient manifest read error; a later
			// compaction corrects the footprint.
			mlog.Warn(ctx, "failed to read manifest stats footprint for growing source flush; StatsBinlogSize may under-count until next compaction",
				mlog.Int64("segmentID", task.segmentID), mlog.String("manifestPath", task.manifestPath), mlog.Err(err))
		} else {
			stats.StatsBinlogSize = statsBlobSize
		}
	}

	req := &datapb.SaveBinlogPathsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(b.serverID),
		),
		SegmentID:           task.segmentID,
		CollectionID:        task.collectionID,
		PartitionID:         task.partitionID,
		Field2BinlogPaths:   insertFieldBinlogs,
		Field2StatslogPaths: statsFieldBinlogs,
		Field2Bm25LogPaths:  bm25FieldBinlogs,
		Deltalogs:           deltaFieldBinlogs,
		CheckPoints:         checkPoints,
		StartPositions:      startPos,
		Flushed:             task.IsFlush(),
		Dropped:             task.IsDrop(),
		Channel:             task.channelName,
		SegLevel:            task.level,
		StorageVersion:      segment.GetStorageVersion(),
		WithFullBinlogs:     true,
		ManifestPath:        task.manifestPath,
		Stats:               stats,
	}

	err := retry.Handle(ctx, func() (bool, error) {
		err := b.broker.SaveBinlogPaths(ctx, req)
		if errors.IsAny(err, merr.ErrSegmentNotFound, merr.ErrChannelNotFound) {
			mlog.Warn(ctx, "meta error found, fail growing source sync",
				mlog.String("channel", task.channelName),
				mlog.Int64("segmentID", task.segmentID),
				mlog.Err(err))
			return false, err
		}
		if err != nil {
			return !merr.IsCanceledOrTimeout(err), err
		}
		return false, nil
	}, b.opts...)
	if err != nil {
		mlog.Warn(ctx, "failed to SaveBinlogPaths for growing source sync",
			mlog.Int64("segmentID", task.segmentID),
			mlog.Err(err))
		return err
	}

	task.metacache.UpdateSegments(metacache.SetStartPosRecorded(true), metacache.WithSegmentIDs(lo.Map(startPos, func(pos *datapb.SegmentStartPosition, _ int) int64 {
		return pos.GetSegmentID()
	})...))
	task.metacache.UpdateSegments(metacache.MergeSegmentAction(
		metacache.UpdateBinlogs(insertFieldBinlogs),
		metacache.UpdateStatslogs(statsFieldBinlogs),
		metacache.UpdateDeltalogs(deltaFieldBinlogs),
		metacache.UpdateBm25logs(bm25FieldBinlogs),
		// Install the digested cumulative collector so the next batch accumulates
		// on top of it instead of resetting to the restored baseline. Only on the
		// success path, so a failed+retried sync re-digests from the unchanged
		// base (idempotent), matching the SyncTask SetStatistics behavior.
		metacache.SetStatistics(statsClone),
	), metacache.WithSegmentIDs(task.segmentID))
	return nil
}

// insertBinlogTimestampRange returns the min TimestampFrom and max TimestampTo
// across a batch's insert binlogs. Digest advances the collector's timestamp
// marks from the explicit range rather than reading insert-binlog timestamps.
func insertBinlogTimestampRange(inserts map[int64]*datapb.FieldBinlog) (uint64, uint64) {
	var tsFrom uint64 = math.MaxUint64
	var tsTo uint64
	for _, fb := range inserts {
		for _, l := range fb.GetBinlogs() {
			if from := l.GetTimestampFrom(); from > 0 && from < tsFrom {
				tsFrom = from
			}
			if to := l.GetTimestampTo(); to > tsTo {
				tsTo = to
			}
		}
	}
	if tsFrom == math.MaxUint64 {
		tsFrom = 0
	}
	return tsFrom, tsTo
}

func (b *brokerMetaWriter) DropChannel(ctx context.Context, channelName string) error {
	err := retry.Handle(ctx, func() (bool, error) {
		status, err := b.broker.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithSourceID(b.serverID),
			),
			ChannelName: channelName,
		})
		err = merr.CheckRPCCall(status, err)
		if err != nil {
			return !merr.IsCanceledOrTimeout(err), err
		}
		return false, nil
	}, b.opts...)
	if err != nil {
		mlog.Warn(ctx, "failed to DropChannel",
			mlog.String("channel", channelName),
			mlog.Err(err))
	}
	return err
}
