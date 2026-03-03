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
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// GcOption garbage collection options
type GcOption struct {
	cli              storage.ChunkManager // client
	enabled          bool                 // enable switch
	checkInterval    time.Duration        // each interval
	missingTolerance time.Duration        // key missing in meta tolerance time
	dropTolerance    time.Duration        // dropped segment related key tolerance time
	scanInterval     time.Duration        // interval for scan residue for interupted log wrttien

	broker           broker.Broker
	removeObjectPool *conc.Pool[struct{}]
}

// garbageCollector handles garbage files in object storage
// which could be dropped collection remanent or data node failure traces
type garbageCollector struct {
	ctx    context.Context
	cancel context.CancelFunc

	option  GcOption
	meta    *meta
	handler Handler

	startOnce        sync.Once
	stopOnce         sync.Once
	wg               sync.WaitGroup
	cmdCh            chan gcCmd
	pauseUntil       *gcPauseRecords
	pausedCollection *typeutil.ConcurrentMap[int64, *gcPauseRecords]
	controlChannels  map[string]chan gcCmd

	systemMetricsListener *hardware.SystemMetricsListener
}

type gcCmd struct {
	cmdType      datapb.GcCommand
	duration     time.Duration
	collectionID int64
	ticket       string
	done         chan error
	timeout      <-chan struct{}
}

type gcPauseRecord struct {
	ticket     string
	pauseUntil time.Time
}

type gcPauseRecords struct {
	mut     sync.RWMutex
	maxLen  int
	records typeutil.Heap[gcPauseRecord]
}

func (gc *gcPauseRecords) PauseUntil() time.Time {
	// nil protection
	if gc == nil {
		return time.Time{}
	}
	gc.mut.RLock()
	defer gc.mut.RUnlock()
	// no pause records, return zero value
	if gc.records.Len() == 0 {
		return time.Time{}
	}

	return gc.records.Peek().pauseUntil
}

func (gc *gcPauseRecords) Insert(ticket string, pauseUntil time.Time) error {
	gc.mut.Lock()
	defer gc.mut.Unlock()

	// heap small enough, short path
	if gc.records.Len() < gc.maxLen {
		gc.records.Push(gcPauseRecord{
			ticket:     ticket,
			pauseUntil: pauseUntil,
		})
		return nil
	}

	records := make([]gcPauseRecord, 0, gc.records.Len())
	now := time.Now()
	for gc.records.Len() > 0 {
		record := gc.records.Pop()
		if record.pauseUntil.After(now) {
			records = append(records, record)
		}
	}

	if gc.records.Len() < gc.maxLen {
		gc.records.Push(gcPauseRecord{
			ticket:     ticket,
			pauseUntil: pauseUntil,
		})
	}

	// too many pause records, refresh heap
	return merr.WrapErrTooManyRequests(64, "too many pause records")
}

func (gc *gcPauseRecords) Delete(ticket string) {
	gc.mut.Lock()
	defer gc.mut.Unlock()
	now := time.Now()
	records := make([]gcPauseRecord, 0, gc.records.Len())
	for gc.records.Len() > 0 {
		record := gc.records.Pop()
		if now.Before(record.pauseUntil) && record.ticket != ticket {
			records = append(records, record)
		}
	}
	gc.records = typeutil.NewObjectArrayBasedMaximumHeap(records, func(r gcPauseRecord) int64 {
		return r.pauseUntil.UnixNano()
	})
}

func (gc *gcPauseRecords) Len() int {
	gc.mut.RLock()
	defer gc.mut.RUnlock()
	return gc.records.Len()
}

func NewGCPauseRecords() *gcPauseRecords {
	return &gcPauseRecords{
		records: typeutil.NewObjectArrayBasedMaximumHeap[gcPauseRecord, int64]([]gcPauseRecord{}, func(r gcPauseRecord) int64 {
			return r.pauseUntil.UnixNano()
		}),
		maxLen: 64,
	}
}

// newSystemMetricsListener creates a system metrics listener for garbage collector.
// used to slow down the garbage collector when cpu usage is high.
func newSystemMetricsListener(opt *GcOption) *hardware.SystemMetricsListener {
	return &hardware.SystemMetricsListener{
		Cooldown:  15 * time.Second,
		Context:   false,
		Condition: func(metrics hardware.SystemMetrics, listener *hardware.SystemMetricsListener) bool { return true },
		Callback: func(metrics hardware.SystemMetrics, listener *hardware.SystemMetricsListener) {
			isSlowDown := listener.Context.(bool)
			if metrics.UsedRatio() > paramtable.Get().DataCoordCfg.GCSlowDownCPUUsageThreshold.GetAsFloat() {
				if !isSlowDown {
					log.Info(context.TODO(), "garbage collector slow down...", log.Float64("cpuUsage", metrics.UsedRatio()))
					opt.removeObjectPool.Resize(1)
					listener.Context = true
				}
				return
			}
			if isSlowDown {
				log.Info(context.TODO(), "garbage collector slow down finished", log.Float64("cpuUsage", metrics.UsedRatio()))
				opt.removeObjectPool.Resize(paramtable.Get().DataCoordCfg.GCRemoveConcurrent.GetAsInt())
				listener.Context = false
			}
		},
	}
}

// newGarbageCollector create garbage collector with meta and option
func newGarbageCollector(meta *meta, handler Handler, opt GcOption) *garbageCollector {
	log.Info(context.TODO(), "GC with option",
		log.Bool("enabled", opt.enabled),
		log.Duration("interval", opt.checkInterval),
		log.Duration("scanInterval", opt.scanInterval),
		log.Duration("missingTolerance", opt.missingTolerance),
		log.Duration("dropTolerance", opt.dropTolerance))
	opt.removeObjectPool = conc.NewPool[struct{}](Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	metaSignal := make(chan gcCmd)
	orphanSignal := make(chan gcCmd)
	// control signal channels
	controlChannels := map[string]chan gcCmd{
		"meta":   metaSignal,
		"orphan": orphanSignal,
	}
	return &garbageCollector{
		ctx:                   ctx,
		cancel:                cancel,
		meta:                  meta,
		handler:               handler,
		option:                opt,
		cmdCh:                 make(chan gcCmd),
		systemMetricsListener: newSystemMetricsListener(&opt),
		pauseUntil:            NewGCPauseRecords(),
		pausedCollection:      typeutil.NewConcurrentMap[int64, *gcPauseRecords](),
		controlChannels:       controlChannels,
	}
}

// start a goroutine and perform gc check every `checkInterval`
func (gc *garbageCollector) start() {
	if gc.option.enabled {
		if gc.option.cli == nil {
			log.Warn(gc.ctx, "DataCoord gc enabled, but SSO client is not provided")
			return
		}
		gc.startOnce.Do(func() {
			gc.work(gc.ctx)
		})
	}
}

// GcStatus holds the current status of the garbage collector.
type GcStatus struct {
	IsPaused      bool
	TimeRemaining time.Duration
}

// GetStatus returns the current status of the garbage collector.
func (gc *garbageCollector) GetStatus() GcStatus {
	pauseUntil := gc.pauseUntil.PauseUntil()
	now := time.Now()

	if now.Before(pauseUntil) {
		return GcStatus{
			IsPaused:      true,
			TimeRemaining: pauseUntil.Sub(now),
		}
	}

	return GcStatus{
		IsPaused:      false,
		TimeRemaining: 0,
	}
}

func (gc *garbageCollector) Pause(ctx context.Context, collectionID int64, ticket string, pauseDuration time.Duration) error {
	if !gc.option.enabled {
		log.Info(ctx, "garbage collection not enabled")
		return nil
	}
	done := make(chan error, 1)
	select {
	case gc.cmdCh <- gcCmd{
		cmdType:      datapb.GcCommand_Pause,
		duration:     pauseDuration,
		collectionID: collectionID,
		ticket:       ticket,
		done:         done,
		timeout:      ctx.Done(),
	}:
		return <-done
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (gc *garbageCollector) Resume(ctx context.Context, collectionID int64, ticket string) error {
	if !gc.option.enabled {
		log.Warn(ctx, "garbage collection not enabled, cannot resume")
		return merr.WrapErrServiceUnavailable("garbage collection not enabled")
	}
	done := make(chan error)
	select {
	case gc.cmdCh <- gcCmd{
		cmdType:      datapb.GcCommand_Resume,
		done:         done,
		collectionID: collectionID,
		ticket:       ticket,
		timeout:      ctx.Done(),
	}:
		<-done
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// work contains actual looping check logic
func (gc *garbageCollector) work(ctx context.Context) {
	// TODO: fast cancel for gc when closing.
	// Run gc tasks in parallel.
	gc.wg.Add(3)
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "meta", gc.option.checkInterval, func(ctx context.Context, signal <-chan gcCmd) {
			gc.recycleDroppedSegments(ctx, signal)
			gc.recycleChannelCPMeta(ctx, signal)
			gc.recycleUnusedIndexes(ctx, signal)
			gc.recycleUnusedSegIndexes(ctx, signal)
			gc.recycleUnusedAnalyzeFiles(ctx, signal)
			gc.recycleUnusedTextIndexFiles(ctx, signal)
			gc.recycleUnusedJSONIndexFiles(ctx, signal)
			gc.recycleUnusedJSONStatsFiles(ctx, signal)
			gc.recyclePendingSnapshots(ctx, signal) // Cleanup orphaned snapshot files from failed 2PC
		})
	}()
	go func() {
		defer gc.wg.Done()
		gc.runRecycleTaskWithPauser(ctx, "orphan", gc.option.scanInterval, func(ctx context.Context, signal <-chan gcCmd) {
			// orphan file not controlled by collection level pause for now
			gc.recycleUnusedBinlogFiles(ctx)
			gc.recycleUnusedIndexFiles(ctx)
		})
	}()
	go func() {
		defer gc.wg.Done()
		gc.startControlLoop(ctx)
	}()
}

func (gc *garbageCollector) ackSignal(signal <-chan gcCmd) {
	select {
	case cmd := <-signal:
		if cmd.done != nil {
			close(cmd.done)
		}
	default:
	}
}

// startControlLoop start a control loop for garbageCollector.
func (gc *garbageCollector) startControlLoop(ctx context.Context) {
	hardware.RegisterSystemMetricsListener(gc.systemMetricsListener)
	defer hardware.UnregisterSystemMetricsListener(gc.systemMetricsListener)

	for {
		select {
		case cmd := <-gc.cmdCh:
			switch cmd.cmdType {
			case datapb.GcCommand_Pause:
				err := gc.pause(cmd)
				cmd.done <- err
			case datapb.GcCommand_Resume:
				gc.resume(cmd)
			}
			close(cmd.done)
		case <-gc.ctx.Done():
			log.Warn(ctx, "garbage collector control loop quit")
			return
		}
	}
}

func (gc *garbageCollector) pause(cmd gcCmd) error {
	reqPauseUntil := time.Now().Add(cmd.duration)
	var err error
	if cmd.collectionID <= 0 { // legacy pause all
		err = gc.pauseUntil.Insert(cmd.ticket, reqPauseUntil)
		log.Info(gc.ctx, "global pause ticket recorded")
	} else {
		curr, has := gc.pausedCollection.Get(cmd.collectionID)
		if !has {
			curr = NewGCPauseRecords()
			gc.pausedCollection.Insert(cmd.collectionID, curr)
		}
		err = curr.Insert(cmd.ticket, reqPauseUntil)
		log.Info(gc.ctx, "collection new pause ticket recorded")
	}
	if err != nil {
		return err
	}
	signalCh := gc.controlChannels["meta"]
	// send signal to worker
	// make sure worker ack the pause command before returning
	signal := gcCmd{
		done:    make(chan error),
		timeout: cmd.timeout,
	}
	select {
	case signalCh <- signal:
		<-signal.done
	case <-cmd.timeout:
		// timeout, resume the pause
		gc.resume(cmd)
	}
	return nil
}

func (gc *garbageCollector) resume(cmd gcCmd) {
	// reset to zero value
	var afterResume time.Time
	if cmd.collectionID <= 0 {
		gc.pauseUntil.Delete(cmd.ticket)
		afterResume = gc.pauseUntil.PauseUntil()
	} else {
		curr, has := gc.pausedCollection.Get(cmd.collectionID)
		if has {
			curr.Delete(cmd.ticket)
			afterResume = curr.PauseUntil()
			if curr.Len() == 0 || time.Now().After(afterResume) {
				gc.pausedCollection.Remove(cmd.collectionID)
			}
		}
	}
	stillPaused := time.Now().Before(afterResume)
	log.Info(gc.ctx, "garbage collection resumed", log.Bool("stillPaused", stillPaused))
}

// runRecycleTaskWithPauser is a helper function to create a task with pauser
func (gc *garbageCollector) runRecycleTaskWithPauser(ctx context.Context, name string, interval time.Duration, task func(ctx context.Context, signal <-chan gcCmd)) {
	logger := log.With(log.String("gcType", name)).With(log.Duration("interval", interval))
	timer := time.NewTicker(interval)
	defer timer.Stop()
	// get signal channel, ok if nil, means no control
	signal := gc.controlChannels[name]
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-signal:
			// notify signal received
			close(cmd.done)
		case <-timer.C:
			globalPauseUntil := gc.pauseUntil.PauseUntil()
			if time.Now().Before(globalPauseUntil) {
				logger.Info(ctx, "garbage collector paused", log.Time("until", globalPauseUntil))
				continue
			}
			logger.Info(ctx, "garbage collector recycle task start...")
			start := time.Now()
			task(ctx, signal)
			logger.Info(ctx, "garbage collector recycle task done", log.Duration("timeCost", time.Since(start)))
		}
	}
}

func (gc *garbageCollector) collectionGCPaused(collectionID int64) bool {
	collPauseUntil, has := gc.pausedCollection.Get(collectionID)
	return has && time.Now().Before(collPauseUntil.PauseUntil())
}

// close stop the garbage collector.
func (gc *garbageCollector) close() {
	gc.stopOnce.Do(func() {
		gc.cancel()
		gc.wg.Wait()
		if gc.option.removeObjectPool != nil {
			gc.option.removeObjectPool.Release()
		}
	})
}

// recycleUnusedBinlogFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedBinlogFiles(ctx context.Context) {
	start := time.Now()
	log.Info(ctx, "start recycleUnusedBinlogFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedBinlogFiles done", log.Duration("timeCost", time.Since(start)))
	}()

	type scanTask struct {
		prefix  string
		checker func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool
		label   string
	}
	scanTasks := []scanTask{
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentInsertLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				return segment != nil
			},
			label: metrics.InsertFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentStatslogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				logID, err := binlog.GetLogIDFromBingLogPath(objectInfo.FilePath)
				if err != nil {
					log.Warn(ctx, "garbageCollector find dirty stats log", log.String("filePath", objectInfo.FilePath), log.Err(err))
					return false
				}
				return segment != nil && segment.IsStatsLogExists(logID)
			},
			label: metrics.StatFileLabel,
		},
		{
			prefix: path.Join(gc.option.cli.RootPath(), common.SegmentDeltaLogPath),
			checker: func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool {
				logID, err := binlog.GetLogIDFromBingLogPath(objectInfo.FilePath)
				if err != nil {
					log.Warn(ctx, "garbageCollector find dirty dleta log", log.String("filePath", objectInfo.FilePath), log.Err(err))
					return false
				}
				return segment != nil && segment.IsDeltaLogExists(logID)
			},
			label: metrics.DeleteFileLabel,
		},
	}

	for _, task := range scanTasks {
		gc.recycleUnusedBinLogWithChecker(ctx, task.prefix, task.label, task.checker)
	}
	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedBinLogWithChecker scans the prefix and checks the path with checker.
// GC the file if checker returns false.
func (gc *garbageCollector) recycleUnusedBinLogWithChecker(ctx context.Context, prefix string, label string, checker func(objectInfo *storage.ChunkObjectInfo, segment *SegmentInfo) bool) {
	logger := log.With(log.String("prefix", prefix))
	logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles start", log.String("prefix", prefix))
	lastFilePath := ""
	total := 0
	valid := 0
	unexpectedFailure := atomic.NewInt32(0)
	removed := atomic.NewInt32(0)
	start := time.Now()

	futures := make([]*conc.Future[struct{}], 0)
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(chunkInfo *storage.ChunkObjectInfo) bool {
		total++
		lastFilePath = chunkInfo.FilePath

		// Check file tolerance first to avoid unnecessary operation.
		if time.Since(chunkInfo.ModifyTime) <= gc.option.missingTolerance {
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles skip file since it is not expired", log.String("filePath", chunkInfo.FilePath), log.Time("modifyTime", chunkInfo.ModifyTime))
			return true
		}

		// Parse segmentID from file path.
		// TODO: Does all files in the same segment have the same segmentID?
		segmentID, err := storage.ParseSegmentIDByBinlog(gc.option.cli.RootPath(), chunkInfo.FilePath)
		if err != nil {
			unexpectedFailure.Inc()
			logger.Warn(ctx, "garbageCollector recycleUnusedBinlogFiles parse segment id error",
				log.String("filePath", chunkInfo.FilePath),
				log.Err(err))
			return true
		}

		segment := gc.meta.GetSegment(ctx, segmentID)
		if checker(chunkInfo, segment) {
			valid++
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles skip file since it is valid", log.String("filePath", chunkInfo.FilePath), log.Int64("segmentID", segmentID))
			return true
		}

		// Check if segment is referenced by any snapshot before deleting its binlog
		if segment != nil {
			snapshotMeta := gc.meta.GetSnapshotMeta()
			if snapshotMeta != nil {
				// If RefIndex is not loaded yet, skip to avoid incorrectly deleting snapshot-referenced files
				if !snapshotMeta.IsRefIndexLoadedForCollection(segment.GetCollectionID()) {
					logger.Info(ctx, "skip GC binlog files since snapshot RefIndex is not loaded yet",
						log.Int64("segmentID", segmentID),
						log.Int64("collectionID", segment.GetCollectionID()))
					valid++
					return true
				}
				if snapshotIDs := snapshotMeta.GetSnapshotBySegment(ctx, segment.GetCollectionID(), segmentID); len(snapshotIDs) > 0 {
					logger.Info(ctx, "skip GC binlog files since segment is referenced by snapshot",
						log.Int64("segmentID", segmentID),
						log.Int64s("snapshotIDs", snapshotIDs))
					valid++
					return true
				}
			}
		}

		// ignore error since it could be cleaned up next time
		file := chunkInfo.FilePath

		future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			logger := logger.With(log.String("file", file))
			logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles remove file...")

			if err = gc.option.cli.Remove(ctx, file); err != nil {
				log.Warn(ctx, "garbageCollector recycleUnusedBinlogFiles remove file failed", log.Err(err))
				unexpectedFailure.Inc()
				return struct{}{}, err
			}
			log.Info(ctx, "garbageCollector recycleUnusedBinlogFiles remove file success")
			removed.Inc()
			return struct{}{}, nil
		})
		futures = append(futures, future)
		return true
	})
	// Wait for all remove tasks done.
	if err := conc.BlockOnAll(futures...); err != nil {
		// error is logged, and can be ignored here.
		logger.Warn(ctx, "some task failure in remove object pool", log.Err(err))
	}

	cost := time.Since(start)
	logger.Info(ctx, "garbageCollector recycleUnusedBinlogFiles done",
		log.Int("total", total),
		log.Int("valid", valid),
		log.Int("unexpectedFailure", int(unexpectedFailure.Load())),
		log.Int("removed", int(removed.Load())),
		log.String("lastFilePath", lastFilePath),
		log.Duration("cost", cost),
		log.Err(err))

	metrics.GarbageCollectorFileScanDuration.
		WithLabelValues(paramtable.GetStringNodeID(), label).
		Observe(float64(cost.Milliseconds()))
}

func (gc *garbageCollector) checkDroppedSegmentGC(segment *SegmentInfo,
	childSegment *SegmentInfo,
	indexSet typeutil.UniqueSet,
	cpTimestamp Timestamp,
) bool {
	if !gc.isExpire(segment.GetDroppedAt()) {
		return false
	}
	isCompacted := childSegment != nil || segment.GetCompacted()
	if isCompacted {
		// For compact A, B -> C, don't GC A or B if C is not indexed,
		// guarantee replacing A, B with C won't downgrade performance
		// If the child is GC'ed first, then childSegment will be nil.
		if childSegment != nil && !indexSet.Contain(childSegment.GetID()) {
			log.RatedInfo(gc.ctx, log.RateDefault, "skipping GC when compact target segment is not indexed",
				log.Int64("child segment ID", childSegment.GetID()))
			return false
		}
	}

	segInsertChannel := segment.GetInsertChannel()
	// Ignore segments from potentially dropped collection. Check if collection is to be dropped by checking if channel is dropped.
	// We do this because collection meta drop relies on all segment being GCed.
	if gc.meta.catalog.ChannelExists(context.Background(), segInsertChannel) &&
		segment.GetDmlPosition().GetTimestamp() > cpTimestamp {
		// segment gc shall only happen when channel cp is after segment dml cp.
		log.RatedInfo(gc.ctx, log.RateDefault, "dropped segment dml position after channel cp, skip meta gc",
			log.Uint64("dmlPosTs", segment.GetDmlPosition().GetTimestamp()),
			log.Uint64("channelCpTs", cpTimestamp),
		)
		return false
	}
	return true
}

// recycleDroppedSegments scans all segments and remove those dropped segments from meta and oss.
func (gc *garbageCollector) recycleDroppedSegments(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log.Info(ctx, "start clear dropped segments...")
	defer func() {
		log.Info(ctx, "clear dropped segments done", log.Duration("timeCost", time.Since(start)))
	}()

	all := gc.meta.SelectSegments(ctx)
	drops := make(map[int64]*SegmentInfo, 0)
	compactTo := make(map[int64]*SegmentInfo)
	channels := typeutil.NewSet[string]()
	for _, segment := range all {
		if segment.GetState() == commonpb.SegmentState_Dropped {
			drops[segment.GetID()] = segment
			channels.Insert(segment.GetInsertChannel())
			// continue
			// A(indexed), B(indexed) -> C(no indexed), D(no indexed) -> E(no indexed), A, B can not be GC
		}
		for _, from := range segment.GetCompactionFrom() {
			compactTo[from] = segment
		}
	}

	droppedCompactTo := make(map[int64]*SegmentInfo)
	for id := range drops {
		if to, ok := compactTo[id]; ok {
			droppedCompactTo[to.GetID()] = to
		}
	}
	indexedSegments := FilterInIndexedSegments(ctx, gc.handler, gc.meta, false, lo.Values(droppedCompactTo)...)
	if ctx.Err() != nil {
		return
	}
	indexedSet := make(typeutil.UniqueSet)
	for _, segment := range indexedSegments {
		indexedSet.Insert(segment.GetID())
	}

	channelCPs := make(map[string]uint64)
	for channel := range channels {
		pos := gc.meta.GetChannelCheckpoint(channel)
		channelCPs[channel] = pos.GetTimestamp()
	}

	// try to get loaded segments
	loadedSegments := typeutil.NewSet[int64]()
	segments, err := gc.handler.ListLoadedSegments(ctx)
	if err != nil {
		log.Warn(ctx, "failed to get loaded segments", log.Err(err))
		return
	}
	for _, segmentID := range segments {
		loadedSegments.Insert(segmentID)
	}

	log.Info(ctx, "start to GC segments", log.Int("drop_num", len(drops)))
	for segmentID, segment := range drops {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}

		gc.ackSignal(signal)

		if gc.collectionGCPaused(segment.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", log.Int64("segmentID", segmentID), log.Int64("collectionID", segment.GetCollectionID()))
			continue
		}

		segInsertChannel := segment.GetInsertChannel()
		if loadedSegments.Contain(segmentID) {
			log.Info(ctx, "skip GC segment since it is loaded", log.Int64("segmentID", segmentID))
			continue
		}

		// Check if snapshot RefIndex is loaded before querying snapshot references
		// If not loaded, skip this segment and try again in next GC cycle
		snapshotMeta := gc.meta.GetSnapshotMeta()
		if snapshotMeta != nil {
			if !snapshotMeta.IsRefIndexLoadedForCollection(segment.GetCollectionID()) {
				log.Info(ctx, "skip GC segment since snapshot RefIndex is not loaded yet",
					log.Int64("collectionID", segment.GetCollectionID()))
				continue
			}

			if snapshotIDs := snapshotMeta.GetSnapshotBySegment(ctx, segment.GetCollectionID(), segmentID); len(snapshotIDs) > 0 {
				log.Info(ctx, "skip GC segment since it is referenced by snapshot",
					log.Int64("collectionID", segment.GetCollectionID()),
					log.Int64("partitionID", segment.GetPartitionID()),
					log.String("channel", segInsertChannel),
					log.Int64("segmentID", segmentID),
					log.Int64s("snapshotIDs", snapshotIDs))
				continue
			}
		}

		if !gc.checkDroppedSegmentGC(segment, compactTo[segment.GetID()], indexedSet, channelCPs[segInsertChannel]) {
			continue
		}

		cloned := segment.Clone()
		binlog.DecompressBinLogs(cloned.SegmentInfo)

		logs := getLogs(cloned)
		for key := range getTextLogs(cloned) {
			logs[key] = struct{}{}
		}

		for key := range getJSONKeyLogs(cloned, gc) {
			logs[key] = struct{}{}
		}

		log.Info(ctx, "GC segment start...", log.Int("insert_logs", len(cloned.GetBinlogs())),
			log.Int("delta_logs", len(cloned.GetDeltalogs())),
			log.Int("stats_logs", len(cloned.GetStatslogs())),
			log.Int("bm25_logs", len(cloned.GetBm25Statslogs())),
			log.Int("text_logs", len(cloned.GetTextStatsLogs())),
			log.Int("json_key_logs", len(cloned.GetJsonKeyStats())))
		if err := gc.removeObjectFiles(ctx, logs); err != nil {
			log.Warn(ctx, "GC segment remove logs failed", log.Err(err))
			cloned = nil
			continue
		}

		if err := gc.meta.DropSegment(ctx, cloned.GetID()); err != nil {
			log.Warn(ctx, "GC segment meta failed to drop segment", log.Err(err))
			cloned = nil
			continue
		}
		log.Info(ctx, "GC segment meta drop segment done")
		cloned = nil // release memory
	}
}

func (gc *garbageCollector) recycleChannelCPMeta(ctx context.Context, signal <-chan gcCmd) {
	channelCPs, err := gc.meta.catalog.ListChannelCheckpoint(ctx)
	if err != nil {
		log.Warn(ctx, "list channel cp fail during GC", log.Err(err))
		return
	}

	collectionID2GcStatus := make(map[int64]bool)
	skippedCnt := 0

	log.Info(ctx, "start to GC channel cp", log.Int("vchannelCPCnt", len(channelCPs)))
	for vChannel := range channelCPs {
		collectionID := funcutil.GetCollectionIDFromVChannel(vChannel)
		if gc.collectionGCPaused(collectionID) {
			continue
		}

		gc.ackSignal(signal)

		// !!! Skip to GC if vChannel format is illegal, it will lead meta leak in this case
		if collectionID == -1 {
			skippedCnt++
			log.Warn(ctx, "parse collection id fail, skip to gc channel cp", log.String("vchannel", vChannel))
			continue
		}

		_, ok := collectionID2GcStatus[collectionID]
		if !ok {
			if ctx.Err() != nil {
				// process canceled, stop.
				return
			}
			timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			has, err := gc.option.broker.HasCollection(timeoutCtx, collectionID)
			if err == nil && !has {
				collectionID2GcStatus[collectionID] = gc.meta.catalog.GcConfirm(ctx, collectionID, -1)
			} else {
				// skip checkpoints GC of this cycle if describe collection fails or the collection state is available.
				log.Debug(ctx, "skip channel cp GC, the collection state is available",
					log.Int64("collectionID", collectionID),
					log.Bool("dropped", has), log.Err(err))
				collectionID2GcStatus[collectionID] = false
			}
		}

		// Skip to GC if all segments meta of the corresponding collection are not removed
		if gcConfirmed, _ := collectionID2GcStatus[collectionID]; !gcConfirmed {
			skippedCnt++
			continue
		}

		err := gc.meta.DropChannelCheckpoint(vChannel)
		if err != nil {
			// Try to GC in the next gc cycle if drop channel cp meta fail.
			log.Warn(ctx, "failed to drop channelcp check point during gc", log.String("vchannel", vChannel), log.Err(err))
		} else {
			log.Info(ctx, "GC channel cp", log.String("vchannel", vChannel))
		}
	}

	log.Info(ctx, "GC channel cp done", log.Int("skippedChannelCP", skippedCnt))
}

func (gc *garbageCollector) isExpire(dropts Timestamp) bool {
	droptime := time.Unix(0, int64(dropts))
	return time.Since(droptime) > gc.option.dropTolerance
}

func getLogs(sinfo *SegmentInfo) map[string]struct{} {
	logs := make(map[string]struct{})
	for _, flog := range sinfo.GetBinlogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	for _, flog := range sinfo.GetStatslogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	for _, flog := range sinfo.GetDeltalogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	for _, flog := range sinfo.GetBm25Statslogs() {
		for _, l := range flog.GetBinlogs() {
			logs[l.GetLogPath()] = struct{}{}
		}
	}
	return logs
}

func getTextLogs(sinfo *SegmentInfo) map[string]struct{} {
	textLogs := make(map[string]struct{})
	for _, flog := range sinfo.GetTextStatsLogs() {
		for _, file := range flog.GetFiles() {
			textLogs[file] = struct{}{}
		}
	}

	return textLogs
}

func getJSONKeyLogs(sinfo *SegmentInfo, gc *garbageCollector) map[string]struct{} {
	jsonkeyLogs := make(map[string]struct{})
	for _, flog := range sinfo.GetJsonKeyStats() {
		for _, file := range flog.GetFiles() {
			prefix := fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", gc.option.cli.RootPath(), common.JSONIndexPath,
				flog.GetBuildID(), flog.GetVersion(), sinfo.GetCollectionID(), sinfo.GetPartitionID(), sinfo.GetID(), flog.GetFieldID())
			file = path.Join(prefix, file)
			jsonkeyLogs[file] = struct{}{}
		}
	}

	return jsonkeyLogs
}

// removeObjectFiles remove file from oss storage, return error if any log failed to remove.
func (gc *garbageCollector) removeObjectFiles(ctx context.Context, filePaths map[string]struct{}) error {
	futures := make([]*conc.Future[struct{}], 0)
	for filePath := range filePaths {
		filePath := filePath
		future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			err := gc.option.cli.Remove(ctx, filePath)
			// ignore the error Key Not Found
			if err != nil {
				if !errors.Is(err, merr.ErrIoKeyNotFound) {
					return struct{}{}, err
				}
				log.Info(ctx, "remove log failed, key not found, may be removed at previous GC, ignore the error",
					log.String("path", filePath),
					log.Err(err))
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	return conc.BlockOnAll(futures...)
}

// recycleUnusedIndexes is used to delete those indexes that is deleted by collection.
func (gc *garbageCollector) recycleUnusedIndexes(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log.Info(ctx, "start recycleUnusedIndexes...")
	defer func() {
		log.Info(ctx, "recycleUnusedIndexes done", log.Duration("timeCost", time.Since(start)))
	}()

	deletedIndexes := gc.meta.indexMeta.GetDeletedIndexes()
	for _, index := range deletedIndexes {
		if ctx.Err() != nil {
			// process canceled.
			return
		}
		if gc.collectionGCPaused(index.CollectionID) {
			continue
		}
		gc.ackSignal(signal)

		if err := gc.meta.indexMeta.RemoveIndex(ctx, index.CollectionID, index.IndexID); err != nil {
			log.Warn(ctx, "remove index on collection fail", log.Err(err))
			continue
		}
		log.Info(ctx, "remove index on collection done")
	}
}

// recycleUnusedSegIndexes remove the index of segment if index is deleted or segment itself is deleted.
func (gc *garbageCollector) recycleUnusedSegIndexes(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log.Info(ctx, "start recycleUnusedSegIndexes...")
	defer func() {
		log.Info(ctx, "recycleUnusedSegIndexes done", log.Duration("timeCost", time.Since(start)))
	}()

	segIndexes := gc.meta.indexMeta.GetAllSegIndexes()
	for _, segIdx := range segIndexes {
		if ctx.Err() != nil {
			// process canceled.
			return
		}
		if gc.collectionGCPaused(segIdx.CollectionID) {
			continue
		}
		gc.ackSignal(signal)

		// 1. segment belongs to is deleted.
		// 2. index is deleted.
		if gc.meta.GetSegment(ctx, segIdx.SegmentID) == nil || !gc.meta.indexMeta.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
			indexFiles := gc.getAllIndexFilesOfIndex(segIdx)
			// Check if snapshot RefIndex is loaded before querying snapshot references
			// If not loaded, skip this index and try again in next GC cycle
			snapshotMeta := gc.meta.GetSnapshotMeta()
			if snapshotMeta != nil {
				if !snapshotMeta.IsRefIndexLoadedForCollection(segIdx.CollectionID) {
					log.Info(ctx, "skip GC segment index since snapshot RefIndex is not loaded yet",
						log.Int64("collectionID", segIdx.CollectionID))
					continue
				}

				if snapshotIDs := snapshotMeta.GetSnapshotByIndex(ctx, segIdx.CollectionID, segIdx.IndexID); len(snapshotIDs) > 0 {
					log.Info(ctx, "skip GC segment index since it is referenced by snapshot",
						log.Int64s("snapshotIDs", snapshotIDs))
					continue
				}
			}

			log.Info(ctx, "GC Segment Index file start...")

			// Remove index files first.
			if err := gc.removeObjectFiles(ctx, indexFiles); err != nil {
				log.Warn(ctx, "fail to remove index files for index", log.Err(err))
				continue
			}

			// Remove meta from index meta.
			if err := gc.meta.indexMeta.RemoveSegmentIndex(ctx, segIdx.BuildID); err != nil {
				log.Warn(ctx, "delete index meta from etcd failed, wait to retry", log.Err(err))
				continue
			}
			log.Info(ctx, "index meta recycle success")
		}
	}
}

// recycleUnusedIndexFiles is used to delete those index files that no longer exist in the meta.
func (gc *garbageCollector) recycleUnusedIndexFiles(ctx context.Context) {
	log.Info(ctx, "start recycleUnusedIndexFiles...")

	prefix := path.Join(gc.option.cli.RootPath(), common.SegmentIndexPath) + "/"
	// list dir first
	keyCount := 0
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, false, func(indexPathInfo *storage.ChunkObjectInfo) bool {
		key := indexPathInfo.FilePath
		keyCount++
		logger := log.With(log.String("prefix", prefix), log.String("key", key))

		buildID, err := parseBuildIDFromFilePath(key)
		if err != nil {
			logger.Warn(ctx, "garbageCollector recycleUnusedIndexFiles parseIndexFileKey", log.Err(err))
			return true
		}
		logger = logger.With(log.Int64("buildID", buildID))
		logger.Info(ctx, "garbageCollector will recycle index files")
		canRecycle, segIdx := gc.meta.indexMeta.CheckCleanSegmentIndex(buildID)
		if !canRecycle {
			// Even if the index is marked as deleted, the index file will not be recycled, wait for the next gc,
			// and delete all index files about the buildID at one time.
			logger.Info(ctx, "garbageCollector can not recycle index files")
			return true
		}
		if segIdx == nil {
			// buildID no longer exists in meta, remove all index files
			logger.Info(ctx, "garbageCollector recycleUnusedIndexFiles find meta has not exist, remove index files")
			err = gc.option.cli.RemoveWithPrefix(ctx, key)
			if err != nil {
				logger.Warn(ctx, "garbageCollector recycleUnusedIndexFiles remove index files failed", log.Err(err))
				return true
			}
			logger.Info(ctx, "garbageCollector recycleUnusedIndexFiles remove index files success")
			return true
		}

		// Check if snapshot RefIndex is loaded before querying snapshot references
		// If not loaded, skip this index and try again in next GC cycle
		snapshotMeta := gc.meta.GetSnapshotMeta()
		if snapshotMeta != nil {
			if !snapshotMeta.IsRefIndexLoadedForCollection(segIdx.CollectionID) {
				logger.Info(ctx, "skip GC index files since snapshot RefIndex is not loaded yet",
					log.Int64("collectionID", segIdx.CollectionID))
				return true
			}

			// Check if this index is referenced by any snapshot
			// If snapshots reference this index, do not delete the index files
			if snapshotIDs := snapshotMeta.GetSnapshotByIndex(ctx, segIdx.CollectionID, segIdx.IndexID); len(snapshotIDs) > 0 {
				logger.Info(ctx, "skip GC index files since index is referenced by snapshot",
					log.Int64s("snapshotIDs", snapshotIDs))
				return true
			}
		}

		filesMap := gc.getAllIndexFilesOfIndex(segIdx)

		logger.Info(ctx, "recycle index files", log.Int("meta files num", len(filesMap)))
		deletedFilesNum := atomic.NewInt32(0)
		fileNum := 0

		futures := make([]*conc.Future[struct{}], 0)
		err = gc.option.cli.WalkWithPrefix(ctx, key, true, func(indexFile *storage.ChunkObjectInfo) bool {
			fileNum++
			file := indexFile.FilePath
			if _, ok := filesMap[file]; !ok {
				future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
					logger := logger.With(log.String("file", file))
					logger.Info(ctx, "garbageCollector recycleUnusedIndexFiles remove file...")

					if err := gc.option.cli.Remove(ctx, file); err != nil {
						logger.Warn(ctx, "garbageCollector recycleUnusedIndexFiles remove file failed", log.Err(err))
						return struct{}{}, err
					}
					deletedFilesNum.Inc()
					logger.Info(ctx, "garbageCollector recycleUnusedIndexFiles remove file success")
					return struct{}{}, nil
				})
				futures = append(futures, future)
			}
			return true
		})
		// Wait for all remove tasks done.
		if err := conc.BlockOnAll(futures...); err != nil {
			// error is logged, and can be ignored here.
			logger.Warn(ctx, "some task failure in remove object pool", log.Err(err))
		}

		logger = logger.With(log.Int("deleteIndexFilesNum", int(deletedFilesNum.Load())), log.Int("walkFileNum", fileNum))
		if err != nil {
			logger.Warn(ctx, "index files recycle failed when walk with prefix", log.Err(err))
			return true
		}
		logger.Info(ctx, "index files recycle done")
		return true
	})
	if err != nil {
		log.Warn(ctx, "garbageCollector recycleUnusedIndexFiles failed", log.Err(err))
		return
	}
	log.Info(ctx, "recycleUnusedIndexFiles done")
}

// getAllIndexFilesOfIndex returns the all index files of index.
func (gc *garbageCollector) getAllIndexFilesOfIndex(segmentIndex *model.SegmentIndex) map[string]struct{} {
	filesMap := make(map[string]struct{})
	for _, fileID := range segmentIndex.IndexFileKeys {
		filepath := metautil.BuildSegmentIndexFilePath(gc.option.cli.RootPath(), segmentIndex.BuildID, segmentIndex.IndexVersion,
			segmentIndex.PartitionID, segmentIndex.SegmentID, fileID)
		filesMap[filepath] = struct{}{}
	}
	return filesMap
}

// recycleUnusedAnalyzeFiles is used to delete those analyze stats files that no longer exist in the meta.
func (gc *garbageCollector) recycleUnusedAnalyzeFiles(ctx context.Context, signal <-chan gcCmd) {
	log.Info(ctx, "start recycleUnusedAnalyzeFiles")
	startTs := time.Now()
	prefix := path.Join(gc.option.cli.RootPath(), common.AnalyzeStatsPath) + "/"
	// list dir first
	keys := make([]string, 0)
	err := gc.option.cli.WalkWithPrefix(ctx, prefix, false, func(chunkInfo *storage.ChunkObjectInfo) bool {
		keys = append(keys, chunkInfo.FilePath)
		return true
	})
	if err != nil {
		log.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles list keys from chunk manager failed", log.Err(err))
		return
	}
	log.Info(ctx, "recycleUnusedAnalyzeFiles, finish list object", log.Duration("time spent", time.Since(startTs)), log.Int("task ids", len(keys)))
	for _, key := range keys {
		if ctx.Err() != nil {
			// process canceled
			return
		}
		// collection gc pause not affect analyze file for now
		gc.ackSignal(signal)

		log.Debug(ctx, "analyze keys", log.String("key", key))
		taskID, err := parseBuildIDFromFilePath(key)
		if err != nil {
			log.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles parseAnalyzeResult failed", log.String("key", key), log.Err(err))
			continue
		}
		log.Info(ctx, "garbageCollector will recycle analyze stats files", log.Int64("taskID", taskID))
		canRecycle, task := gc.meta.analyzeMeta.CheckCleanAnalyzeTask(taskID)
		if !canRecycle {
			// Even if the analysis task is marked as deleted, the analysis stats file will not be recycled, wait for the next gc,
			// and delete all index files about the taskID at one time.
			log.Info(ctx, "garbageCollector no need to recycle analyze stats files", log.Int64("taskID", taskID))
			continue
		}
		if task == nil {
			// taskID no longer exists in meta, remove all analysis files
			log.Info(ctx, "garbageCollector recycleUnusedAnalyzeFiles find meta has not exist, remove index files",
				log.Int64("taskID", taskID))
			err = gc.option.cli.RemoveWithPrefix(ctx, key)
			if err != nil {
				log.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles remove analyze stats files failed",
					log.Int64("taskID", taskID), log.String("prefix", key), log.Err(err))
				continue
			}
			log.Info(ctx, "garbageCollector recycleUnusedAnalyzeFiles remove analyze stats files success",
				log.Int64("taskID", taskID), log.String("prefix", key))
			continue
		}

		log.Info(ctx, "remove analyze stats files which version is less than current task",
			log.Int64("taskID", taskID), log.Int64("current version", task.Version))
		var i int64
		for i = 0; i < task.Version; i++ {
			if ctx.Err() != nil {
				// process canceled.
				return
			}
			removePrefix := prefix + fmt.Sprintf("%d/", task.Version)
			if err := gc.option.cli.RemoveWithPrefix(ctx, removePrefix); err != nil {
				log.Warn(ctx, "garbageCollector recycleUnusedAnalyzeFiles remove files with prefix failed",
					log.Int64("taskID", taskID), log.String("removePrefix", removePrefix))
				continue
			}
		}
		log.Info(ctx, "analyze stats files recycle success", log.Int64("taskID", taskID))
	}
}

// recycleUnusedTextIndexFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedTextIndexFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log.Info(ctx, "start recycleUnusedTextIndexFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedTextIndexFiles done", log.Duration("timeCost", time.Since(start)))
	}()

	hasTextIndexSegments := gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return len(info.GetTextStatsLogs()) != 0
	}))
	deletedFilesNum := atomic.NewInt32(0)

	for _, seg := range hasTextIndexSegments {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}
		if gc.collectionGCPaused(seg.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", log.Int64("segmentID", seg.GetID()), log.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		// Check if segment is referenced by any snapshot before deleting text index files
		snapshotMeta := gc.meta.GetSnapshotMeta()
		if snapshotMeta != nil {
			// If RefIndex is not loaded yet, skip to avoid incorrectly deleting snapshot-referenced files
			if !snapshotMeta.IsRefIndexLoadedForCollection(seg.GetCollectionID()) {
				log.Info(ctx, "skip GC text index files since snapshot RefIndex is not loaded yet",
					log.Int64("segmentID", seg.GetID()),
					log.Int64("collectionID", seg.GetCollectionID()))
				continue
			}
			if snapshotIDs := snapshotMeta.GetSnapshotBySegment(ctx, seg.GetCollectionID(), seg.GetID()); len(snapshotIDs) > 0 {
				log.Info(ctx, "skip GC text index files since segment is referenced by snapshot",
					log.Int64("segmentID", seg.GetID()),
					log.Int64s("snapshotIDs", snapshotIDs))
				continue
			}
		}

		gc.ackSignal(signal)
		for _, fieldStats := range seg.GetTextStatsLogs() {
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", gc.option.cli.RootPath(), common.TextIndexPath,
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log.Info(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file failed", log.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedTextIndexFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", log.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "text index files recycle failed when walk with prefix", log.Err(err))
					return
				}
			}
		}
	}
	log.Info(ctx, "text index files recycle done")

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedJSONStatsFiles load meta file info and compares OSS keys
// if missing found, performs gc cleanup
func (gc *garbageCollector) recycleUnusedJSONStatsFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log.Info(ctx, "start recycleUnusedJSONStatsFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedJSONStatsFiles done", log.Duration("timeCost", time.Since(start)))
	}()

	hasJSONStatsSegments := gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return len(info.GetJsonKeyStats()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)

	for _, seg := range hasJSONStatsSegments {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}
		if gc.collectionGCPaused(seg.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", log.Int64("segmentID", seg.GetID()), log.Int64("collectionID", seg.GetCollectionID()))
			continue
		}
		gc.ackSignal(signal)
		for _, fieldStats := range seg.GetJsonKeyStats() {
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d/%d", gc.option.cli.RootPath(), common.JSONStatsPath, fieldStats.GetJsonKeyStatsDataFormat(),
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file failed", log.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", log.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "json stats files recycle failed when walk with prefix", log.Err(err))
					return
				}
			}

			// clear low data format version stats file
			// for upgrade from old version to new version, we need to clear the old data format version stats file
			for i := int64(1); i < fieldStats.GetJsonKeyStatsDataFormat(); i++ {
				prefix := fmt.Sprintf("%s/%s/%d", gc.option.cli.RootPath(), common.JSONStatsPath, i)
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file failed", log.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedJSONStatsFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", log.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "json stats lower data format files recycle failed when walk with prefix", log.Err(err))
					return
				}
			}
		}
	}
	log.Info(ctx, "json stats files recycle done",
		log.Int("deleteJSONStatsNum", int(deletedFilesNum.Load())),
		log.Int("walkFileNum", fileNum))

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recycleUnusedJSONIndexFiles load meta file info and compares OSS keys
func (gc *garbageCollector) recycleUnusedJSONIndexFiles(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log.Info(ctx, "start recycleUnusedJSONIndexFiles...")
	defer func() {
		log.Info(ctx, "recycleUnusedJSONIndexFiles done", log.Duration("timeCost", time.Since(start)))
	}()

	hasJSONIndexSegments := gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return len(info.GetJsonKeyStats()) != 0
	}))
	fileNum := 0
	deletedFilesNum := atomic.NewInt32(0)

	for _, seg := range hasJSONIndexSegments {
		if ctx.Err() != nil {
			// process canceled, stop.
			return
		}
		if gc.collectionGCPaused(seg.GetCollectionID()) {
			log.Info(ctx, "skip GC segment since collection is paused", log.Int64("segmentID", seg.GetID()), log.Int64("collectionID", seg.GetCollectionID()))
			continue
		}

		// Check if segment is referenced by any snapshot before deleting JSON index files
		snapshotMeta := gc.meta.GetSnapshotMeta()
		if snapshotMeta != nil {
			// If RefIndex is not loaded yet, skip to avoid incorrectly deleting snapshot-referenced files
			if !snapshotMeta.IsRefIndexLoadedForCollection(seg.GetCollectionID()) {
				log.Info(ctx, "skip GC JSON index files since snapshot RefIndex is not loaded yet",
					log.Int64("segmentID", seg.GetID()),
					log.Int64("collectionID", seg.GetCollectionID()))
				continue
			}
			if snapshotIDs := snapshotMeta.GetSnapshotBySegment(ctx, seg.GetCollectionID(), seg.GetID()); len(snapshotIDs) > 0 {
				log.Info(ctx, "skip GC JSON index files since segment is referenced by snapshot",
					log.Int64("segmentID", seg.GetID()),
					log.Int64s("snapshotIDs", snapshotIDs))
				continue
			}
		}

		gc.ackSignal(signal)
		for _, fieldStats := range seg.GetJsonKeyStats() {
			// clear low version task
			for i := int64(1); i < fieldStats.GetVersion(); i++ {
				prefix := fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", gc.option.cli.RootPath(), common.JSONIndexPath,
					fieldStats.GetBuildID(), i, seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID(), fieldStats.GetFieldID())
				futures := make([]*conc.Future[struct{}], 0)

				err := gc.option.cli.WalkWithPrefix(ctx, prefix, true, func(files *storage.ChunkObjectInfo) bool {
					file := files.FilePath

					future := gc.option.removeObjectPool.Submit(func() (struct{}, error) {
						log.Info(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file...")

						if err := gc.option.cli.Remove(ctx, file); err != nil {
							log.Warn(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file failed", log.Err(err))
							return struct{}{}, err
						}
						deletedFilesNum.Inc()
						log.Info(ctx, "garbageCollector recycleUnusedJSONIndexFiles remove file success")
						return struct{}{}, nil
					})
					futures = append(futures, future)
					return true
				})

				// Wait for all remove tasks done.
				if err := conc.BlockOnAll(futures...); err != nil {
					// error is logged, and can be ignored here.
					log.Warn(ctx, "some task failure in remove object pool", log.Err(err))
				}

				if err != nil {
					log.Warn(ctx, "json index files recycle failed when walk with prefix", log.Err(err))
					return
				}
			}
		}
	}
	log.Info(ctx, "json index files recycle done", log.Int("deleteJSONKeyIndexNum", int(deletedFilesNum.Load())), log.Int("walkFileNum", fileNum))

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}

// recyclePendingSnapshots cleans up orphaned snapshot files from failed 2PC commits.
// This method scans etcd for PENDING snapshots that have exceeded the timeout,
// computes their S3 directory/file paths from snapshot ID, and cleans up using RemoveWithPrefix.
//
// Key design decisions:
//   - NO S3 list operations: Uses RemoveWithPrefix for directory cleanup
//   - File paths computed from collection_id + snapshot_id stored in etcd
//   - Timeout mechanism prevents cleanup of snapshots still being created
//
// Process flow:
//  1. Get all PENDING snapshots from catalog that have exceeded timeout.
//  2. For each pending snapshot:
//     a. Compute manifest directory and metadata file path from snapshot ID.
//     b. Delete manifest directory using RemoveWithPrefix.
//     c. Delete metadata file.
//     d. Delete catalog (etcd) record.
//
// Failure handling:
//   - For PENDING snapshots, if any S3 cleanup step fails (b/c), GC will NOT
//     delete the catalog record. This keeps the snapshot eligible for retry in
//     the next GC cycle, ensuring we do not lose the ability to clean up S3
//     artifacts.
func (gc *garbageCollector) recyclePendingSnapshots(ctx context.Context, signal <-chan gcCmd) {
	start := time.Now()
	log.Info(ctx, "start recyclePendingSnapshots...")
	defer func() {
		log.Info(ctx, "recyclePendingSnapshots done", log.Duration("timeCost", time.Since(start)))
	}()

	snapshotMeta := gc.meta.GetSnapshotMeta()
	if snapshotMeta == nil {
		log.Warn(ctx, "snapshotMeta is nil, skip recyclePendingSnapshots")
		return
	}

	// Get pending timeout from config
	pendingTimeout := paramtable.Get().DataCoordCfg.SnapshotPendingTimeout.GetAsDuration(time.Minute)

	// Get all pending snapshots that have exceeded timeout
	pendingSnapshots, err := snapshotMeta.GetPendingSnapshots(ctx, pendingTimeout)
	if err != nil {
		log.Warn(ctx, "failed to get pending snapshots", log.Err(err))
		return
	}

	if len(pendingSnapshots) == 0 {
		return
	}

	log.Info(ctx, "found pending snapshots to cleanup", log.Int("count", len(pendingSnapshots)))
	cleanedCount := 0

	for _, snapshot := range pendingSnapshots {
		snapshotLog := log.With(
			log.String("snapshotName", snapshot.GetName()),
			log.Int64("snapshotID", snapshot.GetId()),
			log.Int64("collectionID", snapshot.GetCollectionId()),
		)

		gc.ackSignal(signal)
		// Compute paths from collection_id + snapshot_id
		manifestDir, metadataPath := GetSnapshotPaths(
			gc.option.cli.RootPath(),
			snapshot.GetCollectionId(),
			snapshot.GetId(),
		)

		snapshotLog.Info(ctx, "cleaning up pending snapshot",
			log.String("manifestDir", manifestDir),
			log.String("metadataPath", metadataPath))

		// Delete manifest directory using RemoveWithPrefix (no list needed)
		// This removes all segment manifest files: manifests/{snapshot_id}/*.avro
		if err := gc.option.cli.RemoveWithPrefix(ctx, manifestDir); err != nil {
			snapshotLog.Warn(ctx, "failed to remove pending snapshot manifest directory", log.Err(err))
			// Keep catalog record for retry in next GC cycle.
			continue
		}

		// Delete metadata file
		if err := gc.option.cli.Remove(ctx, metadataPath); err != nil {
			snapshotLog.Warn(ctx, "failed to remove pending snapshot metadata file", log.Err(err))
			// Keep catalog record for retry in next GC cycle.
			continue
		}

		// Delete etcd record
		if err := snapshotMeta.CleanupPendingSnapshot(ctx, snapshot); err != nil {
			snapshotLog.Warn(ctx, "failed to drop pending snapshot from catalog", log.Err(err))
			continue
		}

		snapshotLog.Info(ctx, "successfully cleaned up pending snapshot")
		cleanedCount++
	}

	log.Info(ctx, "pending snapshots cleanup completed",
		log.Int("totalPending", len(pendingSnapshots)),
		log.Int("cleanedCount", cleanedCount))

	// Clean up DELETING snapshots (two-phase delete cleanup)
	// These are snapshots that were marked for deletion but S3 cleanup failed
	deletingSnapshots, err := snapshotMeta.GetDeletingSnapshots(ctx)
	if err != nil {
		log.Warn(ctx, "failed to get deleting snapshots", log.Err(err))
	} else if len(deletingSnapshots) > 0 {
		log.Info(ctx, "found deleting snapshots to cleanup", log.Int("count", len(deletingSnapshots)))
		deletingCleanedCount := 0

		for _, snapshot := range deletingSnapshots {
			snapshotLog := log.With(
				log.String("snapshotName", snapshot.GetName()),
				log.Int64("snapshotID", snapshot.GetId()),
				log.Int64("collectionID", snapshot.GetCollectionId()),
			)

			gc.ackSignal(signal)

			// Compute paths from collection_id + snapshot_id
			manifestDir, metadataPath := GetSnapshotPaths(
				gc.option.cli.RootPath(),
				snapshot.GetCollectionId(),
				snapshot.GetId(),
			)

			snapshotLog.Info(ctx, "cleaning up deleting snapshot",
				log.String("manifestDir", manifestDir),
				log.String("metadataPath", metadataPath))

			// Delete manifest directory
			if err := gc.option.cli.RemoveWithPrefix(ctx, manifestDir); err != nil {
				snapshotLog.Warn(ctx, "failed to remove deleting snapshot manifest directory", log.Err(err))
				// Continue with metadata and etcd cleanup even if S3 cleanup fails
			}

			// Delete metadata file
			if err := gc.option.cli.Remove(ctx, metadataPath); err != nil {
				snapshotLog.Warn(ctx, "failed to remove deleting snapshot metadata file", log.Err(err))
				// Continue with etcd cleanup even if S3 cleanup fails
			}

			// Delete etcd record
			if err := snapshotMeta.CleanupDeletingSnapshot(ctx, snapshot); err != nil {
				snapshotLog.Warn(ctx, "failed to drop deleting snapshot from catalog", log.Err(err))
				continue
			}

			snapshotLog.Info(ctx, "successfully cleaned up deleting snapshot")
			deletingCleanedCount++
		}

		log.Info(ctx, "deleting snapshots cleanup completed",
			log.Int("totalDeleting", len(deletingSnapshots)),
			log.Int("cleanedCount", deletingCleanedCount))
	}

	metrics.GarbageCollectorRunCount.WithLabelValues(paramtable.GetStringNodeID()).Add(1)
}
