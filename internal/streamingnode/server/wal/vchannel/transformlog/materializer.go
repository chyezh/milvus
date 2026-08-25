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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	defaultMaterializeMaxRows  = 500000
	defaultMaterializeMaxBytes = 32 * 1024 * 1024
)

type Materializer interface {
	Materialize(context.Context, MaterializeRequest) error
}

// PendingL0Recorder durably records a materialized L0 segment whose DataCoord
// registration is still in flight (outbox pattern). The binlog objects are
// already durable when Record is called; a crash before the registration
// completes is recovered by replaying the recorded registration, so the
// delete data is neither lost nor duplicated. The record must be removed only
// after the vchannel meta carrying the materialization frontier is persisted;
// the recovery flow removes it after the (idempotent) re-registration.
type PendingL0Recorder interface {
	// Record persists the registration request of the materialized segment.
	Record(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error
	// Remove drops the pending registration of the segment. It is used to
	// roll back the outbox record of a failed registration whose retry
	// re-materializes the batch into a fresh segment; a leaked record only
	// re-registers a fully written segment on recovery (idempotent).
	Remove(ctx context.Context, segmentID int64) error
}

type MaterializeRequest struct {
	VChannel       string
	TargetTimeTick uint64
	Entries        []*streamingpb.TransformLogEntry
	MaxRows        uint64
	MaxBytes       uint64
}

type SyncMaterializer struct {
	chunkManager storage.ChunkManager
	allocator    allocator.Interface
	metaWriter   syncmgr.MetaWriter
	nodeID       int64
	// pendingL0Recorder records the outbox entries between the object write
	// and the DataCoord registration; nil disables the outbox.
	pendingL0Recorder PendingL0Recorder
}

func NewSyncMaterializer(
	chunkManager storage.ChunkManager,
	allocator allocator.Interface,
	metaWriter syncmgr.MetaWriter,
	nodeID int64,
) *SyncMaterializer {
	return &SyncMaterializer{
		chunkManager: chunkManager,
		allocator:    allocator,
		metaWriter:   metaWriter,
		nodeID:       nodeID,
	}
}

// WithPendingL0Recorder installs the outbox recorder used between the object
// write and the DataCoord registration of every materialized L0 segment.
func (m *SyncMaterializer) WithPendingL0Recorder(recorder PendingL0Recorder) *SyncMaterializer {
	m.pendingL0Recorder = recorder
	return m
}

func (m *SyncMaterializer) Materialize(ctx context.Context, req MaterializeRequest) error {
	if len(req.Entries) == 0 {
		return nil
	}
	if m.chunkManager == nil {
		return merr.WrapErrServiceInternalMsg("chunk manager is nil")
	}
	if m.allocator == nil {
		return merr.WrapErrServiceInternalMsg("id allocator is nil")
	}
	if m.metaWriter == nil {
		return merr.WrapErrServiceInternalMsg("meta writer is nil")
	}
	collectionID := funcutil.GetCollectionIDFromVChannel(req.VChannel)
	if collectionID <= 0 {
		return merr.WrapErrServiceInternalMsg("invalid vchannel %q for transform log materialization", req.VChannel)
	}
	for _, group := range splitMaterializeGroups(req) {
		if err := m.materializeGroup(ctx, req.VChannel, collectionID, req.TargetTimeTick, group); err != nil {
			return err
		}
	}
	return nil
}

func (m *SyncMaterializer) materializeGroup(
	ctx context.Context,
	vchannel string,
	collectionID int64,
	targetTimeTick uint64,
	group materializeGroup,
) error {
	segmentID, err := m.allocator.AllocOne()
	if err != nil {
		return err
	}
	startPosition := &msgpb.MsgPosition{ChannelName: vchannel, Timestamp: group.fromTimeTick}
	checkpoint := &msgpb.MsgPosition{ChannelName: vchannel, Timestamp: targetTimeTick}
	schema := materializeSchema(group.pkType)
	metaCache := newMaterializeMetaCache(collectionID, vchannel, schema)
	metaCache.AddSegment(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    group.partitionID,
		InsertChannel:  vchannel,
		StartPosition:  startPosition,
		State:          commonpb.SegmentState_Growing,
		Level:          datapb.SegmentLevel_L0,
		StorageVersion: storage.StorageV2,
	}, func(_ *datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, metacache.NoneBm25StatsFactory, metacache.SetStartPosRecorded(false))

	pack := (&syncmgr.SyncPack{}).
		WithDeleteData(storage.NewDeleteData(group.pks, group.timestamps)).
		WithCollectionID(collectionID).
		WithPartitionID(group.partitionID).
		WithChannelName(vchannel).
		WithSegmentID(segmentID).
		WithStartPosition(startPosition).
		WithCheckpoint(checkpoint).
		WithTimeRange(group.fromTimeTick, group.toTimeTick).
		WithLevel(datapb.SegmentLevel_L0).
		WithDataSource(metrics.StreamingDataSourceLabel).
		WithBatchRows(0).
		WithFlush()

	task := syncmgr.NewSyncTask().
		WithAllocator(m.allocator).
		WithChunkManager(m.chunkManager).
		WithMetaWriter(m.metaWriter).
		WithMetaCache(metaCache).
		WithSchema(schema).
		WithSyncPack(pack).
		WithStorageConfig(packed.CreateStorageConfig())

	mlog.Info(ctx, "materialize transform log into l0 segment",
		mlog.FieldCollectionID(collectionID),
		mlog.FieldPartitionID(group.partitionID),
		mlog.FieldSegmentID(segmentID),
		mlog.String("vchannel", vchannel),
		mlog.Uint64("targetTimeTick", targetTimeTick),
		mlog.Int64("rows", int64(len(group.pks))),
		mlog.Uint64("bytes", group.bytes),
	)

	// Step 1: write the binlog objects. The delete data becomes durable here.
	if err := task.Write(ctx); err != nil {
		return err
	}
	// Step 2: durably record the pending registration (outbox) before the
	// DataCoord registration. A crash between step 2 and step 3 leaves the
	// record behind; recovery replays it (DataCoord applies the request as a
	// full replacement, so the replay is idempotent), which guarantees the
	// delete data is neither lost nor duplicated across the crash window.
	if m.pendingL0Recorder != nil {
		req := task.SaveBinlogPathsRequest(m.nodeID)
		if req == nil {
			return merr.WrapErrSegmentNotFound(segmentID)
		}
		if err := m.pendingL0Recorder.Record(ctx, req); err != nil {
			return err
		}
	}
	// Step 3: register the segment at DataCoord. The registration is a full
	// replacement, so re-running it (recovery replay) is idempotent.
	if err := task.WriteMeta(ctx); err != nil {
		// The registration did not complete; roll the outbox record back so
		// the retry's fresh segment does not resurrect this one on recovery.
		// Best-effort: a leaked record only re-registers a fully written
		// segment (idempotent, no data loss).
		if m.pendingL0Recorder != nil {
			_ = m.pendingL0Recorder.Remove(ctx, segmentID)
		}
		return err
	}
	return nil
}

type materializeGroup struct {
	partitionID  int64
	pkType       schemapb.DataType
	pks          []storage.PrimaryKey
	timestamps   []uint64
	fromTimeTick uint64
	toTimeTick   uint64
	bytes        uint64
}

func splitMaterializeGroups(req MaterializeRequest) []materializeGroup {
	maxRows := req.MaxRows
	if maxRows == 0 {
		maxRows = defaultMaterializeMaxRows
	}
	maxBytes := req.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaterializeMaxBytes
	}
	groups := make([]materializeGroup, 0)
	currentByPartition := make(map[int64]*materializeGroup)
	flushCurrent := func(partitionID int64) {
		group := currentByPartition[partitionID]
		if group == nil || len(group.pks) == 0 {
			return
		}
		groups = append(groups, *group)
		delete(currentByPartition, partitionID)
	}
	for _, entry := range req.Entries {
		entryBytes := uint64(proto.Size(entry))
		for _, block := range entry.GetDelete().GetBlocks() {
			pks := storage.ParseIDs2PrimaryKeys(block.GetPrimaryKeys())
			if len(pks) == 0 {
				continue
			}
			pkBytes := entryBytes / uint64(len(pks))
			if pkBytes == 0 {
				pkBytes = 1
			}
			partitionID := block.GetPartitionId()
			for _, pk := range pks {
				group := currentByPartition[partitionID]
				if group != nil && (group.pkType != pk.Type() || exceedsMaterializeLimit(group, 1, pkBytes, maxRows, maxBytes)) {
					flushCurrent(partitionID)
					group = nil
				}
				if group == nil {
					group = &materializeGroup{
						partitionID:  partitionID,
						pkType:       pk.Type(),
						fromTimeTick: entry.GetTimeTick(),
					}
					currentByPartition[partitionID] = group
				}
				group.pks = append(group.pks, pk)
				group.timestamps = append(group.timestamps, entry.GetTimeTick())
				group.toTimeTick = entry.GetTimeTick()
				group.bytes += pkBytes
			}
		}
	}
	for partitionID := range currentByPartition {
		flushCurrent(partitionID)
	}
	return groups
}

func exceedsMaterializeLimit(group *materializeGroup, rows uint64, bytes uint64, maxRows uint64, maxBytes uint64) bool {
	if len(group.pks) == 0 {
		return false
	}
	if maxRows > 0 && uint64(len(group.pks))+rows > maxRows {
		return true
	}
	return maxBytes > 0 && group.bytes+bytes > maxBytes
}

func materializeSchema(pkType schemapb.DataType) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "transformlog_pk",
				DataType:     pkType,
				IsPrimaryKey: true,
			},
		},
	}
}

func newMaterializeMetaCache(collectionID int64, vchannel string, schema *schemapb.CollectionSchema) metacache.MetaCache {
	return metacache.NewMetaCache(&datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  vchannel,
		},
		Schema: schema,
	}, func(_ *datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, metacache.NoneBm25StatsFactory)
}

func transformLogEntryRows(entry *streamingpb.TransformLogEntry) uint64 {
	var rows uint64
	for _, block := range entry.GetDelete().GetBlocks() {
		rows += uint64(len(storage.ParseIDs2PrimaryKeys(block.GetPrimaryKeys())))
	}
	return rows
}
