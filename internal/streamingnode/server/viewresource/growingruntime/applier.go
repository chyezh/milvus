package growingruntime

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type segcoreApplier struct {
	mu              sync.Mutex
	closeOnce       sync.Once
	collection      *segcore.CCollection
	segments        map[int64]segcore.CSegment
	flushedSegments map[int64]struct{}
}

func newSegcoreApplier(ctx context.Context, desc Descriptor) (Applier, error) {
	if desc.Schema() == nil {
		return NoopApplier{}, nil
	}
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID:  desc.CollectionID(),
		Schema:        desc.Schema(),
		LoadFieldList: desc.Settings().GetRequiredFields(),
	})
	if err != nil {
		return nil, err
	}
	applier := &segcoreApplier{
		collection:      collection,
		segments:        make(map[int64]segcore.CSegment),
		flushedSegments: make(map[int64]struct{}),
	}
	for _, segment := range desc.WALView.SegmentSnapshot.Segments {
		if segment.Data.PersistedStorage == nil {
			continue
		}
		if err := applier.LoadPersistedSegment(ctx, segment); err != nil {
			applier.Close()
			return nil, err
		}
	}
	return applier, nil
}

func (a *segcoreApplier) LoadPersistedSegment(ctx context.Context, segment walview.VisibleSegment) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.segments[segment.SegmentID]; ok {
		return nil
	}
	csegment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  a.collection,
		SegmentID:   segment.SegmentID,
		SegmentType: segcore.SegmentTypeGrowing,
		LoadInfo:    loadInfoFromVisibleSegment(segment),
	})
	if err != nil {
		return err
	}
	a.segments[segment.SegmentID] = csegment
	return csegment.Load(ctx)
}

func (a *segcoreApplier) ApplySnapshotInsert(ctx context.Context, segment walview.VisibleSegment, msg message.ImmutableMessage) error {
	return a.applyInsertMessage(ctx, segment.SegmentID, msg)
}

func (a *segcoreApplier) ApplyDeleteReplay(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
	return a.applyTransformLogEntry(ctx, entry)
}

func (a *segcoreApplier) ApplyLiveMessage(ctx context.Context, msg message.ImmutableMessage) error {
	if msg == nil {
		return nil
	}
	switch msg.MessageType() {
	case message.MessageTypeCreateSegment:
		created := message.MustAsImmutableCreateSegmentMessageV2(msg)
		_, err := a.getOrCreateSegment(created.Header().GetSegmentId())
		return err
	case message.MessageTypeInsert:
		return a.applyInsertMessage(ctx, 0, msg)
	case message.MessageTypeTxn:
		if err := a.applyInsertMessage(ctx, 0, msg); err != nil {
			return err
		}
		return a.applyLiveDeleteMessage(ctx, msg)
	case message.MessageTypeDelete:
		return a.applyLiveDeleteMessage(ctx, msg)
	case message.MessageTypeFlush:
		a.markSegmentFlushed(message.MustAsImmutableFlushMessageV2(msg).Header().GetSegmentId())
		return nil
	default:
		return nil
	}
}

func (a *segcoreApplier) Close() {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		for _, segment := range a.segments {
			segment.Release()
		}
		a.segments = nil
		a.flushedSegments = nil
		if a.collection != nil {
			a.collection.Release()
			a.collection = nil
		}
	})
}

func (a *segcoreApplier) applyInsertMessage(ctx context.Context, segmentID int64, raw message.ImmutableMessage) error {
	if raw == nil {
		return nil
	}
	if segmentID != 0 {
		return walview.ForEachSegmentInsertMessage(raw, segmentID, func(insert walview.SegmentInsertMessage) error {
			return a.insert(ctx, insert)
		})
	}
	return walview.ForEachSegmentInsertMessage(raw, 0, func(insert walview.SegmentInsertMessage) error {
		return a.insert(ctx, insert)
	})
}

func (a *segcoreApplier) insert(ctx context.Context, insert walview.SegmentInsertMessage) error {
	body := insert.Message.MustBody()
	if body == nil {
		return errors.New("growing insert message has nil request")
	}
	request := proto.Clone(body).(*msgpb.InsertRequest)
	segmentID := insert.Assignment.GetSegmentAssignment().GetSegmentId()
	request.PartitionID = insert.Assignment.GetPartitionId()
	request.SegmentID = segmentID
	a.mu.Lock()
	flushed := a.segmentFlushedLocked(segmentID)
	a.mu.Unlock()
	if flushed {
		return errors.Errorf("growing segment %d already flushed", segmentID)
	}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: insert.TimeTick,
			EndTimestamp:   insert.TimeTick,
		},
		InsertRequest: request,
	}
	record, err := storage.TransferInsertMsgToInsertRecord(a.collection.Schema(), insertMsg)
	if err != nil {
		return err
	}
	segment, err := a.getOrCreateSegment(segmentID)
	if err != nil {
		return err
	}
	_, err = segment.Insert(ctx, &segcore.InsertRequest{
		RowIDs:     request.GetRowIDs(),
		Timestamps: request.GetTimestamps(),
		Record:     record,
	})
	return err
}

func (a *segcoreApplier) applyLiveDeleteMessage(ctx context.Context, msg message.ImmutableMessage) error {
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		deleted := message.MustAsImmutableDeleteMessageV1(msg)
		return a.applyDeleteRequest(ctx, msg.TimeTick(), deleted.MustBody())
	case message.MessageTypeTxn:
		txn := message.AsImmutableTxnMessage(msg)
		if txn == nil {
			return errors.New("invalid txn WAL message")
		}
		return txn.RangeOver(func(inner message.ImmutableMessage) error {
			if inner.MessageType() != message.MessageTypeDelete {
				return nil
			}
			deleted := message.MustAsImmutableDeleteMessageV1(inner)
			return a.applyDeleteRequest(ctx, msg.TimeTick(), deleted.MustBody())
		})
	default:
		return nil
	}
}

func (a *segcoreApplier) applyDeleteRequest(ctx context.Context, timeTick uint64, request *msgpb.DeleteRequest) error {
	if request == nil {
		return nil
	}
	return a.deleteFromAllSegments(ctx, storage.ParseIDs2PrimaryKeysBatch(request.GetPrimaryKeys()), DeleteTimestampsFromRequest(timeTick, request))
}

func (a *segcoreApplier) applyTransformLogEntry(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
	if entry == nil || entry.GetDelete() == nil {
		return nil
	}
	for _, block := range entry.GetDelete().GetBlocks() {
		if err := a.deleteFromAllSegments(ctx, storage.ParseIDs2PrimaryKeysBatch(block.GetPrimaryKeys()), DeleteTimestampsFromTransformLogBlock(entry.GetTimeTick(), block)); err != nil {
			return err
		}
	}
	return nil
}

func (a *segcoreApplier) deleteFromAllSegments(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	if primaryKeys.Len() == 0 {
		return nil
	}
	if len(timestamps) == 0 {
		timestamps = make([]typeutil.Timestamp, primaryKeys.Len())
	}
	a.mu.Lock()
	segments := make([]segcore.CSegment, 0, len(a.segments))
	for _, segment := range a.segments {
		segments = append(segments, segment)
	}
	a.mu.Unlock()
	for _, segment := range segments {
		if _, err := segment.Delete(ctx, &segcore.DeleteRequest{
			PrimaryKeys: primaryKeys,
			Timestamps:  timestamps,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (a *segcoreApplier) getOrCreateSegment(segmentID int64) (segcore.CSegment, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if segment, ok := a.segments[segmentID]; ok {
		return segment, nil
	}
	segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  a.collection,
		SegmentID:   segmentID,
		SegmentType: segcore.SegmentTypeGrowing,
	})
	if err != nil {
		return nil, err
	}
	a.segments[segmentID] = segment
	return segment, nil
}

func (a *segcoreApplier) markSegmentFlushed(segmentID int64) {
	if segmentID == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.flushedSegments == nil {
		a.flushedSegments = make(map[int64]struct{})
	}
	a.flushedSegments[segmentID] = struct{}{}
}

func (a *segcoreApplier) segmentFlushedLocked(segmentID int64) bool {
	_, ok := a.flushedSegments[segmentID]
	return ok
}

func (a *segcoreApplier) segmentIDs() []int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := make([]int64, 0, len(a.segments))
	for id := range a.segments {
		ids = append(ids, id)
	}
	return ids
}

func (a *segcoreApplier) segment(segmentID int64) (segcore.CSegment, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	segment, ok := a.segments[segmentID]
	return segment, ok
}

func (a *segcoreApplier) ReleaseSegment(segmentID int64) {
	a.mu.Lock()
	segment, ok := a.segments[segmentID]
	if ok {
		delete(a.segments, segmentID)
	}
	delete(a.flushedSegments, segmentID)
	a.mu.Unlock()
	if ok {
		segment.Release()
	}
}

func (a *segcoreApplier) snapshotSegments() map[int64]segcore.CSegment {
	a.mu.Lock()
	defer a.mu.Unlock()
	segments := make(map[int64]segcore.CSegment, len(a.segments))
	for id, segment := range a.segments {
		segments[id] = segment
	}
	return segments
}

func loadInfoFromVisibleSegment(segment walview.VisibleSegment) *querypb.SegmentLoadInfo {
	persisted := segment.Data.PersistedStorage
	if persisted == nil {
		return nil
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:      segment.SegmentID,
		PartitionID:    segment.PartitionID,
		Level:          datapb.SegmentLevel_L1,
		BinlogPaths:    make([]*datapb.FieldBinlog, 0),
		Statslogs:      make([]*datapb.FieldBinlog, 0),
		Bm25Logs:       make([]*datapb.FieldBinlog, 0),
		ManifestPath:   persisted.GetManifestPath(),
		StorageVersion: storage.StorageV2,
	}
	if persisted.GetManifestPath() != "" {
		loadInfo.StorageVersion = storage.StorageV3
	}
	if segment.Assignment != nil {
		loadInfo.CollectionID = segment.Assignment.GetCollectionId()
		loadInfo.InsertChannel = segment.Assignment.GetVchannel()
	}
	for _, binlogs := range persisted.GetBinlogs() {
		loadInfo.BinlogPaths = append(loadInfo.BinlogPaths, binlogs.GetFieldBinlog()...)
		loadInfo.Statslogs = append(loadInfo.Statslogs, binlogs.GetStatsBinlog()...)
		loadInfo.Bm25Logs = append(loadInfo.Bm25Logs, binlogs.GetBm25Binlog()...)
	}
	if persisted.GetMergedStatsBinlog() != nil {
		loadInfo.Statslogs = append(loadInfo.Statslogs, persisted.GetMergedStatsBinlog())
	}
	return loadInfo
}

func repeatedTimeTicks(timeTick uint64, n int) []typeutil.Timestamp {
	if n == 0 {
		return nil
	}
	timestamps := make([]typeutil.Timestamp, n)
	for i := range timestamps {
		timestamps[i] = timeTick
	}
	return timestamps
}

func DeleteTimestampsFromRequest(timeTick uint64, request *msgpb.DeleteRequest) []typeutil.Timestamp {
	if request == nil {
		return nil
	}
	timestamps := request.GetTimestamps()
	if len(timestamps) == 0 {
		return repeatedTimeTicks(timeTick, primaryKeyCount(request.GetPrimaryKeys()))
	}
	result := make([]typeutil.Timestamp, len(timestamps))
	copy(result, timestamps)
	return result
}

func DeleteTimestampsFromTransformLogBlock(timeTick uint64, block *streamingpb.TransformDeleteBlock) []typeutil.Timestamp {
	if block == nil {
		return nil
	}
	return repeatedTimeTicks(timeTick, primaryKeyCount(block.GetPrimaryKeys()))
}

func primaryKeyCount(ids *schemapb.IDs) int {
	if ids == nil {
		return 0
	}
	switch ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		return len(ids.GetIntId().GetData())
	case *schemapb.IDs_StrId:
		return len(ids.GetStrId().GetData())
	default:
		return 0
	}
}
