package growing

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type PackWriter interface {
	FlushInsertBuffer(ctx context.Context, pack *FlushPack) (*FlushResult, error)
	FlushDeleteBuffer(ctx context.Context, pack *DeleteFlushPack) (*DeleteFlushResult, error)
}

type FlushPack struct {
	Meta         *streamingpb.SegmentAssignmentMeta
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	VChannel     string
	FromTimeTick uint64
	ToTimeTick   uint64
	Schema       *schemapb.CollectionSchema
	Rows         uint64
	BinarySize   uint64
	Inserts      []InsertEntry
}

type InsertEntry struct {
	timeTick   uint64
	assignment *messagespb.PartitionSegmentAssignment
	request    *msgpb.InsertRequest
}

type FlushResult struct {
	PersistedStorage *streamingpb.L1SegmentPersistedStorage
}

type DeleteFlushPack struct {
	VChannel      string
	CollectionID  int64
	PartitionID   int64
	FromTimeTick  uint64
	ToTimeTick    uint64
	Schema        *schemapb.CollectionSchema
	Deletes       []DeleteEntry
	StartPosition *msgpb.MsgPosition
	Checkpoint    *msgpb.MsgPosition
}

type DeleteEntry struct {
	timeTick uint64
	rows     uint64
	request  *msgpb.DeleteRequest
}

type DeleteFlushResult struct {
	Batch *L0DeleteBatch
}

func cloneL1SegmentBinLogs(binlogs []*streamingpb.L1SegmentBinLogs) []*streamingpb.L1SegmentBinLogs {
	if len(binlogs) == 0 {
		return nil
	}
	cloned := make([]*streamingpb.L1SegmentBinLogs, 0, len(binlogs))
	for _, binlog := range binlogs {
		cloned = append(cloned, cloneL1SegmentBinLog(binlog))
	}
	return cloned
}

func cloneL1SegmentBinLog(value *streamingpb.L1SegmentBinLogs) *streamingpb.L1SegmentBinLogs {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*streamingpb.L1SegmentBinLogs)
}

func cloneFieldBinlog(value *datapb.FieldBinlog) *datapb.FieldBinlog {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*datapb.FieldBinlog)
}

func clonePartitionSegmentAssignment(value *messagespb.PartitionSegmentAssignment) *messagespb.PartitionSegmentAssignment {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*messagespb.PartitionSegmentAssignment)
}

func cloneInsertRequest(value *msgpb.InsertRequest) *msgpb.InsertRequest {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*msgpb.InsertRequest)
}

func cloneDeleteRequest(value *msgpb.DeleteRequest) *msgpb.DeleteRequest {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*msgpb.DeleteRequest)
}
