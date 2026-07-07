package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func logQNQueryViewEvent(event string, key qviews.QueryViewKey, fields ...mlog.Field) {
	mlog.Info(context.TODO(), "querynode query view event", append(qnQueryViewLogFields(event, key), fields...)...)
}

func logQNQueryViewStateChange(event string, key qviews.QueryViewKey, from, to qviews.QueryViewState, fields ...mlog.Field) {
	mlog.Info(context.TODO(), "querynode query view state event", append(qnQueryViewStateLogFields(event, key, from, to), fields...)...)
}

func logQNQueryViewReport(event string, key qviews.QueryViewKey, state qviews.QueryViewState, fields ...mlog.Field) {
	allFields := qnQueryViewLogFields(event, key)
	allFields = append(allFields, mlog.String("state", state.String()))
	allFields = append(allFields, fields...)
	mlog.Info(context.TODO(), "querynode query view report", allFields...)
}

func logQNQueryViewSegmentFailure(event string, key qviews.QueryViewKey, segmentID int64, err error, fields ...mlog.Field) {
	allFields := qnQueryViewLogFields(event, key)
	allFields = append(allFields, mlog.FieldSegmentID(segmentID))
	if err != nil {
		allFields = append(allFields, mlog.Err(err))
	}
	allFields = append(allFields, fields...)
	mlog.Warn(context.TODO(), "querynode query view segment failed", allFields...)
}

func qnQueryViewStateLogFields(event string, key qviews.QueryViewKey, from, to qviews.QueryViewState) []mlog.Field {
	fields := qnQueryViewLogFields(event, key)
	fields = append(fields,
		mlog.String("fromState", from.String()),
		mlog.String("toState", to.String()),
	)
	return fields
}

func qnQueryViewLogFields(event string, key qviews.QueryViewKey) []mlog.Field {
	return []mlog.Field{
		mlog.String("event", event),
		mlog.String("queryViewKey", key.String()),
		mlog.Int64("replicaID", key.ShardID.ReplicaID),
		mlog.FieldVChannel(key.ShardID.VChannel),
		mlog.String("dataVersion", key.QueryViewVersion.DataVersion.String()),
		mlog.Int64("queryVersion", key.QueryViewVersion.QueryVersion),
	}
}

func countReadySegments(readySegments map[int64][]int64) int {
	total := 0
	for _, segmentIDs := range readySegments {
		total += len(segmentIDs)
	}
	return total
}

func countViewSegments(view *viewpb.QueryViewOfQueryNode) int {
	total := 0
	for _, partition := range view.GetPartitions() {
		total += len(partition.GetSegmentIds())
	}
	return total
}
