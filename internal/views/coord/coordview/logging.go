package coordview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func logCoordQueryViewEvent(ctx context.Context, event string, key qviews.QueryViewKey, fields ...mlog.Field) {
	mlog.Info(ctx, "coord query view event", append(queryViewLogFields(event, key), fields...)...)
}

func logCoordQueryViewStateChange(ctx context.Context, event string, key qviews.QueryViewKey, from, to qviews.QueryViewState, fields ...mlog.Field) {
	mlog.Info(ctx, "coord query view state event", append(queryViewStateLogFields(event, key, from, to), fields...)...)
}

func logCoordQueryViewReport(ctx context.Context, event string, key qviews.QueryViewKey, from, to qviews.QueryViewState, fields ...mlog.Field) {
	allFields := queryViewStateLogFields(event, key, from, to)
	allFields = append(allFields, fields...)
	mlog.Info(ctx, "coord query view report received", allFields...)
}

func queryViewStateLogFields(event string, key qviews.QueryViewKey, from, to qviews.QueryViewState) []mlog.Field {
	fields := queryViewLogFields(event, key)
	fields = append(fields,
		mlog.String("fromState", from.String()),
		mlog.String("toState", to.String()),
	)
	return fields
}

func queryViewLogFields(event string, key qviews.QueryViewKey) []mlog.Field {
	return []mlog.Field{
		mlog.String("event", event),
		mlog.String("queryViewKey", key.String()),
		mlog.Int64("replicaID", key.ShardID.ReplicaID),
		mlog.FieldVChannel(key.ShardID.VChannel),
		mlog.String("dataVersion", key.QueryViewVersion.DataVersion.String()),
		mlog.Int64("queryVersion", key.QueryViewVersion.QueryVersion),
	}
}
