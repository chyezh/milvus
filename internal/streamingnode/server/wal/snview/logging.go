package snview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func logSNQueryViewEvent(event string, key qviews.QueryViewKey, fields ...mlog.Field) {
	mlog.Info(context.TODO(), "streamingnode query view event", append(snQueryViewLogFields(event, key), fields...)...)
}

func logSNQueryViewStateChange(event string, key qviews.QueryViewKey, from, to qviews.QueryViewState, fields ...mlog.Field) {
	mlog.Info(context.TODO(), "streamingnode query view state event", append(snQueryViewStateLogFields(event, key, from, to), fields...)...)
}

func logSNQueryViewReport(event string, key qviews.QueryViewKey, state qviews.QueryViewState, fields ...mlog.Field) {
	allFields := snQueryViewLogFields(event, key)
	allFields = append(allFields, mlog.String("state", state.String()))
	allFields = append(allFields, fields...)
	mlog.Info(context.TODO(), "streamingnode query view report", allFields...)
}

func snQueryViewStateLogFields(event string, key qviews.QueryViewKey, from, to qviews.QueryViewState) []mlog.Field {
	fields := snQueryViewLogFields(event, key)
	fields = append(fields,
		mlog.String("fromState", from.String()),
		mlog.String("toState", to.String()),
	)
	return fields
}

func snQueryViewLogFields(event string, key qviews.QueryViewKey) []mlog.Field {
	return []mlog.Field{
		mlog.String("event", event),
		mlog.String("queryViewKey", key.String()),
		mlog.Int64("replicaID", key.ShardID.ReplicaID),
		mlog.FieldVChannel(key.ShardID.VChannel),
		mlog.String("dataVersion", key.QueryViewVersion.DataVersion.String()),
		mlog.Int64("queryVersion", key.QueryViewVersion.QueryVersion),
	}
}
