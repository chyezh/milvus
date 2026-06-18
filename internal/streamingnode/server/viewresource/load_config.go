package viewresource

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource/growingruntime"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func SettingsFromAlterLoadConfig(header *messagespb.AlterLoadConfigMessageHeader) *viewpb.QueryViewSettings {
	return growingruntime.SettingsFromAlterLoadConfig(header)
}
