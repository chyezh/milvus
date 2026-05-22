package viewresource

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func SettingsFromAlterLoadConfig(header *messagespb.AlterLoadConfigMessageHeader) *viewpb.QueryViewSettings {
	if header == nil {
		return &viewpb.QueryViewSettings{}
	}
	fields := make([]int64, 0, len(header.GetLoadFields()))
	for _, field := range header.GetLoadFields() {
		fields = append(fields, field.GetFieldId())
	}
	return &viewpb.QueryViewSettings{
		RequiredPartitions: append([]int64{}, header.GetPartitionIds()...),
		RequiredFields:     fields,
	}
}
