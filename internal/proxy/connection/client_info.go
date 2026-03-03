package connection

import (
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type clientInfo struct {
	*commonpb.ClientInfo
	identifier     int64
	lastActiveTime time.Time
}

func (c *clientInfo) GetLogger() []log.Field {
	fields := ZapClientInfo(c.ClientInfo)
	fields = append(fields,
		log.Int64("identifier", c.identifier),
		log.Time("last_active_time", c.lastActiveTime),
	)
	return fields
}
