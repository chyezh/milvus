package helper

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
)

// NewWALHelper creates a new WALHelper.
func NewWALHelper(opt *walimpls.OpenOption) *WALHelper {
	wh := &WALHelper{
		channel: opt.Channel,
	}
	wh.SetLogger(log.With(log.FieldModule("walimpls"), zap.Stringer("channel", opt.Channel)))
	return wh
}

// WALHelper is a helper for WAL implementation.
type WALHelper struct {
	log.Binder
	channel types.PChannelInfo
}

// Channel returns the channel of the WAL.
func (w *WALHelper) Channel() types.PChannelInfo {
	return w.channel
}
