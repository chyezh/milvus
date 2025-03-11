package helper

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
)

// NewWALHelper creates a new WALHelper.
func NewWALHelper(opt *walimpls.OpenOption) *WALHelper {
	return &WALHelper{
		logger:     log.With(zap.Any("channel", opt.Channel), zap.String("accessMode", opt.AccessMode.String())),
		channel:    opt.Channel,
		accessMode: opt.AccessMode,
	}
}

// WALHelper is a helper for WAL implementation.
type WALHelper struct {
	logger     *log.MLogger
	channel    types.PChannelInfo
	accessMode types.AccessMode
}

// Channel returns the channel of the WAL.
func (w *WALHelper) Channel() types.PChannelInfo {
	return w.channel
}

func (w *WALHelper) AccessMode() types.AccessMode {
	return w.accessMode
}

// Log returns the logger of the WAL.
func (w *WALHelper) Log() *log.MLogger {
	return w.logger
}
