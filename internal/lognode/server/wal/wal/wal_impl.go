package wal

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal/writer"
	"github.com/milvus-io/milvus/internal/proto/logpb"
)

// walImpl is a wrapper of basicWAL to extend the basicWAL interface.
type walImpl struct {
	writer.Writer
	*scannerManager
}

// Channel returns the channel assignment info of the wal.
func (w *walImpl) Channel() logpb.PChannelInfo {
	return w.Writer.Channel()
}

// Close closes the wal instance.
func (m *walImpl) Close() {
	m.Writer.Close()
	m.scannerManager.Close()
}
