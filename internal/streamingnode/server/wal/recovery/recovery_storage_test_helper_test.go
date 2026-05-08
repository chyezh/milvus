package recovery

import (
	"testing"

	"github.com/milvus-io/milvus/internal/streamingnode/server/gsegment"
)

func newTestSegmentManager(t testing.TB) *gsegment.SegmentManager {
	t.Helper()
	manager := gsegment.NewSegmentManager(nil)
	t.Cleanup(manager.Close)
	return manager
}
