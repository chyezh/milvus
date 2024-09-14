package rm

import (
	"fmt"
	"os"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
)

// newDiskResourceManager creates a new disk cache.
func newDiskResourceManager(rootPath string, limit int64) *DiskResourceManager {
	if err := os.RemoveAll(rootPath); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(rootPath, os.ModePerm); err != nil {
		panic(err)
	}
	return &DiskResourceManager{
		ResourceManager: newRM(limit, func(bo walcache.BlockOperator) {
			bo.AsyncEvictDisk()
		}),
		rootPath: rootPath,
	}
}

// DiskResourceManager is a disk resource manager.
type DiskResourceManager struct {
	*ResourceManager
	rootPath string
}

// DiskRM creates a disk resource manager.
func (drm *DiskResourceManager) GetPath(blockID int64) string {
	return fmt.Sprintf("%s/%d.cacheblock", drm.rootPath, blockID)
}
