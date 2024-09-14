package rm

import "github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"

func Init(
	memoryLimit int64,
	diskLimit int64,
	diskPath string,
) {
	Memory = newRM(memoryLimit, func(bo walcache.BlockOperator) {
		bo.AsyncEvictMemory()
	})
	Memory.StartEventLoop()

	if diskLimit > 0 && diskPath != "" {
		Disk = newDiskResourceManager(diskPath, diskLimit)
		Disk.StartEventLoop()
	}
}

var (
	Memory *ResourceManager
	Disk   *DiskResourceManager
)

// allocateReq is the request of allocate.
type allocateReq struct {
	r    walcache.BlockOperator
	resp chan struct{}
}

// freeReq is the request of free.
type freeReq struct {
	bytes int64
}
