package cache

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/cache/block"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/cache/lclock"
)

type Cache struct {
	mu              sync.Mutex
	flushed         []*block.ImmutableBLockList
	tail            *block.MutableBlockList
	pendings        pendingQueue
	lastConfirmInfo lclock.ConfirmedInfo
}

// Add adds a new message into cache.
func (c *Cache) Add(msg messageWithClock) {
	c.mu.Lock()
	c.pendings.Add(msg)
	c.mu.Unlock()
}

// SyncupConfirmInfo syncs up the confirmed info.
func (c *Cache) SyncupConfirmInfo(info lclock.ConfirmedInfo) {
	c.mu.Lock()
	if info.LAC > c.lastConfirmInfo.LAC || info.LF > c.lastConfirmInfo.LF {
		c.lastConfirmInfo = info
		c.advanceCache()
	}
	c.mu.Unlock()
}

// advanceCache advances the cache.
func (c *Cache) advanceCache() {
	// always pop the messages that end clock reach the last confirmed clock.
	c.pendings.PopUntilBeginClockReachLF(c.lastConfirmInfo.LF)
	msgs := c.pendings.PopUntilEndClckReachLAC(c.lastConfirmInfo.LAC)
	if len(msgs) > 0 {
		c.tail.Append(msgs)
	}
}
