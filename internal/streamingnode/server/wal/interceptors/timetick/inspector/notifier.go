package inspector

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// NewSyncNotifier creates a new sync notifier.
func NewSyncNotifier() *SyncNotifier {
	return &SyncNotifier{
		cond:   syncutil.NewContextCond(&sync.Mutex{}),
		signal: typeutil.NewSet[types.PChannelInfo](),
	}
}

// SyncNotifier is a notifier for sync signal.
type SyncNotifier struct {
	cond   *syncutil.ContextCond
	signal typeutil.Set[types.PChannelInfo]
}

// AddAndNotify adds a signal and notifies the waiter.
func (n *SyncNotifier) AddAndNotify(pChannelInfo types.PChannelInfo) {
	n.cond.LockAndBroadcast()
	n.signal.Insert(pChannelInfo)
	n.cond.L.Unlock()
}

// WaitChan returns the wait channel.
func (n *SyncNotifier) WaitChan() <-chan struct{} {
	n.cond.L.Lock()
	if n.signal.Len() > 0 {
		n.cond.L.Unlock()
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return n.cond.WaitChan()
}

// Get gets the signal.
func (n *SyncNotifier) Get() typeutil.Set[types.PChannelInfo] {
	n.cond.L.Lock()
	signal := n.signal
	n.signal = typeutil.NewSet[types.PChannelInfo]()
	n.cond.L.Unlock()
	return signal
}
