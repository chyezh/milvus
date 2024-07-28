package inspector

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// TimeTickInfo records the information of time tick.
type TimeTickInfo struct {
	MessageID              message.MessageID // the last message id.
	LastTimeTick           uint64            // the last time tick.
	LastConfirmedMessageID message.MessageID // the last confirmed message id.
	// The time tick may be udpated, without last timetickMessage
}

// IsZero returns true if the time tick info is zero.
func (t *TimeTickInfo) IsZero() bool {
	return t.LastTimeTick == 0
}

// NewTimeTickInfoListener creates a new time tick info listener.
func NewTimeTickInfoListener() *TimeTickInfoListener {
	return &TimeTickInfoListener{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
		info: TimeTickInfo{},
	}
}

// TimeTickInfoListener is a listener for time tick info.
type TimeTickInfoListener struct {
	cond *syncutil.ContextCond
	info TimeTickInfo
}

// Update updates the time tick info.
func (l *TimeTickInfoListener) Update(info TimeTickInfo) {
	l.cond.L.Lock()
	if l.info.IsZero() || l.info.MessageID.LT(info.MessageID) {
		l.info = info
	}
	l.cond.L.Unlock()
}

// OnlyUpdateTs only updates the time tick.
func (l *TimeTickInfoListener) OnlyUpdateTs(timetick uint64) {
	l.cond.LockAndBroadcast()
	if !l.info.IsZero() && l.info.LastTimeTick < timetick {
		l.info.LastTimeTick = timetick
	}
	l.cond.L.Unlock()
}

// WatchAtMessageID watch the message id.
func (l *TimeTickInfoListener) WatchAtMessageID(messageID message.MessageID, ts uint64) <-chan struct{} {
	l.cond.L.Lock()
	if l.info.IsZero() || !l.info.MessageID.EQ(messageID) {
		l.cond.L.Unlock()
		return nil
	}
	if ts < l.info.LastTimeTick {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return l.cond.WaitChan()
}

// Get gets the time tick info.
func (l *TimeTickInfoListener) Get() TimeTickInfo {
	l.cond.L.Lock()
	info := l.info
	l.cond.L.Unlock()
	return info
}
