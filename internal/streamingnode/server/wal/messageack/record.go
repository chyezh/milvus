package messageack

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
)

type Record interface {
	Point() utility.WALConsumeCheckpoint
	Retain() Ref
	Seal()
	Sealed() bool
	RefCount() int64
	Completed() bool
}

type Ref interface {
	Done()
}

type record struct {
	mu                 sync.Mutex
	point              utility.WALConsumeCheckpoint
	sealed             bool
	refCount           int64
	completionNotified bool
	onCompleted        func()
}

func NewRecord(point utility.WALConsumeCheckpoint, onCompleted func()) Record {
	return &record{
		point:       point,
		refCount:    1,
		onCompleted: onCompleted,
	}
}

func (r *record) Point() utility.WALConsumeCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	return *r.point.Clone()
}

func (r *record) Retain() Ref {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		panic("message ack retained after seal")
	}
	r.refCount++
	return &ref{record: r}
}

func (r *record) Seal() {
	r.mu.Lock()
	if r.sealed {
		r.mu.Unlock()
		return
	}
	r.sealed = true
	r.refCount--
	onCompleted := r.takeCompletionCallbackLocked()
	r.mu.Unlock()
	if onCompleted != nil {
		onCompleted()
	}
}

func (r *record) Sealed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sealed
}

func (r *record) RefCount() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.refCount
}

func (r *record) Completed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sealed && r.refCount == 0
}

func (r *record) release() {
	r.mu.Lock()
	if r.refCount <= 0 {
		r.mu.Unlock()
		panic("message ack reference count underflow")
	}
	r.refCount--
	onCompleted := r.takeCompletionCallbackLocked()
	r.mu.Unlock()
	if onCompleted != nil {
		onCompleted()
	}
}

func (r *record) takeCompletionCallbackLocked() func() {
	if !r.sealed || r.refCount != 0 || r.completionNotified {
		return nil
	}
	r.completionNotified = true
	return r.onCompleted
}

type ref struct {
	once   sync.Once
	record *record
}

func (r *ref) Done() {
	r.once.Do(r.record.release)
}

var _ Record = (*record)(nil)
