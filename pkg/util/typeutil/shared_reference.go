package typeutil

import (
	"go.uber.org/atomic"
)

// Closable is an interface that can be closed.
type Closable interface {
	Close()
}

// SharedReference is a reference type that can be shared with only one close operation.
// Without supported of determined destructor, A safe SharedReference is rely on user's behavior.
type SharedReference[T Closable] struct {
	inner     T
	strongCnt *atomic.Int32
}

// NewSharedReference creates a new SharedReference with the inner object.
func NewSharedReference[T Closable](inner T) *SharedReference[T] {
	return &SharedReference[T]{
		inner:     inner,
		strongCnt: atomic.NewInt32(1),
	}
}

// Deref returns the inner object.
// Deref should only be called if the reference is not closed.
// Otherwise UB happens.
func (sr *SharedReference[T]) Deref() T {
	return sr.inner
}

// Clone returns a new SharedReference with the same inner object.
// Clone should only be called if the reference is not closed.
// Otherwise UB happens.
func (sr *SharedReference[T]) Clone() *SharedReference[T] {
	k := sr.strongCnt.Inc()
	if k == 1 {
		panic("SharedReference: Clone on a closed reference")
	}
	return sr
}

// Downgrade returns a new WeakReference with the same inner object.
// After downgrade, the SharedReference can not be used.
func (sr *SharedReference[T]) Downgrade() *WeakReference[T] {
	w := &WeakReference[T]{
		inner:     sr.inner,
		strongCnt: sr.strongCnt,
	}
	sr.Close()
	return w
}

// Close closes the reference, if the reference count is 0, the inner object will be closed.
func (sr *SharedReference[T]) Close() {
	refcnt := sr.strongCnt.Dec()
	if refcnt < 0 {
		panic("SharedReference: Close on a closed reference")
	}
	if refcnt == 0 {
		sr.inner.Close()
	}
}

// WeakReference is a weak reference type that can be shared with only one close operation.
type WeakReference[T Closable] struct {
	inner     T
	strongCnt *atomic.Int32
	// TODO: add weak ref count in future.
}

func (wr *WeakReference[T]) Upgrade() *SharedReference[T] {
	n := wr.strongCnt.Inc()
	for {
		// There's no strong reference of dLogImpl, so it's impossible to create a DLogRef.
		if n == 0 {
			return nil
		}
		if wr.strongCnt.CompareAndSwap(n, n+1) {
			return &SharedReference[T]{
				inner:     wr.inner,
				strongCnt: wr.strongCnt,
			}
		}
		n = wr.strongCnt.Load()
	}
}
