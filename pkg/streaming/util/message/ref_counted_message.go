package message

import (
	"sync"
	"sync/atomic"
)

type refCountedImmutableMessageCore struct {
	mu sync.Mutex

	message       ImmutableMessage
	refCount      int64
	ownerReleased bool
	finalized     bool
	finalizer     func()
}

// NewRefCountedImmutableMessageOwner takes ownership of msg and creates its
// unique root reference.
func NewRefCountedImmutableMessageOwner(
	msg ImmutableMessage,
	finalizer func(),
) RefCountedImmutableMessageOwner {
	if msg == nil {
		panic("ref-counted immutable message is nil")
	}
	core := &refCountedImmutableMessageCore{
		message:   msg,
		refCount:  1,
		finalizer: finalizer,
	}
	return &refCountedImmutableMessageOwner{core: core}
}

func (c *refCountedImmutableMessageCore) loadMessage() ImmutableMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finalized || c.message == nil {
		panic("ref-counted immutable message accessed after finalization")
	}
	return c.message
}

func (c *refCountedImmutableMessageCore) clone() RetainedImmutableMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finalized || c.message == nil {
		panic("ref-counted immutable message cloned after finalization")
	}
	c.refCount++
	handle := &retainedImmutableMessage{}
	handle.core.Store(c)
	return handle
}

func (c *refCountedImmutableMessageCore) ownerClone() RetainedImmutableMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ownerReleased {
		panic("ref-counted immutable message owner cloned after release")
	}
	c.refCount++
	handle := &retainedImmutableMessage{}
	handle.core.Store(c)
	return handle
}

func (c *refCountedImmutableMessageCore) releaseOwner() {
	c.mu.Lock()
	if c.ownerReleased {
		c.mu.Unlock()
		return
	}
	c.ownerReleased = true
	finalizer, finalized := c.releaseLocked()
	c.mu.Unlock()
	c.finishFinalization(finalizer, finalized)
}

func (c *refCountedImmutableMessageCore) release() {
	c.mu.Lock()
	finalizer, finalized := c.releaseLocked()
	c.mu.Unlock()
	c.finishFinalization(finalizer, finalized)
}

func (c *refCountedImmutableMessageCore) releaseLocked() (func(), bool) {
	if c.refCount <= 0 {
		panic("ref-counted immutable message reference count underflow")
	}
	c.refCount--
	if c.refCount != 0 || c.finalized {
		return nil, false
	}
	c.finalized = true
	return c.finalizer, true
}

func (c *refCountedImmutableMessageCore) finishFinalization(finalizer func(), finalized bool) {
	if !finalized {
		return
	}
	defer func() {
		c.mu.Lock()
		c.message = nil
		c.finalizer = nil
		c.mu.Unlock()
	}()
	if finalizer != nil {
		finalizer()
	}
}

type refCountedImmutableMessageOwner struct {
	core *refCountedImmutableMessageCore
}

func (m *refCountedImmutableMessageOwner) Message() ImmutableMessage {
	if m.core == nil {
		panic("ref-counted immutable message owner accessed after release")
	}
	return m.core.loadMessage()
}

func (m *refCountedImmutableMessageOwner) Clone() RetainedImmutableMessage {
	if m.core == nil {
		panic("ref-counted immutable message owner cloned after release")
	}
	return m.core.ownerClone()
}

func (m *refCountedImmutableMessageOwner) Release() {
	if m.core != nil {
		m.core.releaseOwner()
		m.core = nil
	}
}

type retainedImmutableMessage struct {
	core atomic.Pointer[refCountedImmutableMessageCore]
}

func (m *retainedImmutableMessage) Message() ImmutableMessage {
	return m.loadCore().loadMessage()
}

func (m *retainedImmutableMessage) Clone() RetainedImmutableMessage {
	return m.loadCore().clone()
}

func (m *retainedImmutableMessage) Release() {
	if core := m.core.Swap(nil); core != nil {
		core.release()
	}
}

func (m *retainedImmutableMessage) loadCore() *refCountedImmutableMessageCore {
	core := m.core.Load()
	if core == nil {
		panic("retained immutable message accessed after release")
	}
	return core
}

// OwnedMessage combines a specialized immutable message with the root owner
// that protects it during synchronous dispatch.
type OwnedMessage[T ImmutableMessage] struct {
	message T
	owner   RefCountedImmutableMessageOwner
}

func NewOwnedMessage[T ImmutableMessage](owner RefCountedImmutableMessageOwner, msg T) OwnedMessage[T] {
	return OwnedMessage[T]{message: msg, owner: owner}
}

func (m OwnedMessage[T]) Message() T {
	if m.owner != nil {
		_ = m.owner.Message()
	}
	return m.message
}

func (m OwnedMessage[T]) Clone() RetainedMessage[T] {
	return RetainedMessage[T]{message: m.message, retained: m.owner.Clone()}
}

func (m OwnedMessage[T]) CloneHandle() RetainedImmutableMessage {
	return m.owner.Clone()
}

func (m OwnedMessage[T]) Untyped() OwnedMessage[ImmutableMessage] {
	return OwnedMessage[ImmutableMessage]{message: m.message, owner: m.owner}
}

// RetainedMessage combines typed access with one independently releasable
// reference. The typed message remains valid until Release.
type RetainedMessage[T ImmutableMessage] struct {
	message  T
	retained RetainedImmutableMessage
}

func (m RetainedMessage[T]) Message() T {
	_ = m.retained.Message()
	return m.message
}

func (m RetainedMessage[T]) Clone() RetainedMessage[T] {
	return RetainedMessage[T]{message: m.message, retained: m.retained.Clone()}
}

func (m RetainedMessage[T]) Untyped() RetainedMessage[ImmutableMessage] {
	return RetainedMessage[ImmutableMessage]{message: m.message, retained: m.retained}
}

func (m RetainedMessage[T]) Handle() RetainedImmutableMessage {
	return m.retained
}

func (m *RetainedMessage[T]) Release() {
	if m.retained != nil {
		m.retained.Release()
		m.retained = nil
		var zero T
		m.message = zero
	}
}

var (
	_ RefCountedImmutableMessageOwner = (*refCountedImmutableMessageOwner)(nil)
	_ RetainedImmutableMessage        = (*retainedImmutableMessage)(nil)
)
