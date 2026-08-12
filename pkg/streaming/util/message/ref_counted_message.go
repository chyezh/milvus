package message

import (
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

type refCountedImmutableMessageCore struct {
	mu sync.Mutex

	message       ImmutableMessage
	refCount      int64
	ownerReleased bool
	finalized     bool
	finalizer     func()
}

// NewOwnedImmutableMessage takes ownership of msg and creates its unique root
// reference.
func NewOwnedImmutableMessage(
	msg ImmutableMessage,
	finalizer func(),
) OwnedImmutableMessage {
	if msg == nil {
		panic("ref-counted immutable message is nil")
	}
	core := &refCountedImmutableMessageCore{
		message:   msg,
		refCount:  1,
		finalizer: finalizer,
	}
	return &ownedImmutableMessage{core: core}
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

type ownedImmutableMessage struct {
	core *refCountedImmutableMessageCore
}

func (m *ownedImmutableMessage) Message() ImmutableMessage {
	if m.core == nil {
		panic("ref-counted immutable message owner accessed after release")
	}
	return m.core.loadMessage()
}

func (m *ownedImmutableMessage) Clone() RetainedImmutableMessage {
	if m.core == nil {
		panic("ref-counted immutable message owner cloned after release")
	}
	return m.core.ownerClone()
}

func (m *ownedImmutableMessage) Release() {
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

type ownedImmutable[T ImmutableMessage] struct {
	message T
	owner   OwnedImmutableMessage
}

func newOwnedImmutable[T ImmutableMessage](owner OwnedImmutableMessage, msg T) OwnedImmutable[T] {
	return &ownedImmutable[T]{message: msg, owner: owner}
}

func (m *ownedImmutable[T]) Message() T {
	_ = m.owner.Message()
	return m.message
}

func (m *ownedImmutable[T]) Clone() RetainedImmutable[T] {
	return newRetainedImmutable(m.message, m.owner.Clone())
}

func (m *ownedImmutable[T]) CloneHandle() RetainedImmutableMessage {
	return m.owner.Clone()
}

func (m *ownedImmutable[T]) Untyped() OwnedImmutable[ImmutableMessage] {
	return newOwnedImmutable[ImmutableMessage](m.owner, m.message)
}

type retainedImmutable[T ImmutableMessage] struct {
	message  T
	retained RetainedImmutableMessage
}

func newRetainedImmutable[T ImmutableMessage](msg T, retained RetainedImmutableMessage) RetainedImmutable[T] {
	return &retainedImmutable[T]{message: msg, retained: retained}
}

func (m *retainedImmutable[T]) Message() T {
	_ = m.retained.Message()
	return m.message
}

func (m *retainedImmutable[T]) Clone() RetainedImmutable[T] {
	return newRetainedImmutable(m.message, m.retained.Clone())
}

func (m *retainedImmutable[T]) Release() {
	if m.retained != nil {
		m.retained.Release()
		m.retained = nil
		var zero T
		m.message = zero
	}
}

// MustAsSpecializedOwnedImmutableMessage converts the message protected by
// owner to a typed immutable message and binds both values in one owned view.
func MustAsSpecializedOwnedImmutableMessage[H proto.Message, B proto.Message](
	owner OwnedImmutableMessage,
) SpecializedOwnedImmutableMessage[H, B] {
	msg := MustAsSpecializedImmutableMessage[H, B](owner.Message())
	return newOwnedImmutable(owner, msg)
}

// MustAsOwnedImmutableMessage binds owner to its underlying immutable message.
func MustAsOwnedImmutableMessage(owner OwnedImmutableMessage) OwnedImmutable[ImmutableMessage] {
	return newOwnedImmutable(owner, owner.Message())
}

// MustAsOwnedImmutableTxnMessage binds owner to its transaction message.
// The transaction and all of its child messages share one lifetime.
func MustAsOwnedImmutableTxnMessage(owner OwnedImmutableMessage) OwnedImmutable[ImmutableTxnMessage] {
	txn := AsImmutableTxnMessage(owner.Message())
	if txn == nil {
		panic("failed to parse immutable transaction message")
	}
	return newOwnedImmutable(owner, txn)
}

var (
	_ OwnedImmutableMessage               = (*ownedImmutableMessage)(nil)
	_ RetainedImmutableMessage            = (*retainedImmutableMessage)(nil)
	_ OwnedImmutable[ImmutableMessage]    = (*ownedImmutable[ImmutableMessage])(nil)
	_ RetainedImmutable[ImmutableMessage] = (*retainedImmutable[ImmutableMessage])(nil)
)
