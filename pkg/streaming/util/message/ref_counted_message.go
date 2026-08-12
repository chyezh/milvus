package message

import (
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

type refCountedImmutableMessageCore struct {
	mu sync.Mutex

	message   ImmutableMessage
	sealed    bool
	refCount  int64
	finalized bool
	finalizer func()
}

// NewRefCountedImmutableMessage takes ownership of msg and returns its
// dispatch-lifetime controller.
func NewRefCountedImmutableMessage(
	msg ImmutableMessage,
	finalizer func(),
) RefCountedImmutableMessageController {
	if msg == nil {
		panic("ref-counted immutable message is nil")
	}
	core := &refCountedImmutableMessageCore{
		message:   msg,
		refCount:  1,
		finalizer: finalizer,
	}
	return &refCountedImmutableMessage{
		immutableMessageView: newImmutableMessageView(core),
		core:                 core,
	}
}

func (c *refCountedImmutableMessageCore) immutableMessage() ImmutableMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finalized || c.message == nil {
		panic("ref-counted immutable message accessed after finalization")
	}
	return c.message
}

func (c *refCountedImmutableMessageCore) retain() RetainedImmutableMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed {
		panic("ref-counted immutable message retained after seal")
	}
	c.refCount++
	handle := &retainedImmutableMessage{}
	handle.core.Store(c)
	handle.immutableMessageView = newImmutableMessageView(handle)
	return handle
}

func (c *refCountedImmutableMessageCore) seal() {
	c.mu.Lock()
	if c.sealed {
		c.mu.Unlock()
		return
	}
	c.sealed = true
	c.refCount--
	finalizer, finalized := c.takeFinalizerLocked()
	c.mu.Unlock()
	c.finishFinalization(finalizer, finalized)
}

func (c *refCountedImmutableMessageCore) release() {
	c.mu.Lock()
	if c.refCount <= 0 {
		c.mu.Unlock()
		panic("ref-counted immutable message reference count underflow")
	}
	c.refCount--
	finalizer, finalized := c.takeFinalizerLocked()
	c.mu.Unlock()
	c.finishFinalization(finalizer, finalized)
}

func (c *refCountedImmutableMessageCore) takeFinalizerLocked() (func(), bool) {
	if !c.sealed || c.refCount != 0 || c.finalized {
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
	if finalizer == nil {
		return
	}
	finalizer()
}

func (c *refCountedImmutableMessageCore) isSealed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sealed
}

func (c *refCountedImmutableMessageCore) isExclusive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sealed && c.refCount == 1
}

type immutableMessageProvider interface {
	immutableMessage() ImmutableMessage
}

type wrappedImmutableMessage interface {
	unwrapImmutableMessage() ImmutableMessage
}

type immutableMessageView struct {
	provider immutableMessageProvider
}

func newImmutableMessageView(provider immutableMessageProvider) *immutableMessageView {
	return &immutableMessageView{provider: provider}
}

func (m *immutableMessageView) unwrapImmutableMessage() ImmutableMessage {
	return m.provider.immutableMessage()
}

func (m *immutableMessageView) MarshalLogObject(enc mlog.ObjectEncoder) error {
	return m.provider.immutableMessage().MarshalLogObject(enc)
}

func (m *immutableMessageView) MessageType() MessageType {
	return m.provider.immutableMessage().MessageType()
}

func (m *immutableMessageView) Version() Version {
	return m.provider.immutableMessage().Version()
}

func (m *immutableMessageView) MessageTypeWithVersion() MessageTypeWithVersion {
	return m.provider.immutableMessage().MessageTypeWithVersion()
}

func (m *immutableMessageView) Payload() []byte {
	return m.provider.immutableMessage().Payload()
}

func (m *immutableMessageView) EstimateSize() int {
	return m.provider.immutableMessage().EstimateSize()
}

func (m *immutableMessageView) Properties() RProperties {
	return m.provider.immutableMessage().Properties()
}

func (m *immutableMessageView) IsUnreplicable() bool {
	return m.provider.immutableMessage().IsUnreplicable()
}

func (m *immutableMessageView) TimeTick() uint64 {
	return m.provider.immutableMessage().TimeTick()
}

func (m *immutableMessageView) BarrierTimeTick() uint64 {
	return m.provider.immutableMessage().BarrierTimeTick()
}

func (m *immutableMessageView) TxnContext() *TxnContext {
	return m.provider.immutableMessage().TxnContext()
}

func (m *immutableMessageView) BroadcastHeader() *BroadcastHeader {
	return m.provider.immutableMessage().BroadcastHeader()
}

func (m *immutableMessageView) ReplicateHeader() *ReplicateHeader {
	return m.provider.immutableMessage().ReplicateHeader()
}

func (m *immutableMessageView) IsPersisted() bool {
	return m.provider.immutableMessage().IsPersisted()
}

func (m *immutableMessageView) IsPChannelLevel() bool {
	return m.provider.immutableMessage().IsPChannelLevel()
}

func (m *immutableMessageView) IntoMessageProto() *messagespb.Message {
	return m.provider.immutableMessage().IntoMessageProto()
}

func (m *immutableMessageView) WALName() WALName {
	return m.provider.immutableMessage().WALName()
}

func (m *immutableMessageView) VChannel() string {
	return m.provider.immutableMessage().VChannel()
}

func (m *immutableMessageView) PChannel() string {
	return m.provider.immutableMessage().PChannel()
}

func (m *immutableMessageView) MessageID() MessageID {
	return m.provider.immutableMessage().MessageID()
}

func (m *immutableMessageView) LastConfirmedMessageID() MessageID {
	return m.provider.immutableMessage().LastConfirmedMessageID()
}

func (m *immutableMessageView) IntoImmutableMessageProto() *commonpb.ImmutableMessage {
	return m.provider.immutableMessage().IntoImmutableMessageProto()
}

func (m *immutableMessageView) IntoBroadcastMutableMessage() BroadcastMutableMessage {
	return m.provider.immutableMessage().IntoBroadcastMutableMessage()
}

type refCountedImmutableMessage struct {
	*immutableMessageView
	core *refCountedImmutableMessageCore
}

func (m *refCountedImmutableMessage) Retain() RetainedImmutableMessage {
	return m.core.retain()
}

func (m *refCountedImmutableMessage) Seal() {
	m.core.seal()
}

type retainedImmutableMessage struct {
	*immutableMessageView
	core atomic.Pointer[refCountedImmutableMessageCore]
}

func (m *retainedImmutableMessage) immutableMessage() ImmutableMessage {
	return m.loadCore().immutableMessage()
}

func (m *retainedImmutableMessage) Sealed() bool {
	return m.loadCore().isSealed()
}

func (m *retainedImmutableMessage) IsExclusive() bool {
	return m.loadCore().isExclusive()
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

func unwrapImmutableMessage(msg ImmutableMessage) ImmutableMessage {
	for {
		wrapped, ok := msg.(wrappedImmutableMessage)
		if !ok {
			return msg
		}
		msg = wrapped.unwrapImmutableMessage()
	}
}

var (
	_ RefCountedImmutableMessageController = (*refCountedImmutableMessage)(nil)
	_ RetainedImmutableMessage             = (*retainedImmutableMessage)(nil)
)
