//go:build test && dynamic

package transformlogbuffer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type fakeAccesser struct {
	mu       sync.Mutex
	opts     []wal.TransformLogReadOption
	scanners []*fakeScanner
}

func (a *fakeAccesser) Read(_ context.Context, opt wal.TransformLogReadOption) wal.TransformLogScanner {
	a.mu.Lock()
	defer a.mu.Unlock()
	scanner := newFakeScanner()
	a.opts = append(a.opts, opt)
	a.scanners = append(a.scanners, scanner)
	return scanner
}

func (a *fakeAccesser) readCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.opts)
}

func (a *fakeAccesser) firstScanner() *fakeScanner {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.scanners[0]
}

type fakeScanner struct {
	ch        chan wal.TransformLogEvent
	done      chan struct{}
	err       error
	closeOnce sync.Once
}

func newFakeScanner() *fakeScanner {
	return &fakeScanner{
		ch:   make(chan wal.TransformLogEvent, 16),
		done: make(chan struct{}),
	}
}

func (s *fakeScanner) Name() string {
	return "fake"
}

func (s *fakeScanner) Chan() <-chan wal.TransformLogEvent {
	return s.ch
}

func (s *fakeScanner) Error() error {
	return s.err
}

func (s *fakeScanner) Done() <-chan struct{} {
	return s.done
}

func (s *fakeScanner) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
	})
	return s.err
}

type fakeSegment struct {
	id          int64
	vchannel    string
	partitionID int64
	startAfter  uint64

	mu           sync.Mutex
	applied      []uint64
	err          error
	applyStarted chan struct{}
	applyBlock   chan struct{}
}

func (s *fakeSegment) ID() int64 {
	return s.id
}

func (s *fakeSegment) VChannel() string {
	return s.vchannel
}

func (s *fakeSegment) PartitionID() int64 {
	return s.partitionID
}

func (s *fakeSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}

func (s *fakeSegment) ApplyTransform(_ context.Context, entry *streamingpb.TransformLogEntry) error {
	if s.err != nil {
		return s.err
	}
	if s.applyStarted != nil {
		select {
		case s.applyStarted <- struct{}{}:
		default:
		}
	}
	if s.applyBlock != nil {
		<-s.applyBlock
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, entry.GetTimeTick())
	return nil
}

func (s *fakeSegment) AppliedTransformTimeTick() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.applied) == 0 {
		return 0
	}
	return s.applied[len(s.applied)-1]
}

func (s *fakeSegment) WaitTransformApplied(context.Context, uint64) error {
	return nil
}

func (s *fakeSegment) Release(context.Context) error {
	return nil
}

func (s *fakeSegment) appliedTicks() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.applied...)
}

func TestBufferAcquireReusesVChannelScannerAndRegistersFromLocalBuffer(t *testing.T) {
	accesser := &fakeAccesser{}
	buffer := New(accesser)
	view1 := newTestQueryView("v1", 50)
	view2 := newTestQueryView("v1", 80)

	guard1, err := buffer.Acquire(context.Background(), view1)
	require.NoError(t, err)
	guard2, err := buffer.Acquire(context.Background(), view2)
	require.NoError(t, err)
	require.Equal(t, 1, accesser.readCount())

	scanner := accesser.firstScanner()
	scanner.ch <- wal.TransformLogEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 60}}
	scanner.ch <- wal.TransformLogEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 90}}
	scanner.ch <- wal.TransformLogEvent{CaughtUp: &wal.TransformLogCaughtUp{StartAfterTimeTick: 50}}

	segment := &fakeSegment{id: 10, vchannel: "v1", startAfter: 80}
	reg, err := buffer.RegisterSegment(context.Background(), segment)
	require.NoError(t, err)
	require.NoError(t, reg.WaitCatchup(context.Background()))
	assert.Equal(t, []uint64{90}, segment.appliedTicks())
	assert.Equal(t, 1, accesser.readCount())

	guard1.Release()
	oldSegment := &fakeSegment{id: 11, vchannel: "v1", startAfter: 60}
	_, err = buffer.RegisterSegment(context.Background(), oldSegment)
	require.Error(t, err)

	guard2.Release()
	reg.Unregister()
	select {
	case <-scanner.Done():
	case <-time.After(time.Second):
		t.Fatal("scanner was not closed after last guard release")
	}
}

func TestBufferRegistrationKeepsApplyingLiveEntriesAfterCaughtUp(t *testing.T) {
	accesser := &fakeAccesser{}
	buffer := New(accesser)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()

	segment := &fakeSegment{id: 10, vchannel: "v1", startAfter: 50}
	reg, err := buffer.RegisterSegment(context.Background(), segment)
	require.NoError(t, err)
	defer reg.Unregister()

	scanner := accesser.firstScanner()
	scanner.ch <- wal.TransformLogEvent{CaughtUp: &wal.TransformLogCaughtUp{StartAfterTimeTick: 50}}
	require.NoError(t, reg.WaitCatchup(context.Background()))

	scanner.ch <- wal.TransformLogEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 60}}
	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]uint64{60}, segment.appliedTicks())
	}, time.Second, 10*time.Millisecond)
}

func TestGuardWaitTransformVisibleUsesVChannelFrontier(t *testing.T) {
	accesser := &fakeAccesser{}
	buffer := New(accesser)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()

	require.NoError(t, guard.WaitTransformVisible(context.Background(), 50))

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- guard.WaitTransformVisible(context.Background(), 70)
	}()

	select {
	case err := <-waitDone:
		t.Fatalf("wait finished before target frontier: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	scanner := accesser.firstScanner()
	scanner.ch <- wal.TransformLogEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 60}}
	select {
	case err := <-waitDone:
		t.Fatalf("wait finished at lower frontier: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	scanner.ch <- wal.TransformLogEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 70}}
	require.NoError(t, <-waitDone)
}

func TestGuardWaitTransformVisibleWaitsForLiveApply(t *testing.T) {
	accesser := &fakeAccesser{}
	buffer := New(accesser)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()

	segment := &fakeSegment{
		id:           10,
		vchannel:     "v1",
		startAfter:   50,
		applyStarted: make(chan struct{}, 1),
		applyBlock:   make(chan struct{}),
	}
	reg, err := buffer.RegisterSegment(context.Background(), segment)
	require.NoError(t, err)
	defer reg.Unregister()

	scanner := accesser.firstScanner()
	scanner.ch <- wal.TransformLogEvent{CaughtUp: &wal.TransformLogCaughtUp{StartAfterTimeTick: 50}}
	require.NoError(t, reg.WaitCatchup(context.Background()))

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- guard.WaitTransformVisible(context.Background(), 60)
	}()

	scanner.ch <- wal.TransformLogEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 60}}
	require.Eventually(t, func() bool {
		select {
		case <-segment.applyStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	select {
	case err := <-waitDone:
		t.Fatalf("wait finished before live apply completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(segment.applyBlock)
	require.NoError(t, <-waitDone)
	assert.Equal(t, []uint64{60}, segment.appliedTicks())
}

func TestGuardWaitTransformVisibleReturnsScannerError(t *testing.T) {
	accesser := &fakeAccesser{}
	buffer := New(accesser)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- guard.WaitTransformVisible(context.Background(), 70)
	}()

	scanner := accesser.firstScanner()
	scanner.err = errors.New("transform log truncated")
	_ = scanner.Close()

	require.ErrorContains(t, <-waitDone, "transform log truncated")
}

func TestBufferRegisterSegmentFailsWhenScannerFailsBeforeCaughtUp(t *testing.T) {
	accesser := &fakeAccesser{}
	buffer := New(accesser)
	_, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)

	scanner := accesser.firstScanner()
	scanner.err = errors.New("truncated")
	_ = scanner.Close()

	_, err = buffer.RegisterSegment(context.Background(), &fakeSegment{id: 10, vchannel: "v1", startAfter: 50})
	require.ErrorContains(t, err, "truncated")
}

func newTestQueryView(vchannel string, startAfter uint64) *qviews.QueryViewAtQueryNode {
	return qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{
			Vchannel:                    vchannel,
			TransformStartAfterTimetick: startAfter,
		},
		&viewpb.QueryViewOfQueryNode{NodeId: 1},
	).(*qviews.QueryViewAtQueryNode)
}
