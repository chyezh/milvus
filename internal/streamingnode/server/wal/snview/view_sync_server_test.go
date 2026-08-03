package snview

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func TestPChannelViewSyncServerRejectsUnavailableWAL(t *testing.T) {
	server := NewPChannelViewSyncServer(&fakeViewSyncWALManager{
		err: streamingstatus.NewChannelNotExist("p0"),
	})
	stream := newTestSyncQueryViewServerStream(newIncomingViewSyncContext(types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}, 2))

	err := server.SyncQueryView(stream)

	require.Error(t, err)
	streamingErr := streamingstatus.AsStreamingError(streamingstatus.ConvertStreamingError("ViewSyncService.SyncQueryView", err))
	require.True(t, streamingErr.IsWrongStreamingNode())
}

func TestPChannelViewSyncServerClosesStreamWhenWALCloses(t *testing.T) {
	walDone := make(chan struct{})
	server := NewPChannelViewSyncServer(&fakeViewSyncWALManager{
		wal: fakeViewSyncWAL{available: walDone},
	})
	stream := newTestSyncQueryViewServerStream(newIncomingViewSyncContext(types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}, 2))

	done := make(chan error, 1)
	go func() {
		done <- server.SyncQueryView(stream)
	}()
	<-stream.recvStarted

	close(walDone)

	select {
	case resp := <-stream.sendCh:
		require.NotNil(t, resp.GetClose())
	case <-time.After(time.Second):
		t.Fatal("expected sync stream close response after WAL closed")
	}
	close(stream.recvCh)
	require.NoError(t, <-done)
	require.Equal(t, int64(2), server.walManager.(*fakeViewSyncWALManager).walReplicaID)
}

func TestPChannelViewSyncServerUsesWrappedWALProvider(t *testing.T) {
	walDone := make(chan struct{})
	raw := fakeViewSyncWAL{available: walDone}
	server := NewPChannelViewSyncServer(&fakeViewSyncWALManager{
		wal: wrappedTestWAL{WAL: raw, raw: raw},
	})
	stream := newTestSyncQueryViewServerStream(newIncomingViewSyncContext(types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}))

	done := make(chan error, 1)
	go func() {
		done <- server.SyncQueryView(stream)
	}()
	select {
	case <-stream.recvStarted:
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("expected sync stream to start")
	}

	close(walDone)

	select {
	case resp := <-stream.sendCh:
		require.NotNil(t, resp.GetClose())
	case <-time.After(time.Second):
		t.Fatal("expected sync stream close response after WAL closed")
	}
	close(stream.recvCh)
	require.NoError(t, <-done)
}

func TestPChannelScopedQueryViewHandlerFiltersWALReplica(t *testing.T) {
	raw := &capturingViewSyncQueryViewHandler{}
	handler := &pchannelScopedQueryViewHandler{
		pchannel:     "p0",
		walReplicaID: 2,
		handler:      raw,
	}
	var reports []qviews.QueryViewAtWorkNode
	matched := newTestApplyView(1, 2, func(report qviews.QueryViewAtWorkNode) {
		reports = append(reports, report)
	})
	mismatched := newTestApplyView(2, 3, func(report qviews.QueryViewAtWorkNode) {
		reports = append(reports, report)
	})

	handler.ApplyViews([]worknodehandler.ApplyView{matched, mismatched})

	require.Len(t, raw.views, 1)
	require.Equal(t, matched.View.QueryViewKey(), raw.views[0].View.QueryViewKey())
	require.Len(t, reports, 1)
	require.Equal(t, qviews.QueryViewStateUnrecoverable, reports[0].State())
}

func newIncomingViewSyncContext(pchannel types.PChannelInfo, walReplicaID ...int64) context.Context {
	replicaID := int64(0)
	if len(walReplicaID) > 0 {
		replicaID = walReplicaID[0]
	}
	outgoingCtx := worknodehandler.EncodeQueryViewWALReplicaToOutgoingContext(context.Background(), pchannel, replicaID)
	md, _ := metadata.FromOutgoingContext(outgoingCtx)
	return metadata.NewIncomingContext(context.Background(), md)
}

func newTestApplyView(queryReplicaID int64, walReplicaID int64, onReport func(qviews.QueryViewAtWorkNode)) worknodehandler.ApplyView {
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		ReplicaId:    queryReplicaID,
		Vchannel:     funcutil.GetVirtualChannel("p0", 1, 0),
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1},
			QueryVersion: 1,
		},
		State: viewpb.QueryViewState_QueryViewStatePreparing,
	}
	return worknodehandler.ApplyView{
		View: qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{
			WalReplicaId: walReplicaID,
		}),
		OnReport: onReport,
	}
}

type fakeViewSyncWALManager struct {
	channel      types.PChannelInfo
	walReplicaID int64
	wal          wal.WAL
	err          error
}

func (m *fakeViewSyncWALManager) Open(context.Context, types.PChannelInfo) error {
	return nil
}

func (m *fakeViewSyncWALManager) GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	return m.GetAvailableWALReplica(channel, 0)
}

func (m *fakeViewSyncWALManager) GetAvailableWALReplica(channel types.PChannelInfo, walReplicaID int64) (wal.WAL, error) {
	m.channel = channel
	m.walReplicaID = walReplicaID
	return m.wal, m.err
}

func (m *fakeViewSyncWALManager) GetAvailableRawWALByPChannel(string) (wal.WAL, error) {
	return nil, nil
}

func (m *fakeViewSyncWALManager) Metrics() (*types.StreamingNodeMetrics, error) {
	return &types.StreamingNodeMetrics{}, nil
}

func (m *fakeViewSyncWALManager) Remove(context.Context, types.PChannelInfo) error {
	return nil
}

func (m *fakeViewSyncWALManager) Close() {}

type fakeViewSyncWAL struct {
	wal.WAL
	available <-chan struct{}
}

func (w fakeViewSyncWAL) Available() <-chan struct{} {
	return w.available
}

func (w fakeViewSyncWAL) QueryViewHandler() worknodehandler.QueryViewHandler {
	return fakeViewSyncQueryViewHandler{}
}

type wrappedTestWAL struct {
	wal.WAL
	raw wal.WAL
}

func (w wrappedTestWAL) UnwrapWAL() wal.WAL {
	return w.raw
}

type fakeViewSyncQueryViewHandler struct{}

func (fakeViewSyncQueryViewHandler) ApplyViews([]worknodehandler.ApplyView) {}

type capturingViewSyncQueryViewHandler struct {
	views []worknodehandler.ApplyView
}

func (h *capturingViewSyncQueryViewHandler) ApplyViews(views []worknodehandler.ApplyView) {
	h.views = append(h.views, views...)
}

type testSyncQueryViewServerStream struct {
	ctx         context.Context
	sendCh      chan *viewpb.SyncResponse
	recvCh      chan *viewpb.SyncRequest
	recvStarted chan struct{}
	recvOnce    sync.Once
}

func newTestSyncQueryViewServerStream(ctx context.Context) *testSyncQueryViewServerStream {
	return &testSyncQueryViewServerStream{
		ctx:         ctx,
		sendCh:      make(chan *viewpb.SyncResponse, 8),
		recvCh:      make(chan *viewpb.SyncRequest, 8),
		recvStarted: make(chan struct{}),
	}
}

func (s *testSyncQueryViewServerStream) Send(resp *viewpb.SyncResponse) error {
	s.sendCh <- resp
	return nil
}

func (s *testSyncQueryViewServerStream) Recv() (*viewpb.SyncRequest, error) {
	s.recvOnce.Do(func() {
		close(s.recvStarted)
	})
	req, ok := <-s.recvCh
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

func (s *testSyncQueryViewServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *testSyncQueryViewServerStream) SendHeader(metadata.MD) error { return nil }
func (s *testSyncQueryViewServerStream) SetTrailer(metadata.MD)       {}
func (s *testSyncQueryViewServerStream) Context() context.Context     { return s.ctx }
func (s *testSyncQueryViewServerStream) SendMsg(interface{}) error    { return nil }
func (s *testSyncQueryViewServerStream) RecvMsg(interface{}) error    { return nil }
