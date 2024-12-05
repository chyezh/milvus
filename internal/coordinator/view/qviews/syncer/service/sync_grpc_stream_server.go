package service

import "github.com/milvus-io/milvus/internal/proto/viewpb"

// syncGrpcServer is a wrapped sync stream rpc server.
type syncGrpcServer struct {
	viewpb.QueryViewSyncService_SyncServer
}

// SendViews sends the view to client.
func (s *syncGrpcServer) SendViews(view *viewpb.SyncQueryViewsResponse) error {
	return s.Send(&viewpb.SyncResponse{
		Response: &viewpb.SyncResponse_Views{
			Views: view,
		},
	})
}

// SendClose sends the close response to client.
func (s *syncGrpcServer) SendClose() error {
	return s.Send(&viewpb.SyncResponse{
		Response: &viewpb.SyncResponse_Close{
			Close: &viewpb.SyncCloseResponse{},
		},
	})
}
