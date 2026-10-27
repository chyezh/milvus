package querycoordv2

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetQueryViewLoadInfo resolves the live load configuration for version zero,
// or the exact retained snapshot requested by a View for a nonzero version.
// Snapshot publication belongs to the view producer, before it publishes a View.
func (s *Server) GetQueryViewLoadInfo(ctx context.Context, req *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
	resp := &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: req.GetCollectionID()}
	if err := merr.CheckHealthy(s.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if req.GetCollectionID() == 0 {
		resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg("collection id is zero"))
		return resp, nil
	}
	if req.GetVersion() == 0 {
		// This extraction still uses the legacy load path. Its live metadata,
		// not retained history or replica readiness, determines whether loaded.
		collection := s.meta.GetCollection(ctx, req.GetCollectionID())
		if collection == nil {
			resp.Status = merr.Status(merr.WrapErrCollectionNotLoaded(req.GetCollectionID()))
			return resp, nil
		}
		resp.PartitionIDs = s.meta.GetPartitionIDsByCollection(ctx, req.GetCollectionID())
		for _, field := range collection.GetLoadFields() {
			resp.LoadFields = append(resp.LoadFields, &messagespb.LoadFieldConfig{FieldId: field})
		}
		return resp, nil
	}
	if s.loadInfoCatalog == nil {
		resp.Status = merr.Status(merr.WrapErrServiceNotReadyMsg("query view load history is not initialized"))
		return resp, nil
	}
	info, err := s.loadInfoCatalog.GetLoadInfo(ctx, req.GetCollectionID(), req.GetVersion())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if info == nil {
		resp.Status = merr.Status(merr.WrapErrServiceNotReadyMsg("query view load snapshot %d for collection %d is unavailable", req.GetVersion(), req.GetCollectionID()))
		return resp, nil
	}
	resp = proto.Clone(info).(*querypb.GetQueryViewLoadInfoResponse)
	resp.Status = merr.Success()
	// Index metadata is supplied by MixCoord from current compatible indexes.
	resp.IndexInfoList = nil
	for _, field := range resp.LoadFields {
		field.IndexId = 0
	}
	for _, field := range resp.RequiredFields {
		field.IndexId = 0
	}
	return resp, nil
}
