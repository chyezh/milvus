package qview

import (
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

var ErrDataVersionTooOld = errors.New("data version is too old")

type QueryViewBuilder struct {
	dataView  *viewpb.DataViewOfCollection
	queryView *viewpb.QueryViewOfShard
}

// QueryViewsOfShard is a struct that contains all the query views of a shard which lifetime is not gone.
type QueryViewsOfShard struct {
	collectionID    int64
	replicaID       int64
	vchannel        string
	settings        *viewpb.QueryViewSettings
	latestUpVersion *QueryViewVersion          // latestUpVersion is the latest version that the query view is up.
	queryViews      []*QueryViewOfShardAtCoord // queryViews has been sorted by the version of the underlying query view.
}

// AllocateNewQueryView allocates a new query view for the shard with data version.
func (qvs *QueryViewsOfShard) AllocateNewQueryView(dataView *viewpb.DataViewOfCollection) (*QueryViewBuilder, error) {
	// if qvs.latestUpVersion is not nil and the data version is too old, return error.
	// The latest up status query view should always keep increasing.
	if qvs.latestUpVersion != nil && qvs.latestUpVersion.DataVersion > dataView.DataVersion {
		return nil, ErrDataVersionTooOld
	}
	return &QueryViewBuilder{
		dataView:  dataView,
		queryView: qvs.newQueryView(dataView.DataVersion),
	}, nil
}

// ApplyNewQueryView applies the new query view to the query views of the shard.
func (qvs *QueryViewsOfShard) ApplyNewQueryView(newView *QueryViewOfShardAtCoord) (*QueryViewsOfShardOperationGuard, error) {
	return nil, nil
}

// newQueryView creates a new query view for the shard with the data version.
func (qvs *QueryViewsOfShard) newQueryView(dataVersion int64) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: qvs.collectionID,
			ReplicaId:    qvs.replicaID,
			Vchannel:     qvs.vchannel,
			Version: &viewpb.QueryViewVersion{
				DataVersion:  dataVersion,
				QueryVersion: qvs.generateNewQueryView(dataVersion),
			},
			State:    viewpb.QueryViewState(QueryViewStatePreparing),
			Settings: proto.Clone(qvs.settings).(*viewpb.QueryViewSettings),
		},
	}
}

// generateNewQueryVersion generates a new query version based on the data version.
func (qvs *QueryViewsOfShard) generateNewQueryView(dataVersion int64) int64 {
	for _, queryView := range qvs.queryViews {
		if queryView.inner.Shard.GetVersion().DataVersion == dataVersion {
			return queryView.inner.Shard.GetVersion().QueryVersion + 1
		}
	}
	// Return one by default.
	return 1
}

// AddNewQueryView adds a new query view to the query view of the shard.
func (qvs *QueryViewsOfShard) AddNewQueryView(view *QueryViewOfShardAtCoord) error {
	if len(qvs.queryViews) == 0 {
		// It is the first query view of current shard.
		qvs.queryViews = append(qvs.queryViews, view)
	}
	qvs.queryViews = append(qvs.queryViews, view)
	return nil
}

type QueryViewsOfShardOperationGuard struct {
	qvs *QueryViewsOfShard
}
