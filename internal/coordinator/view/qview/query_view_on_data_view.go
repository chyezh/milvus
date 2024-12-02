package qview

import (
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// newQueryViewsOnDataView creates a new query views on the data view.
func newQueryViewsOnDataView(
	dataVersion int64,
	dataViewOfShard *viewpb.DataViewOfShard,
	views []*QueryViewOfShardAtCoord,
) *QueryViewsOnDataViewOfShard {
	nextQueryVersion := int64(1)
	for _, view := range views {
		if view.Version().QueryVersion >= nextQueryVersion {
			nextQueryVersion = view.Version().QueryVersion + 1
		}
	}
	return &QueryViewsOnDataViewOfShard{
		dataVersion:      dataVersion,
		dataView:         dataViewOfShard,
		views:            make([]*QueryViewOfShardAtCoord, 0),
		nextQueryVersion: nextQueryVersion,
	}
}

// QueryViewsOnDataViewOfShard is a struct that contains all the query views of a shard which lifetime is not gone.
type QueryViewsOnDataViewOfShard struct {
	dataVersion      int64
	dataView         *viewpb.DataViewOfShard
	nextQueryVersion int64
	views            []QueryViewVersion
}

// DataVersion returns the data version of the query views on the data view.
func (qvd *QueryViewsOnDataViewOfShard) DataVersion() int64 {
	return qvd.dataVersion
}

// DataViewOfShard returns the data view of the query views on the data view.
func (qvd *QueryViewsOnDataViewOfShard) DataViewOfShard() *viewpb.DataViewOfShard {
	return qvd.dataView
}

// NextQueryVersion returns the next query version of the query views on the data view.
func (qvd *QueryViewsOnDataViewOfShard) NextQueryVersion() int64 {
	return qvd.nextQueryVersion
}

// AddView adds a new query view to the query view of the shard.
func (qvd *QueryViewsOnDataViewOfShard) AddView(newView *QueryViewOfShardAtCoord) {
	if qvd.dataVersion != newView.Version().DataVersion {
		panic("invalid data version")
	}
	if qvd.nextQueryVersion != newView.Version().QueryVersion {
		panic("invalid query version")
	}
	qvd.nextQueryVersion++
	qvd.views = append(qvd.views, newView)
}

// RemoveView removes a query view from the query view of the shard.
func (qvd *QueryViewsOnDataViewOfShard) RemoveView(version QueryViewVersion) {
	for i, view := range qvd.views {
		if view.Version() == version {
			if view.State() != QueryViewStateDropped {
				panic("only dropped view can be removed from memory")
			}
			qvd.views = append(qvd.views[:i], qvd.views[i+1:]...)
			return
		}
	}
}
