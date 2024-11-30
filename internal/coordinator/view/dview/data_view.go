package dview

import "context"

// DataViewOfCollection will be a simple struct.
type DataViewOfCollection struct{}

type WatchParams struct {
	CollectionID int64
	DataVersion  int64
	DataViews    chan<- DataViewOfCollection
}

type ResourceUsage struct {
	Memery uint64
	Disk   uint64
}

type DataViewWatcher interface {
	// Watch make a subscription with the given watch params.
	// When watch is calling, the manager will send the latest data view of collection right away.
	// and then every time the data view of the collection is updated,
	// the manager will send the new data view to the channel order by the data version asc.
	Watch(ctx context.Context, params WatchParams) error
}

// EstimateResourceUsageFrom estimates the resource usage from the start data view to the end data view.
// Precondition: the collection of start and end should be same, the DataVersion of start should be lte than the DataVersion of end.
func EstimateResourceUsageFrom(start DataViewOfCollection, end DataViewOfCollection) ResourceUsage
