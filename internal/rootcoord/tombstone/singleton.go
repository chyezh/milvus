package tombstone

// collectionTombstone is the singleton instance of CollectionTtombstone
var collectionTombstone collectionTombstoneImpl

// InitCollectionTombstone initializes the collection tombstone
func InitCollectionTombstone(table ExpectedMetaTable) {
	collectionTombstone.innerTabl.Set(table)
}

// CollectionTombstone returns the singleton instance of CollectionTombstone
func CollectionTombstone() collectionTombstoneImpl {
	return collectionTombstone
}
