package qnview

type segmentRefKey struct {
	segmentID    int64
	walReplicaID int64
}

func newSegmentRefKey(segmentID int64, walReplicaID int64) segmentRefKey {
	return segmentRefKey{segmentID: segmentID, walReplicaID: walReplicaID}
}

func segmentRefKeyFromSegment(segment TransformSegment) segmentRefKey {
	if segment == nil {
		return segmentRefKey{}
	}
	walReplicaID := int64(0)
	if withReplica, ok := segment.(interface{ WALReplicaID() int64 }); ok {
		walReplicaID = withReplica.WALReplicaID()
	}
	return newSegmentRefKey(segment.ID(), walReplicaID)
}
