package qvmanager

type QueryViewManager struct {
	replicas map[int64]*QueryViewsOfReplica // map the replica id to the query view of the replica.
	recovery QueryViewRecovery
}

type QueryViewsOfReplica struct {
	collectionID int64
	replicaID    int64
	shards       map[string]*QueryViewOfShard // map the vchannel name into the query view of the vchannel.
}
