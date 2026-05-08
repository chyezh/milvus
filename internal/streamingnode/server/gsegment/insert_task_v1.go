// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package gsegment

import (
	"context"

	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
)

func (t *InsertChunkTask) uploadV1(ctx context.Context) error {
	serializer, err := syncmgr.NewStorageSerializerWithCollectionID(t.collectionID, t.schema)
	if err != nil {
		return err
	}
	insertBlobs, err := serializer.SerializeBinlog(ctx, t.partitionID, t.segmentID, t.insertData)
	if err != nil {
		return err
	}
	t.fieldBinlog, _, err = syncmgr.WriteV1InsertBlobs(ctx, syncmgr.V1InsertWriteInput{
		CollectionID: t.collectionID,
		PartitionID:  t.partitionID,
		SegmentID:    t.segmentID,
		TsFrom:       t.chunk.startFromTimeTick,
		TsTo:         t.chunk.endToTimeTick,
		Blobs:        insertBlobs,
		ChunkManager: t.chunkManager,
		Allocator:    t.allocator,
	})
	if err != nil {
		return err
	}

	stats, err := syncmgr.WriteV1CurrentStats(ctx, syncmgr.V1CurrentStatsWriteInput{
		CollectionID: t.collectionID,
		PartitionID:  t.partitionID,
		SegmentID:    t.segmentID,
		Schema:       t.schema,
		InsertData:   t.insertData,
		Record:       t.record,
		BatchRows:    totalRows(t.insertData),
		TsFrom:       t.chunk.startFromTimeTick,
		TsTo:         t.chunk.endToTimeTick,
		ChunkManager: t.chunkManager,
		Allocator:    t.allocator,
	})
	if err != nil {
		return err
	}
	t.statsBinlog = stats.Stats
	t.bm25Binlog = stats.BM25Stats
	return nil
}
