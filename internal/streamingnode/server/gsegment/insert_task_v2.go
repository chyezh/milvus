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
	"github.com/milvus-io/milvus/internal/storagev2/packed"
)

func (t *InsertChunkTask) uploadV2(ctx context.Context) error {
	columnGroups := t.columnGroups
	if len(columnGroups) == 0 {
		columnGroups = syncmgr.ColumnGroupsFromRecord(t.schema, t.record)
	}
	var err error
	t.fieldBinlog, _, err = syncmgr.WritePackedInsert(ctx, syncmgr.PackedInsertWriteInput{
		CollectionID:        t.collectionID,
		PartitionID:         t.partitionID,
		SegmentID:           t.segmentID,
		Schema:              t.schema,
		Record:              t.record,
		TsFrom:              t.chunk.startFromTimeTick,
		TsTo:                t.chunk.endToTimeTick,
		RootPath:            t.storageConfig.GetRootPath(),
		BucketName:          t.storageConfig.GetBucketName(),
		ColumnGroups:        columnGroups,
		BufferSize:          0,
		MultiPartUploadSize: packed.DefaultMultiPartUploadSize,
		StorageConfig:       t.storageConfig,
		Allocator:           t.allocator,
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
