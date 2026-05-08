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

func (t *InsertChunkTask) uploadV3(ctx context.Context) error {
	basePath, version, err := syncmgr.ManifestBaseAndVersion(t.storageConfig.GetRootPath(), t.collectionID, t.partitionID, t.segmentID, t.manifestPath)
	if err != nil {
		return err
	}
	columnGroups := t.columnGroups
	if len(columnGroups) == 0 {
		columnGroups = syncmgr.ColumnGroupsFromRecord(t.schema, t.record)
	}

	t.fieldBinlog, t.manifestPath, err = syncmgr.WriteManifestInsert(ctx, syncmgr.ManifestInsertWriteInput{
		Schema:              t.schema,
		Record:              t.record,
		BasePath:            basePath,
		Version:             version,
		TsFrom:              t.chunk.startFromTimeTick,
		TsTo:                t.chunk.endToTimeTick,
		ColumnGroups:        columnGroups,
		BufferSize:          0,
		MultiPartUploadSize: packed.DefaultMultiPartUploadSize,
		StorageConfig:       t.storageConfig,
	})
	if err != nil {
		return err
	}
	manifestPath, _, err := syncmgr.WriteManifestCurrentStats(ctx, syncmgr.ManifestCurrentStatsWriteInput{
		CollectionID:  t.collectionID,
		Schema:        t.schema,
		InsertData:    t.insertData,
		Record:        t.record,
		ManifestPath:  t.manifestPath,
		StorageConfig: t.storageConfig,
		ChunkManager:  t.chunkManager,
		Allocator:     t.allocator,
		BatchRows:     totalRows(t.insertData),
	})
	if err != nil {
		return err
	}
	t.manifestPath = manifestPath
	return nil
}
