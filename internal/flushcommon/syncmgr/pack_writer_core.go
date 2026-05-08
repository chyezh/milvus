// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package syncmgr

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// manifestRecordWriter is the common interface for both packedRecordManifestWriter
// and packedTextManifestWriter used for V3 storage writes.
type manifestRecordWriter interface {
	storage.RecordWriter
	GetColumnGroupWrittenCompressed(columnGroup typeutil.UniqueID) uint64
	GetColumnGroupWrittenUncompressed(columnGroup typeutil.UniqueID) uint64
	GetWrittenPaths(columnGroup typeutil.UniqueID) string
	GetWrittenManifest() string
	GetWrittenRowNum() int64
}

type FieldBlobWriteInput struct {
	RootPath     string
	LogRoot      string
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	FieldID      int64
	LogID        int64
	TsFrom       typeutil.Timestamp
	TsTo         typeutil.Timestamp
	Blob         *storage.Blob
	ChunkManager storage.ChunkManager
	RetryOptions []retry.Option
}

func WriteFieldBlob(ctx context.Context, input FieldBlobWriteInput) (*datapb.Binlog, int64, error) {
	if input.Blob == nil {
		return nil, 0, nil
	}
	p := metautil.JoinIDPath(input.CollectionID, input.PartitionID, input.SegmentID, input.FieldID, input.LogID)
	key := path.Join(input.RootPath, input.LogRoot, p)
	err := retry.Handle(ctx, func() (bool, error) {
		err := input.ChunkManager.Write(ctx, key, input.Blob.Value)
		if err == nil {
			return false, nil
		}
		err = storage.ToMilvusIoError(key, err)
		if merr.IsNonRetryableErr(err) {
			return false, err
		}
		return true, err
	}, input.RetryOptions...)
	if err != nil {
		return nil, 0, err
	}
	size := int64(len(input.Blob.GetValue()))
	return &datapb.Binlog{
		EntriesNum:    input.Blob.RowNum,
		TimestampFrom: input.TsFrom,
		TimestampTo:   input.TsTo,
		LogPath:       key,
		LogSize:       size,
		MemorySize:    input.Blob.MemorySize,
	}, size, nil
}

type V1InsertWriteInput struct {
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	TsFrom       typeutil.Timestamp
	TsTo         typeutil.Timestamp
	Blobs        map[int64]*storage.Blob
	ChunkManager storage.ChunkManager
	Allocator    allocator.Interface
	RetryOptions []retry.Option
}

func WriteV1InsertBlobs(ctx context.Context, input V1InsertWriteInput) (map[int64]*datapb.FieldBinlog, int64, error) {
	logs := make(map[int64]*datapb.FieldBinlog)
	var written int64
	for fieldID, blob := range input.Blobs {
		id, err := input.Allocator.AllocOne()
		if err != nil {
			return nil, 0, err
		}
		binlog, size, err := WriteFieldBlob(ctx, FieldBlobWriteInput{
			RootPath:     input.ChunkManager.RootPath(),
			LogRoot:      common.SegmentInsertLogPath,
			CollectionID: input.CollectionID,
			PartitionID:  input.PartitionID,
			SegmentID:    input.SegmentID,
			FieldID:      fieldID,
			LogID:        id,
			TsFrom:       input.TsFrom,
			TsTo:         input.TsTo,
			Blob:         blob,
			ChunkManager: input.ChunkManager,
			RetryOptions: input.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		logs[fieldID] = &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{binlog},
		}
	}
	return logs, written, nil
}

type V1StatsWriteInput struct {
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	FieldID      int64
	TsFrom       typeutil.Timestamp
	TsTo         typeutil.Timestamp
	BatchBlob    *storage.Blob
	MergedBlob   *storage.Blob
	ChunkManager storage.ChunkManager
	Allocator    allocator.Interface
	RetryOptions []retry.Option
}

func WriteV1StatsBlobs(ctx context.Context, input V1StatsWriteInput) (map[int64]*datapb.FieldBinlog, int64, error) {
	binlogs := make([]*datapb.Binlog, 0, 2)
	var written int64
	if input.BatchBlob != nil {
		id, err := input.Allocator.AllocOne()
		if err != nil {
			return nil, 0, err
		}
		binlog, size, err := WriteFieldBlob(ctx, FieldBlobWriteInput{
			RootPath:     input.ChunkManager.RootPath(),
			LogRoot:      common.SegmentStatslogPath,
			CollectionID: input.CollectionID,
			PartitionID:  input.PartitionID,
			SegmentID:    input.SegmentID,
			FieldID:      input.FieldID,
			LogID:        id,
			TsFrom:       input.TsFrom,
			TsTo:         input.TsTo,
			Blob:         input.BatchBlob,
			ChunkManager: input.ChunkManager,
			RetryOptions: input.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		binlogs = append(binlogs, binlog)
	}
	if input.MergedBlob != nil {
		binlog, size, err := WriteFieldBlob(ctx, FieldBlobWriteInput{
			RootPath:     input.ChunkManager.RootPath(),
			LogRoot:      common.SegmentStatslogPath,
			CollectionID: input.CollectionID,
			PartitionID:  input.PartitionID,
			SegmentID:    input.SegmentID,
			FieldID:      input.FieldID,
			LogID:        int64(storage.CompoundStatsType),
			TsFrom:       input.TsFrom,
			TsTo:         input.TsTo,
			Blob:         input.MergedBlob,
			ChunkManager: input.ChunkManager,
			RetryOptions: input.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		binlogs = append(binlogs, binlog)
	}
	return map[int64]*datapb.FieldBinlog{
		input.FieldID: {
			FieldID: input.FieldID,
			Binlogs: binlogs,
		},
	}, written, nil
}

type V1BM25WriteInput struct {
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	TsFrom       typeutil.Timestamp
	TsTo         typeutil.Timestamp
	BatchBlobs   map[int64]*storage.Blob
	MergedBlobs  map[int64]*storage.Blob
	ChunkManager storage.ChunkManager
	Allocator    allocator.Interface
	RetryOptions []retry.Option
}

func WriteV1BM25Blobs(ctx context.Context, input V1BM25WriteInput) (map[int64]*datapb.FieldBinlog, int64, error) {
	logs := make(map[int64]*datapb.FieldBinlog)
	var written int64
	for fieldID, blob := range input.BatchBlobs {
		id, err := input.Allocator.AllocOne()
		if err != nil {
			return nil, 0, err
		}
		binlog, size, err := WriteFieldBlob(ctx, FieldBlobWriteInput{
			RootPath:     input.ChunkManager.RootPath(),
			LogRoot:      common.SegmentBm25LogPath,
			CollectionID: input.CollectionID,
			PartitionID:  input.PartitionID,
			SegmentID:    input.SegmentID,
			FieldID:      fieldID,
			LogID:        id,
			TsFrom:       input.TsFrom,
			TsTo:         input.TsTo,
			Blob:         blob,
			ChunkManager: input.ChunkManager,
			RetryOptions: input.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		logs[fieldID] = &datapb.FieldBinlog{FieldID: fieldID, Binlogs: []*datapb.Binlog{binlog}}
	}
	for fieldID, blob := range input.MergedBlobs {
		binlog, size, err := WriteFieldBlob(ctx, FieldBlobWriteInput{
			RootPath:     input.ChunkManager.RootPath(),
			LogRoot:      common.SegmentBm25LogPath,
			CollectionID: input.CollectionID,
			PartitionID:  input.PartitionID,
			SegmentID:    input.SegmentID,
			FieldID:      fieldID,
			LogID:        int64(storage.CompoundStatsType),
			TsFrom:       input.TsFrom,
			TsTo:         input.TsTo,
			Blob:         blob,
			ChunkManager: input.ChunkManager,
			RetryOptions: input.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		fieldBinlog := logs[fieldID]
		if fieldBinlog == nil {
			fieldBinlog = &datapb.FieldBinlog{FieldID: fieldID}
			logs[fieldID] = fieldBinlog
		}
		fieldBinlog.Binlogs = append(fieldBinlog.Binlogs, binlog)
	}
	return logs, written, nil
}

type PackedInsertWriteInput struct {
	CollectionID        int64
	PartitionID         int64
	SegmentID           int64
	Schema              *schemapb.CollectionSchema
	Record              storage.Record
	TsFrom              typeutil.Timestamp
	TsTo                typeutil.Timestamp
	RootPath            string
	BucketName          string
	ColumnGroups        []storagecommon.ColumnGroup
	BufferSize          int64
	MultiPartUploadSize int64
	StorageConfig       *indexpb.StorageConfig
	PluginContext       *indexcgopb.StoragePluginContext
	Allocator           allocator.Interface
}

func WritePackedInsert(ctx context.Context, input PackedInsertWriteInput) (map[int64]*datapb.FieldBinlog, string, error) {
	if input.MultiPartUploadSize == 0 {
		input.MultiPartUploadSize = packed.DefaultMultiPartUploadSize
	}
	logs := make(map[int64]*datapb.FieldBinlog)
	paths := make([]string, 0, len(input.ColumnGroups))
	for _, columnGroup := range input.ColumnGroups {
		id, err := input.Allocator.AllocOne()
		if err != nil {
			return nil, "", err
		}
		p := metautil.BuildInsertLogPath(input.RootPath, input.CollectionID, input.PartitionID, input.SegmentID, columnGroup.GroupID, id)
		paths = append(paths, p)
	}
	w, err := storage.NewPackedRecordWriter(input.BucketName, paths, input.Schema, input.BufferSize, input.MultiPartUploadSize, input.ColumnGroups, input.StorageConfig, input.PluginContext)
	if err != nil {
		return nil, "", err
	}
	if err := w.Write(input.Record); err != nil {
		_ = w.Close()
		return nil, "", err
	}
	if err := w.Close(); err != nil {
		return nil, "", err
	}
	for _, columnGroup := range input.ColumnGroups {
		columnGroupID := columnGroup.GroupID
		logs[columnGroupID] = &datapb.FieldBinlog{
			FieldID:     columnGroupID,
			ChildFields: columnGroup.Fields,
			Binlogs: []*datapb.Binlog{{
				LogSize:         int64(w.GetColumnGroupWrittenCompressed(columnGroup.GroupID)),
				MemorySize:      int64(w.GetColumnGroupWrittenUncompressed(columnGroup.GroupID)),
				LogPath:         w.GetWrittenPaths(columnGroupID),
				EntriesNum:      w.GetWrittenRowNum(),
				TimestampFrom:   input.TsFrom,
				TimestampTo:     input.TsTo,
				FieldNullCounts: fieldNullCounts(input.Record, columnGroup),
			}},
		}
	}
	return logs, "", nil
}

type ManifestInsertWriteInput struct {
	Schema              *schemapb.CollectionSchema
	Record              storage.Record
	BasePath            string
	Version             int64
	TsFrom              typeutil.Timestamp
	TsTo                typeutil.Timestamp
	ColumnGroups        []storagecommon.ColumnGroup
	BufferSize          int64
	MultiPartUploadSize int64
	StorageConfig       *indexpb.StorageConfig
	PluginContext       *indexcgopb.StoragePluginContext
}

func WriteManifestInsert(ctx context.Context, input ManifestInsertWriteInput) (map[int64]*datapb.FieldBinlog, string, error) {
	var w manifestRecordWriter
	textColumnConfigs := buildTextColumnConfigs(input.Schema, path.Dir(input.BasePath))
	var err error
	if len(textColumnConfigs) > 0 {
		w, err = storage.NewPackedTextManifestWriter("", input.BasePath, input.Version, input.Schema,
			input.BufferSize, input.MultiPartUploadSize, input.ColumnGroups, input.StorageConfig, textColumnConfigs)
	} else {
		w, err = storage.NewPackedRecordManifestWriter(input.BasePath, input.Version, input.Schema,
			input.BufferSize, input.MultiPartUploadSize, input.ColumnGroups, input.StorageConfig, input.PluginContext)
	}
	if err != nil {
		return nil, "", err
	}
	if err := w.Write(input.Record); err != nil {
		_ = w.Close()
		return nil, "", err
	}
	if err := w.Close(); err != nil {
		return nil, "", err
	}
	logs := make(map[int64]*datapb.FieldBinlog)
	for _, columnGroup := range input.ColumnGroups {
		columnGroupID := columnGroup.GroupID
		logs[columnGroupID] = &datapb.FieldBinlog{
			FieldID:     columnGroupID,
			ChildFields: columnGroup.Fields,
			Binlogs: []*datapb.Binlog{{
				LogSize:         int64(w.GetColumnGroupWrittenCompressed(columnGroup.GroupID)),
				MemorySize:      int64(w.GetColumnGroupWrittenUncompressed(columnGroup.GroupID)),
				LogPath:         w.GetWrittenPaths(columnGroupID),
				EntriesNum:      w.GetWrittenRowNum(),
				TimestampFrom:   input.TsFrom,
				TimestampTo:     input.TsTo,
				FieldNullCounts: fieldNullCounts(input.Record, columnGroup),
			}},
		}
	}
	return logs, w.GetWrittenManifest(), nil
}

type DeltaWriteInput struct {
	CollectionID  int64
	PartitionID   int64
	SegmentID     int64
	LogID         int64
	PKType        schemapb.DataType
	Path          string
	DeleteData    *storage.DeleteData
	Version       int64
	StorageConfig *indexpb.StorageConfig
	Uploader      func(context.Context, map[string][]byte) error
}

func WriteDelta(ctx context.Context, input DeltaWriteInput) (*datapb.Binlog, int64, error) {
	writer, err := storage.NewDeltalogWriter(
		ctx, input.CollectionID, input.PartitionID, input.SegmentID, input.LogID, input.PKType, input.Path,
		storage.WithVersion(input.Version),
		storage.WithStorageConfig(input.StorageConfig),
		storage.WithUploader(input.Uploader),
	)
	if err != nil {
		return nil, 0, err
	}
	record, tsFrom, tsTo, err := storage.BuildDeleteRecord(input.DeleteData.Pks, input.DeleteData.Tss)
	if err != nil {
		return nil, 0, err
	}
	defer record.Release()
	if err := writer.Write(record); err != nil {
		return nil, 0, err
	}
	if err := writer.Close(); err != nil {
		return nil, 0, err
	}
	memorySize := input.DeleteData.Size()
	logSize := input.DeleteData.Size() / 4
	if input.Version == storage.StorageV2 {
		logSize = 0
		if written := writer.GetWrittenUncompressed(); written != 0 {
			memorySize = int64(written)
		}
	}
	binlog := &datapb.Binlog{
		EntriesNum:    input.DeleteData.RowCount,
		TimestampFrom: tsFrom,
		TimestampTo:   tsTo,
		LogPath:       input.Path,
		LogSize:       logSize,
		MemorySize:    memorySize,
	}
	if input.Version == storage.StorageV2 {
		binlog.LogID = input.LogID
	}
	return binlog, logSize, nil
}

func fieldNullCounts(record storage.Record, columnGroup storagecommon.ColumnGroup) map[int64]int64 {
	result := make(map[int64]int64, len(columnGroup.Fields))
	for _, fieldID := range columnGroup.Fields {
		if col := record.Column(fieldID); col != nil {
			result[fieldID] = int64(col.NullN())
		}
	}
	return result
}

func ManifestBasePath(rootPath string, collectionID, partitionID, segmentID int64) string {
	return path.Join(rootPath, common.SegmentInsertLogPath, metautil.JoinIDPath(collectionID, partitionID, segmentID))
}

func ManifestBaseAndVersion(rootPath string, collectionID, partitionID, segmentID int64, manifestPath string) (string, int64, error) {
	if manifestPath != "" {
		return packed.UnmarshalManifestPath(manifestPath)
	}
	return ManifestBasePath(rootPath, collectionID, partitionID, segmentID), packed.ManifestEarliest, nil
}

func FieldBinlogValues(binlogs map[int64]*datapb.FieldBinlog) []*datapb.FieldBinlog {
	values := make([]*datapb.FieldBinlog, 0, len(binlogs))
	for _, binlog := range binlogs {
		if binlog != nil {
			values = append(values, binlog)
		}
	}
	return values
}

func ColumnGroupsFromRecord(schema *schemapb.CollectionSchema, record storage.Record) []storagecommon.ColumnGroup {
	allFields := typeutil.GetAllFieldSchemas(schema)
	stats := make(map[int64]storagecommon.ColumnStats, len(allFields))
	for _, field := range allFields {
		arr := record.Column(field.GetFieldID())
		if arr == nil || arr.Len() == 0 {
			continue
		}
		stats[field.GetFieldID()] = storagecommon.ColumnStats{
			AvgSize: int64(arr.Data().SizeInBytes()) / int64(arr.Len()),
		}
	}
	return storagecommon.SplitColumns(allFields, stats, storagecommon.DefaultPolicies()...)
}

func DeltaSummary(logID int64, entriesNum int64, memorySize int64) *datapb.FieldBinlog {
	return &datapb.FieldBinlog{
		Binlogs: []*datapb.Binlog{{
			LogID:      logID,
			EntriesNum: entriesNum,
			MemorySize: memorySize,
		}},
	}
}

func NewUnsupportedStorageVersionError(version int64) error {
	return fmt.Errorf("unsupported storage version %d", version)
}
