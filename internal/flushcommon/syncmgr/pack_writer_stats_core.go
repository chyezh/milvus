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
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type ManifestPKStatsWriteInput struct {
	ManifestPath  string
	StorageConfig *indexpb.StorageConfig
	ChunkManager  storage.ChunkManager
	Allocator     allocator.Interface
	FieldID       int64
	BatchBlob     *storage.Blob
	MergedBlob    *storage.Blob
}

type V1CurrentStatsWriteInput struct {
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	Schema       *schemapb.CollectionSchema
	InsertData   []*storage.InsertData
	Record       storage.Record
	BatchRows    int64
	TsFrom       typeutil.Timestamp
	TsTo         typeutil.Timestamp
	ChunkManager storage.ChunkManager
	Allocator    allocator.Interface
}

type V1CurrentStatsWriteResult struct {
	Stats       map[int64]*datapb.FieldBinlog
	BM25Stats   map[int64]*datapb.FieldBinlog
	SizeWritten int64
}

func WriteV1CurrentStats(ctx context.Context, input V1CurrentStatsWriteInput) (*V1CurrentStatsWriteResult, error) {
	result := &V1CurrentStatsWriteResult{}
	serializer, err := NewStorageSerializerWithCollectionID(input.CollectionID, input.Schema)
	if err != nil {
		return nil, err
	}
	_, statsBlob, err := serializer.SerializeStatslog(input.InsertData, input.BatchRows)
	if err != nil {
		return nil, err
	}
	result.Stats, result.SizeWritten, err = WriteV1StatsBlobs(ctx, V1StatsWriteInput{
		CollectionID: input.CollectionID,
		PartitionID:  input.PartitionID,
		SegmentID:    input.SegmentID,
		FieldID:      serializer.PKFieldID(),
		TsFrom:       input.TsFrom,
		TsTo:         input.TsTo,
		BatchBlob:    statsBlob,
		ChunkManager: input.ChunkManager,
		Allocator:    input.Allocator,
	})
	if err != nil {
		return nil, err
	}

	bm25Collector := storage.NewBm25StatsCollector(input.Schema)
	if err := bm25Collector.Collect(input.Record); err != nil {
		return nil, err
	}
	bm25Blobs, err := bm25Collector.SerializeBlobs()
	if err != nil {
		return nil, err
	}
	var bm25Size int64
	result.BM25Stats, bm25Size, err = WriteV1BM25Blobs(ctx, V1BM25WriteInput{
		CollectionID: input.CollectionID,
		PartitionID:  input.PartitionID,
		SegmentID:    input.SegmentID,
		TsFrom:       input.TsFrom,
		TsTo:         input.TsTo,
		BatchBlobs:   bm25Blobs,
		ChunkManager: input.ChunkManager,
		Allocator:    input.Allocator,
	})
	if err != nil {
		return nil, err
	}
	result.SizeWritten += bm25Size
	return result, nil
}

func WriteManifestPKStats(ctx context.Context, input ManifestPKStatsWriteInput) (string, int64, error) {
	if input.BatchBlob == nil && input.MergedBlob == nil {
		return input.ManifestPath, 0, nil
	}
	basePath, _, err := packed.UnmarshalManifestPath(input.ManifestPath)
	if err != nil {
		return "", 0, err
	}

	statKey := fmt.Sprintf("bloom_filter.%d", input.FieldID)
	files, memorySize := existingManifestStat(ctx, input.ChunkManager, input.ManifestPath, input.StorageConfig, statKey)
	var sizeWritten int64

	if input.BatchBlob != nil {
		id, err := input.Allocator.AllocOne()
		if err != nil {
			return "", 0, err
		}
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/bloom_filter.%d/%d", input.FieldID, id))
		size, err := writeManifestStatFile(input.StorageConfig, fullPath, input.BatchBlob)
		if err != nil {
			return "", 0, err
		}
		files = append(files, fullPath)
		memorySize += size
		sizeWritten += size
	}
	if input.MergedBlob != nil {
		files = NonCompoundPaths(files)
		memorySize = PathBytes(ctx, input.ChunkManager, files)
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/bloom_filter.%d/%d", input.FieldID, int64(storage.CompoundStatsType)))
		size, err := writeManifestStatFile(input.StorageConfig, fullPath, input.MergedBlob)
		if err != nil {
			return "", 0, err
		}
		files = append(files, fullPath)
		memorySize += size
		sizeWritten += size
	}

	newManifest, err := packed.AddStatsToManifest(input.ManifestPath, input.StorageConfig, []packed.StatEntry{{
		Key:      statKey,
		Files:    files,
		Metadata: map[string]string{"memory_size": strconv.FormatInt(memorySize, 10)},
	}})
	if err != nil {
		return "", 0, fmt.Errorf("failed to add stats to manifest: %w", err)
	}
	return newManifest, sizeWritten, nil
}

type ManifestBM25StatsWriteInput struct {
	ManifestPath  string
	StorageConfig *indexpb.StorageConfig
	ChunkManager  storage.ChunkManager
	Allocator     allocator.Interface
	BatchBlobs    map[int64]*storage.Blob
	MergedBlobs   map[int64]*storage.Blob
}

func WriteManifestBM25Stats(ctx context.Context, input ManifestBM25StatsWriteInput) (string, int64, error) {
	if len(input.BatchBlobs) == 0 && len(input.MergedBlobs) == 0 {
		return input.ManifestPath, 0, nil
	}
	basePath, _, err := packed.UnmarshalManifestPath(input.ManifestPath)
	if err != nil {
		return "", 0, err
	}

	fieldMap := existingManifestBM25Stats(ctx, input.ChunkManager, input.ManifestPath, input.StorageConfig)
	var sizeWritten int64
	for fieldID, blob := range input.BatchBlobs {
		id, err := input.Allocator.AllocOne()
		if err != nil {
			return "", 0, err
		}
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/bm25.%d/%d", fieldID, id))
		size, err := writeManifestStatFile(input.StorageConfig, fullPath, blob)
		if err != nil {
			return "", 0, err
		}
		stat := ensureManifestFieldStats(fieldMap, fieldID)
		stat.files = append(stat.files, fullPath)
		stat.memorySize += size
		sizeWritten += size
	}
	for fieldID, blob := range input.MergedBlobs {
		fullPath := path.Join(basePath, fmt.Sprintf("_stats/bm25.%d/%d", fieldID, int64(storage.CompoundStatsType)))
		size, err := writeManifestStatFile(input.StorageConfig, fullPath, blob)
		if err != nil {
			return "", 0, err
		}
		stat := ensureManifestFieldStats(fieldMap, fieldID)
		stat.files = NonCompoundPaths(stat.files)
		stat.memorySize = PathBytes(ctx, input.ChunkManager, stat.files)
		stat.files = append(stat.files, fullPath)
		stat.memorySize += size
		sizeWritten += size
	}

	statEntries := make([]packed.StatEntry, 0, len(fieldMap))
	for fieldID, stat := range fieldMap {
		statEntries = append(statEntries, packed.StatEntry{
			Key:      fmt.Sprintf("bm25.%d", fieldID),
			Files:    stat.files,
			Metadata: map[string]string{"memory_size": strconv.FormatInt(stat.memorySize, 10)},
		})
	}
	if len(statEntries) == 0 {
		return input.ManifestPath, sizeWritten, nil
	}
	newManifest, err := packed.AddStatsToManifest(input.ManifestPath, input.StorageConfig, statEntries)
	if err != nil {
		return "", 0, fmt.Errorf("failed to add BM25 stats to manifest: %w", err)
	}
	return newManifest, sizeWritten, nil
}

type ManifestCurrentStatsWriteInput struct {
	CollectionID  int64
	Schema        *schemapb.CollectionSchema
	InsertData    []*storage.InsertData
	Record        storage.Record
	ManifestPath  string
	StorageConfig *indexpb.StorageConfig
	ChunkManager  storage.ChunkManager
	Allocator     allocator.Interface
	BatchRows     int64
}

func WriteManifestCurrentStats(ctx context.Context, input ManifestCurrentStatsWriteInput) (string, int64, error) {
	if input.ManifestPath == "" {
		return "", 0, nil
	}
	serializer, err := NewStorageSerializerWithCollectionID(input.CollectionID, input.Schema)
	if err != nil {
		return "", 0, err
	}
	_, statsBlob, err := serializer.SerializeStatslog(input.InsertData, input.BatchRows)
	if err != nil {
		return "", 0, err
	}
	manifestPath, sizeWritten, err := WriteManifestPKStats(ctx, ManifestPKStatsWriteInput{
		ManifestPath:  input.ManifestPath,
		StorageConfig: input.StorageConfig,
		ChunkManager:  input.ChunkManager,
		Allocator:     input.Allocator,
		FieldID:       serializer.PKFieldID(),
		BatchBlob:     statsBlob,
	})
	if err != nil {
		return "", 0, err
	}

	bm25Collector := storage.NewBm25StatsCollector(input.Schema)
	if err := bm25Collector.Collect(input.Record); err != nil {
		return "", 0, err
	}
	bm25Blobs, err := bm25Collector.SerializeBlobs()
	if err != nil {
		return "", 0, err
	}
	manifestPath, bm25Size, err := WriteManifestBM25Stats(ctx, ManifestBM25StatsWriteInput{
		ManifestPath:  manifestPath,
		StorageConfig: input.StorageConfig,
		ChunkManager:  input.ChunkManager,
		Allocator:     input.Allocator,
		BatchBlobs:    bm25Blobs,
	})
	if err != nil {
		return "", 0, err
	}
	return manifestPath, sizeWritten + bm25Size, nil
}

type MergedFieldStatsWriteInput struct {
	CollectionID     int64
	PartitionID      int64
	SegmentID        int64
	Schema           *schemapb.CollectionSchema
	SegmentRows      int64
	TsFrom           typeutil.Timestamp
	TsTo             typeutil.Timestamp
	PreviousBinlogs  []*streamingpb.L1SegmentBinLogs
	CurrentStats     map[int64]*datapb.FieldBinlog
	CurrentBM25Stats map[int64]*datapb.FieldBinlog
	ChunkManager     storage.ChunkManager
	Allocator        allocator.Interface
}

type MergedFieldStatsWriteResult struct {
	MergedStats *datapb.FieldBinlog
	BM25Stats   map[int64]*datapb.FieldBinlog
	SizeWritten int64
}

func WriteMergedFieldStats(ctx context.Context, input MergedFieldStatsWriteInput) (*MergedFieldStatsWriteResult, error) {
	result := &MergedFieldStatsWriteResult{}
	serializer, err := NewStorageSerializerWithCollectionID(input.CollectionID, input.Schema)
	if err != nil {
		return nil, err
	}
	paths := StatsPaths(previousStatsBinlogs(input.PreviousBinlogs), FieldBinlogValues(input.CurrentStats))
	stats, err := ReadPKStats(ctx, input.ChunkManager, paths)
	if err != nil {
		return nil, err
	}
	if len(stats) > 0 {
		blob, err := serializer.SerializePKStatsList(stats, input.SegmentRows)
		if err != nil {
			return nil, err
		}
		logs, size, err := WriteV1StatsBlobs(ctx, V1StatsWriteInput{
			CollectionID: input.CollectionID,
			PartitionID:  input.PartitionID,
			SegmentID:    input.SegmentID,
			FieldID:      serializer.PKFieldID(),
			TsFrom:       input.TsFrom,
			TsTo:         input.TsTo,
			MergedBlob:   blob,
			ChunkManager: input.ChunkManager,
			Allocator:    input.Allocator,
		})
		if err != nil {
			return nil, err
		}
		result.MergedStats = logs[serializer.PKFieldID()]
		result.SizeWritten += size
	}

	mergedBM25Blobs, err := mergedBM25FieldBlobs(ctx, input.ChunkManager, previousBM25Binlogs(input.PreviousBinlogs), FieldBinlogValues(input.CurrentBM25Stats))
	if err != nil {
		return nil, err
	}
	logs, size, err := WriteV1BM25Blobs(ctx, V1BM25WriteInput{
		CollectionID: input.CollectionID,
		PartitionID:  input.PartitionID,
		SegmentID:    input.SegmentID,
		TsFrom:       input.TsFrom,
		TsTo:         input.TsTo,
		MergedBlobs:  mergedBM25Blobs,
		ChunkManager: input.ChunkManager,
		Allocator:    input.Allocator,
	})
	if err != nil {
		return nil, err
	}
	result.BM25Stats = logs
	result.SizeWritten += size
	return result, nil
}

type MergedManifestStatsWriteInput struct {
	CollectionID  int64
	Schema        *schemapb.CollectionSchema
	SegmentRows   int64
	ManifestPath  string
	StorageConfig *indexpb.StorageConfig
	ChunkManager  storage.ChunkManager
	Allocator     allocator.Interface
}

func WriteMergedManifestStats(ctx context.Context, input MergedManifestStatsWriteInput) (string, int64, error) {
	if input.ManifestPath == "" {
		return "", 0, nil
	}
	serializer, err := NewStorageSerializerWithCollectionID(input.CollectionID, input.Schema)
	if err != nil {
		return "", 0, err
	}
	manifestStats, err := packed.GetManifestStats(input.ManifestPath, input.StorageConfig)
	if err != nil {
		return "", 0, err
	}

	manifestPath := input.ManifestPath
	var sizeWritten int64
	pkKey := fmt.Sprintf("bloom_filter.%d", serializer.PKFieldID())
	if stat, ok := manifestStats[pkKey]; ok && len(stat.Paths) > 0 {
		stats, err := ReadPKStats(ctx, input.ChunkManager, stat.Paths)
		if err != nil {
			return "", 0, err
		}
		if len(stats) > 0 {
			blob, err := serializer.SerializePKStatsList(stats, input.SegmentRows)
			if err != nil {
				return "", 0, err
			}
			manifestPath, sizeWritten, err = WriteManifestPKStats(ctx, ManifestPKStatsWriteInput{
				ManifestPath:  manifestPath,
				StorageConfig: input.StorageConfig,
				ChunkManager:  input.ChunkManager,
				Allocator:     input.Allocator,
				FieldID:       serializer.PKFieldID(),
				MergedBlob:    blob,
			})
			if err != nil {
				return "", 0, err
			}
		}
	}

	mergedBM25Blobs, err := mergedBM25ManifestBlobs(ctx, input.ChunkManager, manifestStats)
	if err != nil {
		return "", 0, err
	}
	manifestPath, bm25Size, err := WriteManifestBM25Stats(ctx, ManifestBM25StatsWriteInput{
		ManifestPath:  manifestPath,
		StorageConfig: input.StorageConfig,
		ChunkManager:  input.ChunkManager,
		Allocator:     input.Allocator,
		MergedBlobs:   mergedBM25Blobs,
	})
	if err != nil {
		return "", 0, err
	}
	return manifestPath, sizeWritten + bm25Size, nil
}

func ReadPKStats(ctx context.Context, cm storage.ChunkManager, paths []string) ([]*storage.PrimaryKeyStats, error) {
	blobs, err := readBlobs(ctx, cm, NonCompoundPaths(paths))
	if err != nil {
		return nil, err
	}
	return storage.DeserializeStats(blobs)
}

func ReadBM25Stats(ctx context.Context, cm storage.ChunkManager, paths []string) (*storage.BM25Stats, error) {
	blobs, err := readBlobs(ctx, cm, NonCompoundPaths(paths))
	if err != nil {
		return nil, err
	}
	merged := storage.NewBM25Stats()
	for _, blob := range blobs {
		stats, err := storage.NewBM25StatsWithBytes(blob.Value)
		if err != nil {
			return nil, err
		}
		merged.Merge(stats)
	}
	return merged, nil
}

func StatsPaths(groups ...[]*datapb.FieldBinlog) []string {
	var paths []string
	for _, group := range groups {
		for _, binlog := range group {
			for _, log := range binlog.GetBinlogs() {
				if log.GetLogPath() != "" && !IsCompoundStatsPath(log.GetLogPath()) {
					paths = append(paths, log.GetLogPath())
				}
			}
		}
	}
	return paths
}

func NonCompoundPaths(paths []string) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if p != "" && !IsCompoundStatsPath(p) {
			out = append(out, p)
		}
	}
	return out
}

func IsCompoundStatsPath(p string) bool {
	return path.Base(p) == storage.CompoundStatsType.LogIdx()
}

type manifestFieldStats struct {
	files      []string
	memorySize int64
}

func existingManifestStat(
	ctx context.Context,
	cm storage.ChunkManager,
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	key string,
) ([]string, int64) {
	stats, err := packed.GetManifestStats(manifestPath, storageConfig)
	if err != nil {
		return nil, 0
	}
	stat, ok := stats[key]
	if !ok || len(stat.Paths) == 0 {
		return nil, 0
	}
	files := append([]string{}, stat.Paths...)
	return files, manifestStatMemorySize(ctx, cm, stat)
}

func existingManifestBM25Stats(
	ctx context.Context,
	cm storage.ChunkManager,
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
) map[int64]*manifestFieldStats {
	fieldMap := make(map[int64]*manifestFieldStats)
	existingStats, err := packed.GetManifestStats(manifestPath, storageConfig)
	if err != nil {
		return fieldMap
	}
	for key, existing := range existingStats {
		prefix, fieldID, ok := packed.ParseStatKey(key)
		if !ok || prefix != "bm25" || len(existing.Paths) == 0 {
			continue
		}
		fieldMap[fieldID] = &manifestFieldStats{
			files:      append([]string{}, existing.Paths...),
			memorySize: manifestStatMemorySize(ctx, cm, existing),
		}
	}
	return fieldMap
}

func ensureManifestFieldStats(fieldMap map[int64]*manifestFieldStats, fieldID int64) *manifestFieldStats {
	stat := fieldMap[fieldID]
	if stat == nil {
		stat = &manifestFieldStats{}
		fieldMap[fieldID] = stat
	}
	return stat
}

func writeManifestStatFile(storageConfig *indexpb.StorageConfig, fullPath string, blob *storage.Blob) (int64, error) {
	if blob == nil {
		return 0, nil
	}
	if err := packed.WriteFile(storageConfig, fullPath, blob.Value); err != nil {
		return 0, err
	}
	return int64(len(blob.Value)), nil
}

func manifestStatMemorySize(ctx context.Context, cm storage.ChunkManager, stat packed.ManifestStat) int64 {
	if memStr, ok := stat.Metadata["memory_size"]; ok {
		if mem, err := strconv.ParseInt(memStr, 10, 64); err == nil {
			return mem
		}
	}
	if cm == nil {
		return 0
	}
	return PathBytes(ctx, cm, stat.Paths)
}

func PathBytes(ctx context.Context, cm storage.ChunkManager, paths []string) int64 {
	var total int64
	for _, p := range paths {
		if size, err := cm.Size(ctx, p); err == nil {
			total += size
			continue
		}
		if value, err := cm.Read(ctx, p); err == nil {
			total += int64(len(value))
		}
	}
	return total
}

func readBlobs(ctx context.Context, cm storage.ChunkManager, paths []string) ([]*storage.Blob, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	values, err := cm.MultiRead(ctx, paths)
	if err != nil {
		return nil, err
	}
	blobs := make([]*storage.Blob, 0, len(values))
	for i, value := range values {
		blobs = append(blobs, &storage.Blob{Key: paths[i], Value: value})
	}
	return blobs, nil
}

func previousStatsBinlogs(previous []*streamingpb.L1SegmentBinLogs) []*datapb.FieldBinlog {
	var out []*datapb.FieldBinlog
	for _, binlog := range previous {
		out = append(out, binlog.GetStatsBinlog()...)
	}
	return out
}

func previousBM25Binlogs(previous []*streamingpb.L1SegmentBinLogs) []*datapb.FieldBinlog {
	var out []*datapb.FieldBinlog
	for _, binlog := range previous {
		out = append(out, binlog.GetBm25Binlog()...)
	}
	return out
}

func mergedBM25FieldBlobs(
	ctx context.Context,
	cm storage.ChunkManager,
	previous []*datapb.FieldBinlog,
	current []*datapb.FieldBinlog,
) (map[int64]*storage.Blob, error) {
	byField := make(map[int64][]string)
	for _, binlog := range append(previous, current...) {
		for _, log := range binlog.GetBinlogs() {
			if !IsCompoundStatsPath(log.GetLogPath()) && log.GetLogPath() != "" {
				byField[binlog.GetFieldID()] = append(byField[binlog.GetFieldID()], log.GetLogPath())
			}
		}
	}
	mergedBlobs := make(map[int64]*storage.Blob)
	for fieldID, paths := range byField {
		merged, err := ReadBM25Stats(ctx, cm, paths)
		if err != nil {
			return nil, err
		}
		if merged == nil || merged.NumRow() == 0 {
			continue
		}
		bytes, err := merged.Serialize()
		if err != nil {
			return nil, err
		}
		mergedBlobs[fieldID] = &storage.Blob{
			Value:      bytes,
			MemorySize: int64(len(bytes)),
			RowNum:     merged.NumRow(),
		}
	}
	return mergedBlobs, nil
}

func mergedBM25ManifestBlobs(
	ctx context.Context,
	cm storage.ChunkManager,
	manifestStats map[string]packed.ManifestStat,
) (map[int64]*storage.Blob, error) {
	mergedBlobs := make(map[int64]*storage.Blob)
	for key, stat := range manifestStats {
		prefix, fieldID, ok := packed.ParseStatKey(key)
		if !ok || prefix != "bm25" || len(stat.Paths) == 0 {
			continue
		}
		merged, err := ReadBM25Stats(ctx, cm, stat.Paths)
		if err != nil {
			return nil, err
		}
		if merged == nil || merged.NumRow() == 0 {
			continue
		}
		bytes, err := merged.Serialize()
		if err != nil {
			return nil, err
		}
		mergedBlobs[fieldID] = &storage.Blob{
			Value:      bytes,
			MemorySize: int64(len(bytes)),
			RowNum:     merged.NumRow(),
		}
	}
	return mergedBlobs, nil
}
