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
	"fmt"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// InsertChunkTaskResult is the binlog output of a completed insert sync task.
type InsertChunkTaskResult struct {
	Binlog            *streamingpb.L1SegmentBinLogs
	ManifestPath      string
	MergedStatsBinlog *datapb.FieldBinlog
}

// InsertChunkTask serializes one sealed InsertChunk into field/stat/bm25 blobs
// and uploads them to object storage.
//
// Stages: Init (CPU, build InsertData) → Serialize (CPU, codec + pk stats) →
// Upload (IO, blob puts) → Done. See SyncChunkTask for the Poll contract.
type InsertChunkTask struct {
	chunk          *InsertChunk
	schema         *schemapb.CollectionSchema
	collectionID   int64
	partitionID    int64
	segmentID      int64
	storageVersion int64
	manifestPath   string
	flush          bool
	segmentRows    int64
	columnGroups   []storagecommon.ColumnGroup
	previousBinlog []*streamingpb.L1SegmentBinLogs
	chunkManager   storage.ChunkManager
	allocator      allocator.Interface
	storageConfig  *indexpb.StorageConfig
	// onDone is invoked exactly once in terminal state. result is nil on failure.
	onDone func(result *InsertChunkTaskResult, err error)

	state      taskState
	cpuBounded bool
	insertData []*storage.InsertData
	record     storage.Record
	// uploaded object paths
	fieldBinlog       map[int64]*datapb.FieldBinlog
	statsBinlog       map[int64]*datapb.FieldBinlog
	bm25Binlog        map[int64]*datapb.FieldBinlog
	mergedStatsBinlog *datapb.FieldBinlog
}

// NewInsertChunkTask creates an insert sync task. onDone is invoked once when
// the task reaches a terminal state; result is non-nil only on success.
func NewInsertChunkTask(
	chunk *InsertChunk,
	schema *schemapb.CollectionSchema,
	collectionID, partitionID, segmentID int64,
	storageVersion int64,
	manifestPath string,
	flush bool,
	segmentRows int64,
	columnGroups []storagecommon.ColumnGroup,
	previousBinlog []*streamingpb.L1SegmentBinLogs,
	cm storage.ChunkManager,
	alloc allocator.Interface,
	storageConfig *indexpb.StorageConfig,
	onDone func(*InsertChunkTaskResult, error),
) *InsertChunkTask {
	return &InsertChunkTask{
		chunk:          chunk,
		schema:         schema,
		collectionID:   collectionID,
		partitionID:    partitionID,
		segmentID:      segmentID,
		storageVersion: storageVersion,
		manifestPath:   manifestPath,
		flush:          flush,
		segmentRows:    segmentRows,
		columnGroups:   columnGroups,
		previousBinlog: previousBinlog,
		chunkManager:   cm,
		allocator:      alloc,
		storageConfig:  storageConfig,
		onDone:         onDone,
		state:          taskStateInit,
		cpuBounded:     true,
	}
}

// Key implements SyncChunkTask.
func (t *InsertChunkTask) Key() string {
	return fmt.Sprintf("insert/seg=%d/tt=%d-%d", t.segmentID, t.chunk.startFromTimeTick, t.chunk.endToTimeTick)
}

// CPUBound implements SyncChunkTask.
func (t *InsertChunkTask) CPUBound() bool { return t.cpuBounded }

// OnComplete implements SyncChunkTask.
func (t *InsertChunkTask) OnComplete(err error) {
	defer t.releaseRecord()
	if t.onDone == nil {
		return
	}
	if err != nil {
		t.onDone(nil, err)
		return
	}
	t.onDone(&InsertChunkTaskResult{
		Binlog:            t.buildBinlog(),
		ManifestPath:      t.manifestPath,
		MergedStatsBinlog: t.mergedStatsBinlog,
	}, nil)
}

// Poll implements SyncChunkTask.
func (t *InsertChunkTask) Poll(ctx context.Context) error {
	switch t.state {
	case taskStateInit:
		data, err := prepareInsertData(t.schema, t.chunk.msgs)
		if err != nil {
			return fmt.Errorf("prepare insert data: %w", err)
		}
		t.insertData = data
		t.state = taskStateSerializing
		t.cpuBounded = true
		return ErrContinue

	case taskStateSerializing:
		if err := t.serialize(); err != nil {
			return fmt.Errorf("serialize insert data: %w", err)
		}
		t.state = taskStateUploading
		t.cpuBounded = false
		return ErrContinue

	case taskStateUploading:
		if err := t.upload(ctx); err != nil {
			// Object-storage errors are transient; let the scheduler retry.
			return NewRetryableError(fmt.Errorf("upload insert blobs: %w", err))
		}
		t.state = taskStateDone
		return nil

	case taskStateDone:
		return nil

	default:
		return fmt.Errorf("insert task in unknown state: %v", t.state)
	}
}

// serialize builds one Arrow record. The storage-version-specific writer is
// selected in upload, mirroring writebuffer's SyncTask writer dispatch.
func (t *InsertChunkTask) serialize() error {
	record, err := buildInsertRecord(t.schema, t.insertData)
	if err != nil {
		return err
	}
	t.record = record
	return nil
}

// upload validates the task, dispatches to the storage-version-specific writer,
// then applies the common flush-time stats merge.
func (t *InsertChunkTask) upload(ctx context.Context) error {
	if err := t.prepareUpload(); err != nil {
		return err
	}

	var err error
	switch t.storageVersion {
	case storage.StorageV1:
		err = t.uploadV1(ctx)
	case storage.StorageV2:
		err = t.uploadV2(ctx)
	case storage.StorageV3:
		err = t.uploadV3(ctx)
	default:
		err = fmt.Errorf("unsupported storage version %d", t.storageVersion)
	}
	if err != nil {
		return err
	}
	return t.finishUpload(ctx)
}

func (t *InsertChunkTask) prepareUpload() error {
	if t.allocator == nil {
		return fmt.Errorf("log id allocator is nil")
	}
	if t.storageConfig == nil {
		t.storageConfig = packed.CreateStorageConfig()
	}
	if t.record == nil {
		return fmt.Errorf("insert record is nil")
	}
	return nil
}

func (t *InsertChunkTask) finishUpload(ctx context.Context) error {
	if !t.flush {
		return nil
	}
	if t.storageVersion == storage.StorageV3 {
		manifestPath, _, err := syncmgr.WriteMergedManifestStats(ctx, syncmgr.MergedManifestStatsWriteInput{
			CollectionID:  t.collectionID,
			Schema:        t.schema,
			SegmentRows:   t.segmentRows,
			ManifestPath:  t.manifestPath,
			StorageConfig: t.storageConfig,
			ChunkManager:  t.chunkManager,
			Allocator:     t.allocator,
		})
		if err != nil {
			return err
		}
		t.manifestPath = manifestPath
		return nil
	}
	result, err := syncmgr.WriteMergedFieldStats(ctx, syncmgr.MergedFieldStatsWriteInput{
		CollectionID:     t.collectionID,
		PartitionID:      t.partitionID,
		SegmentID:        t.segmentID,
		Schema:           t.schema,
		SegmentRows:      t.segmentRows,
		TsFrom:           t.chunk.startFromTimeTick,
		TsTo:             t.chunk.endToTimeTick,
		PreviousBinlogs:  t.previousBinlog,
		CurrentStats:     t.statsBinlog,
		CurrentBM25Stats: t.bm25Binlog,
		ChunkManager:     t.chunkManager,
		Allocator:        t.allocator,
	})
	if err != nil {
		return err
	}
	t.mergedStatsBinlog = result.MergedStats
	if t.bm25Binlog == nil {
		t.bm25Binlog = make(map[int64]*datapb.FieldBinlog)
	}
	for fieldID, binlog := range result.BM25Stats {
		if current := t.bm25Binlog[fieldID]; current != nil {
			current.Binlogs = append(current.Binlogs, binlog.GetBinlogs()...)
			continue
		}
		t.bm25Binlog[fieldID] = binlog
	}
	return nil
}

// buildBinlog packs the uploaded paths into a streamingpb.L1SegmentBinLogs.
func (t *InsertChunkTask) buildBinlog() *streamingpb.L1SegmentBinLogs {
	return &streamingpb.L1SegmentBinLogs{
		FieldBinlog:  syncmgr.FieldBinlogValues(t.fieldBinlog),
		StatsBinlog:  syncmgr.FieldBinlogValues(t.statsBinlog),
		Bm25Binlog:   syncmgr.FieldBinlogValues(t.bm25Binlog),
		FromTimeTick: t.chunk.startFromTimeTick,
		ToTimeTick:   t.chunk.endToTimeTick,
	}
}

func (t *InsertChunkTask) releaseRecord() {
	if t.record != nil {
		t.record.Release()
		t.record = nil
	}
}

func buildInsertRecord(schema *schemapb.CollectionSchema, data []*storage.InsertData) (storage.Record, error) {
	arrowSchema, err := storage.ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	for _, chunk := range data {
		if err := storage.BuildRecord(builder, chunk, schema); err != nil {
			return nil, err
		}
	}

	rec := builder.NewRecord()
	allFields := typeutil.GetAllFieldSchemas(schema)
	field2Col := make(map[storage.FieldID]int, len(allFields))
	for c, field := range allFields {
		field2Col[field.GetFieldID()] = c
	}
	return storage.NewSimpleArrowRecord(rec, field2Col), nil
}

// prepareInsertData converts insert messages into []*storage.InsertData.
func prepareInsertData(schema *schemapb.CollectionSchema, msgs []message.ImmutableInsertMessageV1) ([]*storage.InsertData, error) {
	out := make([]*storage.InsertData, 0, len(msgs))
	for _, msg := range msgs {
		request := msg.MustBody()
		timetick := msg.TimeTick()
		request.Timestamps = make([]uint64, request.NumRows)
		for i := range request.NumRows {
			request.Timestamps[i] = timetick
		}
		data, err := storage.ColumnBasedInsertMsgToInsertData(&msgstream.InsertMsg{InsertRequest: request}, schema)
		if err != nil {
			return nil, err
		}
		ensureSystemFields(data, request.GetRowIDs(), request.GetTimestamps())
		out = append(out, data)
	}
	if err := applyFunctionOutputs(schema, out); err != nil {
		return nil, err
	}
	return out, nil
}

func applyFunctionOutputs(schema *schemapb.CollectionSchema, data []*storage.InsertData) error {
	for _, functionSchema := range schema.GetFunctions() {
		runner, err := function.NewFunctionRunner(schema, functionSchema)
		if err != nil {
			return err
		}
		if runner == nil {
			continue
		}
		defer runner.Close()
		for _, insertData := range data {
			switch functionSchema.GetType() {
			case schemapb.FunctionType_BM25:
				if err := applyBM25Function(runner, insertData); err != nil {
					return err
				}
			case schemapb.FunctionType_MinHash:
				if err := applyMinHashFunction(runner, insertData); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown function type in gsegment insert task: %s", functionSchema.GetType().String())
			}
		}
	}
	return nil
}

func applyBM25Function(runner function.FunctionRunner, data *storage.InsertData) error {
	inputs := make([]any, 0, len(runner.GetInputFields()))
	for _, inputField := range runner.GetInputFields() {
		fieldData, ok := data.Data[inputField.GetFieldID()]
		if !ok {
			return fmt.Errorf("BM25 function input field %d not found", inputField.GetFieldID())
		}
		inputs = append(inputs, fieldData.GetDataRows())
	}
	output, err := runner.BatchRun(inputs...)
	if err != nil {
		return err
	}
	if len(output) == 0 {
		return fmt.Errorf("BM25 function returned empty output")
	}
	sparseArray, ok := output[0].(*schemapb.SparseFloatArray)
	if !ok {
		return fmt.Errorf("BM25 function output is not sparse float array")
	}
	outputFieldID := runner.GetOutputFields()[0].GetFieldID()
	data.Data[outputFieldID] = &storage.SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Contents: sparseArray.GetContents(),
		},
	}
	return nil
}

func applyMinHashFunction(runner function.FunctionRunner, data *storage.InsertData) error {
	inputs := make([]any, 0, len(runner.GetInputFields()))
	for _, inputField := range runner.GetInputFields() {
		fieldData, ok := data.Data[inputField.GetFieldID()]
		if !ok {
			return fmt.Errorf("MinHash function input field %d not found", inputField.GetFieldID())
		}
		inputs = append(inputs, fieldData.GetDataRows())
	}
	output, err := runner.BatchRun(inputs...)
	if err != nil {
		return err
	}
	if len(output) == 0 {
		return fmt.Errorf("MinHash function returned empty output")
	}
	fieldData, ok := output[0].(*schemapb.FieldData)
	if !ok {
		return fmt.Errorf("MinHash function output is not field data")
	}
	vectorField := fieldData.GetVectors()
	if vectorField == nil || vectorField.GetBinaryVector() == nil {
		return fmt.Errorf("MinHash function output is not binary vector")
	}
	outputFieldID := runner.GetOutputFields()[0].GetFieldID()
	data.Data[outputFieldID] = &storage.BinaryVectorFieldData{
		Data: vectorField.GetBinaryVector(),
		Dim:  int(vectorField.GetDim()),
	}
	return nil
}

func ensureSystemFields(data *storage.InsertData, rowIDs []int64, timestamps []uint64) {
	rowNum := data.GetRowNum()
	if _, ok := data.Data[common.RowIDField]; !ok {
		rows := make([]int64, rowNum)
		copy(rows, rowIDs)
		data.Data[common.RowIDField] = &storage.Int64FieldData{Data: rows}
	}
	if _, ok := data.Data[common.TimeStampField]; !ok {
		tss := make([]int64, rowNum)
		for i := range tss {
			if i < len(timestamps) {
				tss[i] = int64(timestamps[i])
			}
		}
		data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: tss}
	}
}

// totalRows sums row counts across a slice of InsertData.
func totalRows(data []*storage.InsertData) int64 {
	var total int64
	for _, d := range data {
		for _, fd := range d.Data {
			total += int64(fd.RowNum())
			break
		}
	}
	return total
}
