package gsegment

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// SyncChunkTask is a task save chunk of messages into OSS.
type SyncChunkTask interface {
	// If the next step of the task is a CPU-bounded operation, the CPUBound method should return true,
	// then the task will be executed in CPU-bounded Queue.
	// Otherwise, the CPUBound method should return false, then the task will be executed in IO-bounded Queue.
	CPUBound() bool

	// Poll is called when the task is not done, and should be repeated called until the Poll returns nil.
	Poll(ctx context.Context) error
}

// NewInsertChunkTask is a task to create a new insert chunk.
type NewInsertChunkTask struct {
	chunk        *InsertChunk
	schema       *schemapb.CollectionSchema
	collectionID int64
	partitionID  int64
	segmentID    int64
	chunkManager storage.ChunkManager

	// State tracking
	state        taskState
	insertData   []*storage.InsertData
	pkStats      *storage.PrimaryKeyStats
	bm25Stats    map[int64]*storage.Blob
	statsBlob    *storage.Blob
	binlogs      map[int64]*storage.Blob
	uploadedPaths map[string]string

	cpuBounded bool
}

type taskState int

const (
	taskStateInit taskState = iota
	taskStateSerializing
	taskStateUploading
	taskStateDone
)

func (t *NewInsertChunkTask) CPUBound() bool {
	return t.cpuBounded
}

func (t *NewInsertChunkTask) Poll(ctx context.Context) error {
	switch t.state {
	case taskStateInit:
		// Prepare insert data from messages
		var err error
		t.insertData, err = t.getInsertData()
		if err != nil {
			return fmt.Errorf("failed to prepare insert data: %w", err)
		}

		// Generate BM25 stats if needed
		if hasBM25Function(t.schema) {
			if err := t.generateBM25Stats(t.insertData); err != nil {
				return fmt.Errorf("failed to generate BM25 stats: %w", err)
			}
		}

		t.state = taskStateSerializing
		t.cpuBounded = true
		return fmt.Errorf("continue") // Non-nil to continue polling

	case taskStateSerializing:
		// Serialize binlogs
		codec := t.getInsertCodec()
		blobs, err := codec.Serialize(t.partitionID, t.segmentID, t.insertData...)
		if err != nil {
			return fmt.Errorf("failed to serialize insert data: %w", err)
		}

		// Convert blobs to map by field ID
		t.binlogs = make(map[int64]*storage.Blob)
		for _, blob := range blobs {
			fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse field ID from key %s: %w", blob.GetKey(), err)
			}
			t.binlogs[fieldID] = blob
		}

		// Generate PK stats
		pkField := typeutil.MustGetPrimaryFieldSchema(t.schema)
		rowNum := t.getTotalRows()
		t.pkStats, err = storage.NewPrimaryKeyStats(pkField.GetFieldID(), int64(pkField.GetDataType()), rowNum)
		if err != nil {
			return fmt.Errorf("failed to create pk stats: %w", err)
		}

		for _, data := range t.insertData {
			if pkData, ok := data.Data[pkField.GetFieldID()]; ok {
				t.pkStats.UpdateByMsgs(pkData)
			}
		}

		t.statsBlob, err = codec.SerializePkStats(t.pkStats, rowNum)
		if err != nil {
			return fmt.Errorf("failed to serialize pk stats: %w", err)
		}

		t.state = taskStateUploading
		t.cpuBounded = false
		t.uploadedPaths = make(map[string]string)
		return fmt.Errorf("continue")

	case taskStateUploading:
		// Upload binlogs and stats to object storage
		if err := t.uploadBlobs(ctx); err != nil {
			return fmt.Errorf("failed to upload blobs: %w", err)
		}

		t.state = taskStateDone
		return nil // Task completed

	case taskStateDone:
		return nil

	default:
		return fmt.Errorf("unknown task state: %v", t.state)
	}
}

func (t *NewInsertChunkTask) getInsertData() ([]*storage.InsertData, error) {
	pkField := typeutil.MustGetPrimaryFieldSchema(t.schema)
	idata, err := writebuffer.PrepareInsertForOneSegment(t.schema, pkField, t.chunk.msgs)
	if err != nil {
		return nil, err
	}
	return idata.GetData(), nil
}

func (t *NewInsertChunkTask) generateBM25Stats(insertData []*storage.InsertData) error {
	functionRunners := t.getFunctionRunners()
	for _, runner := range functionRunners {
		// Convert insertData to the format expected by function runner
		dataSlice := make([]any, len(insertData))
		for i, data := range insertData {
			dataSlice[i] = data
		}
		_, err := runner.BatchRun(dataSlice...)
		if err != nil {
			return err
		}
	}

	// Extract BM25 stats from insert data
	t.bm25Stats = make(map[int64]*storage.Blob)
	// TODO: Extract BM25 stats from processed data
	return nil
}

func (t *NewInsertChunkTask) getFunctionRunners() []function.FunctionRunner {
	functionRunners := make([]function.FunctionRunner, 0)
	for _, tf := range t.schema.GetFunctions() {
		functionRunner, err := function.NewFunctionRunner(t.schema, tf)
		if err != nil {
			return nil
		}
		if functionRunner == nil {
			continue
		}
		functionRunners = append(functionRunners, functionRunner)
	}
	return functionRunners
}

func (t *NewInsertChunkTask) getInsertCodec() *storage.InsertCodec {
	meta := &etcdpb.CollectionMeta{
		ID:     t.collectionID,
		Schema: t.schema,
	}
	return storage.NewInsertCodecWithSchema(meta)
}

func (t *NewInsertChunkTask) getTotalRows() int64 {
	var total int64
	for _, data := range t.insertData {
		if len(data.Data) > 0 {
			for _, fieldData := range data.Data {
				total += int64(fieldData.RowNum())
				break // All fields have same row count
			}
		}
	}
	return total
}

func (t *NewInsertChunkTask) uploadBlobs(ctx context.Context) error {
	// Upload binlogs
	for fieldID, blob := range t.binlogs {
		key := fmt.Sprintf("binlog/%d/%d/%d", t.segmentID, fieldID, t.chunk.startFromTimeTick)
		if _, uploaded := t.uploadedPaths[key]; !uploaded {
			if err := t.chunkManager.Write(ctx, key, blob.Value); err != nil {
				return err
			}
			// Store field ID as key and path as value
			t.uploadedPaths[fmt.Sprintf("%d", fieldID)] = key
		}
	}

	// Upload stats
	if t.statsBlob != nil {
		statsKey := fmt.Sprintf("stats/%d/%d", t.segmentID, t.chunk.startFromTimeTick)
		if _, uploaded := t.uploadedPaths["stats"]; !uploaded {
			if err := t.chunkManager.Write(ctx, statsKey, t.statsBlob.Value); err != nil {
				return err
			}
			t.uploadedPaths["stats"] = statsKey
		}
	}

	// Upload BM25 stats if present
	for fieldID, blob := range t.bm25Stats {
		key := fmt.Sprintf("bm25stats/%d/%d/%d", t.segmentID, fieldID, t.chunk.startFromTimeTick)
		bm25Key := fmt.Sprintf("bm25_%d", fieldID)
		if _, uploaded := t.uploadedPaths[bm25Key]; !uploaded {
			if err := t.chunkManager.Write(ctx, key, blob.Value); err != nil {
				return err
			}
			t.uploadedPaths[bm25Key] = key
		}
	}

	return nil
}

func hasBM25Function(schema *schemapb.CollectionSchema) bool {
	for _, function := range schema.GetFunctions() {
		if function.GetType() == schemapb.FunctionType_BM25 {
			return true
		}
	}
	return false
}

// DeleteChunkTask is a task to persist delete chunk data
type DeleteChunkTask struct {
	chunk        *DeleteChunk
	collectionID int64
	partitionID  int64
	segmentID    int64
	chunkManager storage.ChunkManager

	// State tracking
	state        taskState
	deleteData   *storage.DeleteData
	deleteBlob   *storage.Blob
	uploadedPath string

	cpuBounded bool
}

// NewDeleteChunkTask creates a new delete chunk task
func NewDeleteChunkTask(chunk *DeleteChunk, collectionID, partitionID, segmentID int64, cm storage.ChunkManager) *DeleteChunkTask {
	return &DeleteChunkTask{
		chunk:        chunk,
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		chunkManager: cm,
		state:        taskStateInit,
		cpuBounded:   true,
	}
}

func (t *DeleteChunkTask) CPUBound() bool {
	return t.cpuBounded
}

func (t *DeleteChunkTask) Poll(ctx context.Context) error {
	switch t.state {
	case taskStateInit:
		// Convert delete messages to DeleteData
		pks := make([]storage.PrimaryKey, 0)
		tss := make([]uint64, 0)

		for _, msg := range t.chunk.msgs {
			// Get the delete request body which contains the primary keys
			body, err := msg.Body()
			if err != nil {
				return fmt.Errorf("failed to get delete request body: %w", err)
			}

			// Extract primary keys from the body
			ts := msg.TimeTick()
			if body.PrimaryKeys != nil {
				// Convert schemapb.IDs to storage.PrimaryKey
				switch body.PrimaryKeys.GetIdField().(type) {
				case *schemapb.IDs_IntId:
					for _, id := range body.PrimaryKeys.GetIntId().GetData() {
						pks = append(pks, storage.NewInt64PrimaryKey(id))
						tss = append(tss, ts)
					}
				case *schemapb.IDs_StrId:
					for _, id := range body.PrimaryKeys.GetStrId().GetData() {
						pks = append(pks, storage.NewVarCharPrimaryKey(id))
						tss = append(tss, ts)
					}
				}
			}
		}

		t.deleteData = storage.NewDeleteData(pks, tss)
		t.deleteData.RowCount = int64(len(pks))

		t.state = taskStateSerializing
		t.cpuBounded = true
		return fmt.Errorf("continue")

	case taskStateSerializing:
		// Serialize delete data
		codec := storage.NewDeleteCodec()
		var err error
		t.deleteBlob, err = codec.Serialize(t.collectionID, t.partitionID, t.segmentID, t.deleteData)
		if err != nil {
			return fmt.Errorf("failed to serialize delete data: %w", err)
		}

		t.state = taskStateUploading
		t.cpuBounded = false
		return fmt.Errorf("continue")

	case taskStateUploading:
		// Upload delete blob to object storage
		key := fmt.Sprintf("delta/%d/%d", t.segmentID, t.chunk.startFromTimeTick)
		if err := t.chunkManager.Write(ctx, key, t.deleteBlob.Value); err != nil {
			return fmt.Errorf("failed to upload delete blob: %w", err)
		}
		t.uploadedPath = key

		t.state = taskStateDone
		return nil

	case taskStateDone:
		return nil

	default:
		return fmt.Errorf("unknown task state: %v", t.state)
	}
}
