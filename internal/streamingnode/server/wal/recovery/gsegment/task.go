package gsegment

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
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
	chunk  *InsertChunk
	schema *schemapb.CollectionSchema

	bm25Stats *storage.Blob
	stats     *storage.Blob

	cpuBounded bool
}

func (t *NewInsertChunkTask) CPUBound() bool {
	return t.cpuBounded
}

func (t *NewInsertChunkTask) Poll(ctx context.Context) error {
}

func (t *NewInsertChunkTask) getInsertData() ([]*storage.InsertData, error) {
	pkField := typeutil.MustGetPrimaryFieldSchema(t.schema)
	idata, err := writebuffer.PrepareInsertForOneSegment(t.schema, pkField, t.chunk.msgs)
	if err != nil {
		return nil, err
	}
	return idata.Data, nil
}

func (t *NewInsertChunkTask) generateBM25Stats(insertData []*storage.InsertData) error {
	functionRunners := t.getFunctionRunners()
	for _, function := range functionRunners {
		inputFields := function.GetInputFields()
		outputFields := function.GetOutputFields()

		function.BatchRun(insertData...)
	}
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
