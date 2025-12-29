package gsegment

import (
	"context"
)

// SaveChunkTask is a task save a chunk of messages.
// Poll is called when the task is not done, and should be repeated called until the Poll returns nil.
// If the next step of the task is a CPU-bounded operation, the CPUBound method should return true,
// then the task will be executed in CPU-bounded Queue.
// Otherwise, the CPUBound method should return false, then the task will be executed in IO-bounded Queue.
type SaveChunkTask interface {
	CPUBound() bool

	Poll(ctx context.Context) error
}
