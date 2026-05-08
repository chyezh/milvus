package gsegment

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func isStaleTimeTick(start, current uint64, staleDuration time.Duration) bool {
	if start == 0 || current < start {
		return false
	}
	if staleDuration <= 0 {
		return true
	}
	return tsoutil.PhysicalTime(current).Sub(tsoutil.PhysicalTime(start)) > staleDuration
}
