package collector

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestQueryNode(t *testing.T) {
	m := &QueryNodeMetrics{
		HotCollections: map[int64]QueryNodeMetricsCollection{
			1: {},
		},
	}
	val, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", val)
}
