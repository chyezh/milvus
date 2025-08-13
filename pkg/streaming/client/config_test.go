package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestConfig(t *testing.T) {
	cfg := newConfig()
	assert.Equal(t, cfg.RootPath, paramtable.Get().EtcdCfg.MetaRootPath.GetValue())

	cfg = newConfig(OptClientRootPath("/test"))
	assert.Equal(t, cfg.RootPath, "/test")
}
