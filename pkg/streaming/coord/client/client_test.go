package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	kvfactory "github.com/milvus-io/milvus/pkg/v2/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestDial(t *testing.T) {
	paramtable.Init()

	c, _ := kvfactory.GetEtcdAndPath()
	assert.NotNil(t, c)

	client := NewClient(c)
	assert.NotNil(t, client)
	client.Close()
}
