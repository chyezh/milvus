package coord

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

	client, err := NewClient(&Config{
		ETCDClient: c,
		RootPath:   "/test",
	})
	assert.NoError(t, err)
	assert.NotNil(t, client)
	client.Close()
}
