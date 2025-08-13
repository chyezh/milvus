package menv

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestRuntimConfig(t *testing.T) {
	SetRole(typeutil.StandaloneRole)
	assert.Equal(t, GetRole(), typeutil.StandaloneRole)

	SetLocalComponentEnabled(typeutil.QueryNodeRole)
	assert.True(t, IsLocalComponentEnabled(typeutil.QueryNodeRole))

	SetLocalComponentEnabled(typeutil.QueryCoordRole)
	assert.True(t, IsLocalComponentEnabled(typeutil.QueryCoordRole))
}
