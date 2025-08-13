package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	cfg, err := newConfig()
	assert.Error(t, err)

	cfg, err = newConfig(OptClientRootPath("/test"))
	assert.NoError(t, err)
	assert.Equal(t, cfg.RootPath, "/test")
}
