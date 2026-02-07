//go:build test

package mlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinderImplementsInterfaces(t *testing.T) {
	var _ WithLogger = &Binder{}
	var _ LoggerBinder = &Binder{}
}

func TestBinderDefaultLogger(t *testing.T) {
	b := &Binder{}
	// Should return a non-nil logger even without SetLogger
	l := b.Logger()
	assert.NotNil(t, l)
}

func TestBinderSetAndGetLogger(t *testing.T) {
	b := &Binder{}
	logger := With(String("module", "test"))
	b.SetLogger(logger)

	got := b.Logger()
	assert.Equal(t, logger, got)
}

func TestBinderOverwriteLogger(t *testing.T) {
	b := &Binder{}
	logger1 := With(String("module", "first"))
	logger2 := With(String("module", "second"))

	b.SetLogger(logger1)
	assert.Equal(t, logger1, b.Logger())

	b.SetLogger(logger2)
	assert.Equal(t, logger2, b.Logger())
}
