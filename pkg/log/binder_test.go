//go:build test

package log

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinder_DefaultLogger(t *testing.T) {
	b := &Binder{}
	l := b.Logger()
	assert.NotNil(t, l)
	// Should be usable without panic.
	l.Info(context.Background(), "default logger works")
}

func TestBinder_SetAndGetLogger(t *testing.T) {
	b := &Binder{}
	custom := With(FieldModule("test_module"))
	b.SetLogger(custom)

	l := b.Logger()
	assert.Equal(t, custom, l)
	l.Info(context.Background(), "custom logger works")
}

func TestBinder_SetNilFallsBackToDefault(t *testing.T) {
	b := &Binder{}
	b.SetLogger(With(FieldModule("temp")))
	b.SetLogger(nil)

	l := b.Logger()
	assert.NotNil(t, l)
	l.Info(context.Background(), "nil reset works")
}

func TestBinder_EmbeddedInStruct(t *testing.T) {
	type component struct {
		Binder
	}

	c := &component{}
	// Default logger should work.
	c.Logger().Debug(context.Background(), "embedded default")

	// Set a custom logger.
	c.SetLogger(With(FieldModule("component"), FieldNodeID(42)))
	c.Logger().Info(context.Background(), "embedded custom")
}

func TestBinder_InterfaceCompliance(t *testing.T) {
	var wl WithLogger = &Binder{}
	assert.NotNil(t, wl.Logger())

	var lb LoggerBinder = &Binder{}
	lb.SetLogger(With(FieldModule("iface_test")))
}

func TestBinder_ConcurrentAccess(t *testing.T) {
	b := &Binder{}
	done := make(chan struct{})

	// Concurrent writers.
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				b.SetLogger(With(FieldModule("writer")))
			}
		}()
	}

	// Concurrent readers.
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				l := b.Logger()
				assert.NotNil(t, l)
			}
		}()
	}

	for i := 0; i < 20; i++ {
		<-done
	}
}
