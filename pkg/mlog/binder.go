package mlog

import "sync/atomic"

var (
	_ WithLogger   = &Binder{}
	_ LoggerBinder = &Binder{}
)

// WithLogger is an interface for accessing a component-level logger.
type WithLogger interface {
	Logger() *Logger
}

// LoggerBinder is an interface for setting a component-level logger.
type LoggerBinder interface {
	SetLogger(logger *Logger)
}

// Binder is an embeddable type that implements WithLogger and LoggerBinder.
// It stores a *Logger atomically for concurrent-safe access.
//
// Usage:
//
//	type MyComponent struct {
//	    mlog.Binder
//	    // ...
//	}
//
//	c := &MyComponent{}
//	c.SetLogger(mlog.With(mlog.FieldModule("my_component")))
//	c.Logger().Info(ctx, "started")
type Binder struct {
	logger atomic.Pointer[Logger]
}

// SetLogger sets the component-level logger.
func (b *Binder) SetLogger(logger *Logger) {
	b.logger.Store(logger)
}

// Logger returns the component-level logger.
// If no logger has been set, it returns a default Logger from the global logger.
func (b *Binder) Logger() *Logger {
	l := b.logger.Load()
	if l == nil {
		return With()
	}
	return l
}
