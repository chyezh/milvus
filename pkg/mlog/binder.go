package mlog

import "sync/atomic"

var (
	_ WithLogger   = &Binder{}
	_ LoggerBinder = &Binder{}
)

// WithLogger is an interface to access a component's logger.
type WithLogger interface {
	Logger() *Logger
}

// LoggerBinder is an interface to set a component's logger.
type LoggerBinder interface {
	SetLogger(logger *Logger)
}

// Binder is an embeddable type that provides thread-safe access to a Logger.
// Components embed Binder to hold their own Logger instance:
//
//	type MyComponent struct {
//	    mlog.Binder
//	    // ...
//	}
type Binder struct {
	logger atomic.Pointer[Logger]
}

// SetLogger sets the logger on the Binder.
func (b *Binder) SetLogger(logger *Logger) {
	b.logger.Store(logger)
}

// Logger returns the logger from the Binder.
// If no logger has been set, returns a default Logger from With().
func (b *Binder) Logger() *Logger {
	l := b.logger.Load()
	if l == nil {
		return With()
	}
	return l
}
