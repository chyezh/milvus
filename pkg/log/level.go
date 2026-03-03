package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Level is an alias for zapcore.Level
type Level = zapcore.Level

// Re-export level constants for convenience
const (
	DebugLevel  = zapcore.DebugLevel
	InfoLevel   = zapcore.InfoLevel
	WarnLevel   = zapcore.WarnLevel
	ErrorLevel  = zapcore.ErrorLevel
	DPanicLevel = zapcore.DPanicLevel
	PanicLevel  = zapcore.PanicLevel
	FatalLevel  = zapcore.FatalLevel
)

// ParseLevel parses a level string into a Level value.
// It is a re-export of zapcore.ParseLevel.
var ParseLevel = zapcore.ParseLevel

// globalLevel allows runtime level changes
var globalLevel = zap.NewAtomicLevelAt(InfoLevel)

// SetLevel changes the log level at runtime.
// This affects all loggers created with the default config.
// For custom loggers passed via Init(), the caller should
// manage their own AtomicLevel.
func SetLevel(level Level) {
	globalLevel.SetLevel(level)
}

// GetLevel returns the current log level.
func GetLevel() Level {
	return globalLevel.Level()
}

// LevelEnabled reports whether a message at the given level would be logged.
// Use this to guard expensive field construction on hot paths:
//
//	if log.LevelEnabled(log.DebugLevel) {
//	    log.Debug(ctx, "details", log.String("dump", expensiveDump()))
//	}
func LevelEnabled(level Level) bool {
	return globalLevel.Enabled(level)
}

// GetAtomicLevel returns the AtomicLevel for integration with custom configs.
// Callers can use this when building their own zap.Config:
//
//	cfg.Level = log.GetAtomicLevel()
func GetAtomicLevel() zap.AtomicLevel {
	return globalLevel
}
