package mlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/milvus-io/milvus/pkg/v2/mlog/logcore"
)

// Re-export config types from logcore for convenience.
type (
	// FileLogConfig serializes file log related config in toml/json.
	FileLogConfig = logcore.FileLogConfig

	// Config serializes log related config in toml/json.
	Config = logcore.Config

	// ZapProperties records some information about zap.
	ZapProperties = logcore.ZapProperties
)

// InitLogger initializes the global mlog logger from config.
// It creates the underlying zap logger with file/stdout outputs and sets it as
// the global logger. The returned cleanup function should be called on shutdown.
//
// Usage:
//
//	cfg := &mlog.Config{Level: "info", Stdout: true}
//	cleanup, err := mlog.InitLogger(cfg)
//	if err != nil {
//	    panic(err)
//	}
//	defer cleanup()
func InitLogger(cfg *Config, opts ...zap.Option) (func(), error) {
	logger, props, cleanup, err := logcore.InitLogger(cfg, opts...)
	if err != nil {
		return cleanup, err
	}
	// Set the global mlog logger (AddCallerSkip(1) is applied inside Init)
	globalLogger.Store(logger)
	// Sync the atomic level so mlog.SetLevel/GetLevel works with this logger
	globalLevel = props.Level
	// Register cleanup so mlog.Cleanup() works
	registerCleanup(cleanup)
	return cleanup, nil
}

// InitLoggerWithWriteSyncer initializes the global mlog logger with a custom write syncer.
// The returned cleanup function should be called on shutdown.
func InitLoggerWithWriteSyncer(cfg *Config, output zapcore.WriteSyncer, opts ...zap.Option) (func(), error) {
	logger, props, cleanup, err := logcore.InitLoggerWithWriteSyncer(cfg, output, opts...)
	if err != nil {
		return cleanup, err
	}
	globalLogger.Store(logger)
	globalLevel = props.Level
	registerCleanup(cleanup)
	return cleanup, nil
}

// InitTestLogger initializes the global mlog logger for unit tests.
// The returned cleanup function should be called on test cleanup.
func InitTestLogger(t zaptest.TestingT, cfg *Config, opts ...zap.Option) (func(), error) {
	logger, props, cleanup, err := logcore.InitTestLogger(t, cfg, opts...)
	if err != nil {
		return cleanup, err
	}
	globalLogger.Store(logger)
	globalLevel = props.Level
	registerCleanup(cleanup)
	return cleanup, nil
}

// NewLogger creates a new zap logger from config without setting it as global.
// Use this when you need a standalone logger that doesn't affect the global state.
// The returned cleanup function should be called on shutdown.
func NewLogger(cfg *Config, opts ...zap.Option) (*zap.Logger, *ZapProperties, func(), error) {
	return logcore.InitLogger(cfg, opts...)
}
