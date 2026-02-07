package mlog

// compat.go re-exports types and functions from pkg/log so that consumers
// only need to import pkg/mlog.  All values delegate to pkg/log internally;
// this file exists solely to eliminate consumer imports of pkg/v2/log.

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// ---------- Config types ----------

// Config re-exports log.Config (serializes log-related config in toml/json).
type Config = log.Config

// FileLogConfig re-exports log.FileLogConfig.
type FileLogConfig = log.FileLogConfig

// ZapProperties re-exports log.ZapProperties.
type ZapProperties = log.ZapProperties

// ---------- Initialization ----------

// InitLogger initializes a zap logger from Config.
var InitLogger = log.InitLogger

// InitTestLogger initializes a logger suitable for unit tests.
var InitTestLogger = log.InitTestLogger

// Cleanup flushes buffered log entries then runs any registered cleanup.
var Cleanup = log.Cleanup

// ReplaceGlobals replaces the global logger in *both* pkg/log and mlog
// so that old-style callers (grpc interceptor, CGO bridge) and new-style
// callers all see the same underlying logger.
func ReplaceGlobals(logger *zap.Logger, props *ZapProperties) {
	log.ReplaceGlobals(logger, props)
	Init(logger)
	SetLevel(props.Level.Level())
}

// ---------- CGO types ----------

// CEntry is the C-compatible log entry used by CGO bridge code.
type CEntry = log.CEntry

// CEntryTextIOCore is the interface for async text IO cores that accept CEntry.
type CEntryTextIOCore = log.CEntryTextIOCore

// ---------- Per-request log level overrides (gRPC interceptor) ----------
// These functions manipulate the legacy CtxLogKey-based context used by the
// gRPC interceptor.  They are thin re-exports of pkg/log functions.

var (
	WithDebugLevel = log.WithDebugLevel
	WithInfoLevel  = log.WithInfoLevel
	WithWarnLevel  = log.WithWarnLevel
	WithErrorLevel = log.WithErrorLevel
	WithFatalLevel = log.WithFatalLevel
)

// ---------- Legacy context enrichment ----------
// These work with the legacy CtxLogKey context used by the gRPC interceptor
// and old-style callers.  New code should use mlog.WithFields instead.

// LegacyWithFields attaches fields to the legacy log context (CtxLogKey).
// Use this only in gRPC interceptor code that must bridge old and new systems.
var LegacyWithFields = log.WithFields

// WithTraceID attaches a traceID to the legacy log context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return log.WithTraceID(ctx, traceID)
}

// WithReqID attaches a reqID to the legacy log context.
func WithReqID(ctx context.Context, reqID int64) context.Context {
	return log.WithReqID(ctx, reqID)
}

// ---------- Legacy logger access ----------

// LegacyCtx returns the legacy MLogger from context.
// Use this only in infrastructure code that must bridge old and new systems.
var LegacyCtx = log.Ctx

// LegacyLevel returns the legacy AtomicLevel.
var LegacyLevel = log.Level

// LegacySetLevel sets the level on the legacy logger.
var LegacySetLevel = log.SetLevel

// LegacyGetLevel gets the level from the legacy logger.
var LegacyGetLevel = log.GetLevel
