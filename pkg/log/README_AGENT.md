# log — AI Agent Logging Guide

- ALWAYS USE `github.com/milvus-io/milvus/pkg/v2/log` PACKAGE TO LOG.
- NEVER USE `zap` OR `log` PACKAGE DIRECTLY.

## Rules

1. Every log call must receive a `ctx context.Context`. Never pass `nil`. Choose ctx by priority: function parameter ctx > struct-level ctx (e.g. `s.ctx`) > `context.TODO()`.
2. If the current struct has a `*log.Logger` field, use it. Otherwise use package-level functions like `log.Info(ctx, ...)`.
3. When a predefined `FieldXxx` exists for a key, always use `FieldXxx(val)`. Never write `log.Int64("segment_id", v)`.
4. In loops or hot paths, use `Rated` variants: `log.RatedInfo(ctx, limit, msg, fields...)`.
5. For Debug logs on hot paths where field construction is expensive (`fmt.Sprintf`, serialization, iteration), guard with `LevelEnabled`.
6. `log.Any` has poor performance. Use only when the type is unknown.

## Logging

```go
// Package-level
log.Info(ctx, "segment loaded", log.FieldSegmentID(id), log.Duration("cost", d))
log.Error(ctx, "flush failed", log.Err(err))

// Logger method (when struct has *log.Logger)
l.Info(ctx, "search started", log.Int64("nq", nq))

// Rate-limited (loops / hot paths). limit = events per second; rate.Inf = unlimited
log.RatedWarn(ctx, 1.0, "lagging", log.Int64("gap", gap))

// LevelEnabled guard (hot path + expensive field construction)
if log.LevelEnabled(log.DebugLevel) {
    log.Debug(ctx, "detail", log.String("dump", strings.Join(paths, ",")))
}
```

**Choosing log level:**

| Level | When to use |
|---|---|
| `Debug` | Internal state details useful only during development or troubleshooting. Disabled in production by default. |
| `Info` | Normal operational events: startup, shutdown, configuration loaded, request completed, task finished. |
| `Warn` | Unexpected but recoverable situations: timeout retry, transient RPC failure with retry, fallback path taken, deprecated API called. |
| `Error` | Operation failed and cannot be completed: unrecoverable RPC failure, data corruption, invariant broken. Always attach `log.Err(err)`. |
| `Fatal` | Process cannot continue. Calls `os.Exit(1)`. Use only during initialization for unrecoverable setup failures. |
| `DPanic` / `Panic` | Reserved for "should never happen" invariant violations. Rarely used. |
Each level has a corresponding `Rated` variant. Logger methods have the same signature as package-level functions.

## Constructing Fields

Priority: `FieldXxx(val)` > typed constructor like `log.String(key, val)` > `log.Any(key, val)`.

**Predefined FieldXxx** (key is built-in; never write the key string manually):

| Function | Type | Built-in Key |
|---|---|---|
| `FieldNodeID(v)` | int64 | `node_id` |
| `FieldModule(v)` | string | `module` |
| `FieldTraceID(v)` | string | `trace_id` |
| `FieldSpanID(v)` | string | `span_id` |
| `FieldDbID(v)` | int64 | `db_id` |
| `FieldDbName(v)` | string | `db_name` |
| `FieldCollectionID(v)` | int64 | `collection_id` |
| `FieldCollectionName(v)` | string | `collection_name` |
| `FieldPartitionID(v)` | int64 | `partition_id` |
| `FieldPartitionName(v)` | string | `partition_name` |
| `FieldSegmentID(v)` | int64 | `segment_id` |
| `FieldIndexID(v)` | int64 | `index_id` |
| `FieldFieldID(v)` | int64 | `field_id` |
| `FieldTaskID(v)` | int64 | `task_id` |
| `FieldBroadcastID(v)` | int64 | `broadcast_id` |
| `FieldJobID(v)` | int64 | `job_id` |
| `FieldBuildID(v)` | int64 | `build_id` |
| `FieldVChannel(v)` | string | `vchannel` |
| `FieldPChannel(v)` | string | `pchannel` |
| `FieldMessageID(v)` | ObjectMarshaler | `message_id` |
| `FieldMessage(v)` | ObjectMarshaler | `message` |

**Generic typed constructors** (use when no predefined FieldXxx exists; function names match Go types):
`String` / `Int64` / `Int` / `Float64` / `Bool` / `Duration` / `Time` / `Stringer` / `Binary` / `Err` (key fixed to `"error"`), etc.
Each type has pointer variant `Xxxp` and slice variant `Xxxs`. See `field.go` for the full list.

## Binding Fields

```
Should the field follow the request chain (bind to ctx)?
├─ Yes → ctx = log.WithFields(ctx, fields...)
│        Lazily encoded; duplicate keys are overridden by later values.
│        To propagate across gRPC, add OptPropagated():
│          log.WithFields(ctx, log.FieldCollectionID(id, log.OptPropagated()))
│
└─ No  → Bind to a Logger
          ├─ Component-level (struct lifetime) → log.With(fields...) stored as a field
          ├─ Function-level (shared across multiple log calls in scope) → l := log.With(fields...) as local var
          └─ Fields may be filtered by level → log.WithLazy(fields...) — lazily encoded
```

```go
// Bind to ctx at request entry point
ctx = log.WithFields(ctx, log.FieldCollectionID(collID), log.String("request_id", reqID))

// Bind to Logger at component construction
l := log.With(log.FieldModule("querynode"), log.FieldNodeID(nodeID))

// Local Logger to eliminate repeated fields within a function
func (s *compactor) compact(ctx context.Context, segID int64, plan *Plan) error {
    l := log.With(log.FieldSegmentID(segID), log.Int64("planID", plan.ID))
    l.Info(ctx, "compact start")
    // ...
    l.Info(ctx, "compact done", log.Duration("cost", elapsed))
    return nil
}
```
