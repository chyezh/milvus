// Package log provides a context-aware logging library built on zap.
//
// It enforces context usage in all log operations and supports scoped field
// attachment via context. Context fields accumulate across WithFields calls,
// allowing child contexts to inherit parent fields.
//
// Basic usage:
//
//	ctx := context.Background()
//	log.Info(ctx, "request received", log.String("path", "/api/search"))
//
// Node-level initialization (once per process):
//
//	logger, _ := zap.NewProduction()
//	log.InitNode(logger, nodeId) // nodeId included in all log entries
//
// Scoped context logging:
//
//	ctx = log.WithFields(ctx, log.String("request_id", "abc123"))
//	ctx = log.WithFields(ctx, log.Int64("user_id", 42))
//	log.Info(ctx, "processing") // includes both request_id and user_id
//
// Cross-node field propagation via gRPC:
//
//	// Attach fields that propagate across RPC calls
//	ctx = log.WithFields(ctx,
//	    log.FieldCollectionName("my_collection", log.OptPropagated()),
//	    log.FieldCollectionID(12345, log.OptPropagated()),
//	)
//
//	// Use gRPC interceptors to automatically propagate fields
//	// Server side: log.UnaryServerInterceptor("modulename")
//	// Client side: log.UnaryClientInterceptor()
//
// Runtime log level changes:
//
//	log.SetLevel(log.DebugLevel)
//
// Custom logger initialization:
//
//	logger, _ := zap.NewDevelopment()
//	log.Init(logger)
package log
