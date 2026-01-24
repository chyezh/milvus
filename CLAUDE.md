# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Milvus is a high-performance distributed vector database built for AI applications. Written in Go and C++, it implements a cloud-native, distributed architecture optimized for vector similarity search at scale.

**Key characteristics:**
- Hybrid Go/C++ codebase (Go for distributed coordination, C++ for performance-critical vector operations)
- Microservices architecture with coordinator-node topology
- Supports both standalone and distributed deployment modes
- Uses etcd for metadata storage and cluster coordination
- Object storage backends (MinIO, S3, Azure, GCP) for data persistence

## Build Commands

### Prerequisites

```bash
# always source the environment variables
source ./scripts/setenv.sh
```

Don't use grep, use rg instead.

### Building Milvus

```bash

# If there are only changes in the Go code, you can build only the Go code to speed up the build process
make build-go

# If there are ont only changes in the Go code, you can build only the Go code to speed up the build process
make milvus SKIP_3RDPARTY=1
```

### Testing

```bash
# Run all tests (C++ and Go)
make unittest

# C++ unit tests
make test-cpp

# Go unit tests
make test-go

# Run specific Go component tests
make test-proxy
make test-datacoord
make test-datanode
make test-querycoord
make test-querynode
make test-rootcoord

# Integration tests
make integration-test

# Run single C++ test with filter
make run-test-cpp filter=<test_name_pattern>

# Code coverage
make codecov          # Both Go and C++
make codecov-go       # Go coverage only
make codecov-cpp      # C++ coverage only
```

### Code Quality

```bash
# Format and lint checking
make verifiers        # Run all checks (C++, Rust, Go fmt, static-check)

# Individual checks
make cppcheck         # C++ code formatting check
make rustfmt          # Rust code formatting
make fmt              # Go code formatting check
make static-check     # Go static analysis with golangci-lint

# Auto-fix formatting and linting issues
make lint-fix         # Fix Go formatting and linting
```

### Protocol Buffers and Code Generation

```bash
# Generate protobuf files
make generated-proto

# Generate without rebuilding C++ dependencies
make generated-proto-without-cpp

# Generate mockery mocks for testing
make generate-mockery                    # All mocks
make generate-mockery-querynode          # Specific component
make generate-mockery-datacoord
make generate-mockery-proxy

# Generate message codegen
make generate-message-codegen
```

## High-Level Architecture

Milvus follows a **microservices coordinator-node architecture** where coordinators manage metadata and orchestration while nodes execute actual workloads.

### Component Topology

**Coordinators (Control Plane)**
- **RootCoord** (`internal/rootcoord/`) - Manages global DDL operations (create/drop collection, partition), maintains cluster-wide metadata
- **DataCoord** (`internal/datacoord/`) - Manages data segments, compaction strategies, import operations, segment allocation and lifecycle
- **QueryCoord** (`internal/querycoordv2/`) - Manages query load balancing, replica distribution, segment assignment to query nodes
- **MixCoord** (`internal/coordinator/`) - Unified coordinator combining all three (used in standalone mode)

**Execution Nodes (Data Plane)**
- **Proxy** (`internal/proxy/`) - Client-facing gateway, handles authentication, request routing, result aggregation
- **DataNode** (`internal/datanode/`) - Data ingestion, segment flushing, compaction execution
- **QueryNode** (`internal/querynodev2/`) - Executes search/query requests, manages loaded segments and indices
- **StreamingNode** (`internal/streamingnode/`) - Manages Write-Ahead Log (WAL) and message streaming (newer streaming architecture)

### Go-C++ Integration

The codebase bridges Go and C++ through CGO:

- **Go Layer** (`internal/*/`): Distributed coordination, RPC, metadata management, workflow orchestration
- **C++ Core** (`internal/core/`): Vector search algorithms, index implementations (HNSW, IVF, DiskANN), SIMD-optimized operations, segment data structures
- **CGO Bindings** (`internal/util/cgo/`, `internal/util/segcore/`, `internal/util/indexcgowrapper/`): Wrapper interfaces for Go to call C++ functions

**Data flow example:**
1. Proxy receives search request (Go)
2. Proxy routes to QueryNode (Go RPC)
3. QueryNode calls C++ `SegmentSearch` via CGO
4. C++ performs SIMD-optimized vector distance computation
5. Results marshaled back to Go and returned to client

### Key Directories

**Core Business Logic:**
- `internal/rootcoord/meta_table.go` - Collection/partition metadata management
- `internal/datacoord/meta.go` - Segment metadata and state tracking
- `internal/querynodev2/services.go` - Query request handling
- `internal/proxy/` - Client request entry point
- `internal/storage/` - Data serialization, binlog I/O, cloud storage backends

**Infrastructure:**
- `internal/metastore/` - Metadata persistence abstraction (etcd/TiKV)
- `internal/util/dependency/` - Dependency injection framework
- `internal/util/flowgraph/` - Event-driven pipeline processing
- `internal/distributed/` - RPC client implementations
- `internal/types/` - Component interface definitions

**C++ Core:**
- `internal/core/src/segcore/` - Segment execution engine
- `internal/core/src/index/` - Vector index implementations
- `internal/core/src/query/` - Query execution primitives
- `internal/core/src/storage/` - C++ storage layer

**Testing:**
- `tests/integration/` - Go integration tests (34+ test suites)
- `tests/python_client/` - Python SDK tests
- `*_test.go` - Unit tests alongside code
- `internal/mocks/` - Generated mockery mocks

## Development Patterns

### Component Lifecycle

All components follow a consistent lifecycle pattern (see `cmd/roles/roles.go`):
```go
component.Prepare()  // Initialize dependencies, can run in parallel
component.Run()      // Start serving
component.Stop()     // Graceful shutdown
```

### Deployment Modes

1. **Standalone Mode**: All components in single process (set `ENABLE_MIXCOORD=true`)
2. **Distributed Mode**: Components across processes, gRPC communication between services

### Metadata Architecture

- **etcd**: Primary metadata store, service discovery, leader election
- **TiKV**: Alternative distributed KV backend (optional)
- **Abstraction**: `internal/metastore/` provides unified interface for both backends

### Message Streaming

- Components communicate via message queues (Pulsar/Kafka supported)
- FlowGraph pattern (`internal/util/flowgraph/`) for pipeline processing
- Write-Ahead Log (WAL) for durability

### Testing Strategy

**Unit Tests:**
- Use mockery for dependency mocking (`.mockery.yaml` configs in component directories)
- Run component-specific tests: `make test-<component>`
- Test files co-located with source code (`*_test.go`)

**Integration Tests:**
- Located in `tests/integration/`
- Test inter-component interactions, end-to-end workflows
- Use `MiniClusterSuite` for in-process cluster testing
- Run with: `make integration-test`

**C++ Tests:**
- Use Google Test framework
- Located in `internal/core/unittest/`
- Filter specific tests: `make run-test-cpp filter=SegCore*`

## Working with the Codebase

### Making Changes to Components

When modifying a coordinator or node:
1. Read component interfaces in `internal/types/types.go`
2. Update interface implementations in component directory
3. Regenerate mocks if interface changed: `make generate-mockery-<component>`
4. Run component tests: `make test-<component>`
5. Run integration tests: `make integration-test`

### Adding Protobuf Fields

1. Update `.proto` files in Milvus proto repository
2. Run `make download-milvus-proto` to sync latest proto definitions
3. Run `make generated-proto` to regenerate Go/C++ code
4. Update serialization logic in `internal/storage/` if needed

### C++ Development

1. C++ code uses CMake build system
2. Build C++ separately: `make build-cpp`
3. C++ dependencies managed via conan
4. Format C++ code: `make cppcheck`
5. Build modes: `mode=Release` (default) or `mode=Debug`

### Modifying Dependencies

**Go dependencies:**
```bash
go mod tidy              # Update go.mod/go.sum
```

**C++ dependencies:**
- Defined in `internal/core/conanfile.py`
- Managed by conan package manager

### Running Locally

```bash
# Build Milvus
make milvus

# Start dependencies (etcd, MinIO)
docker-compose up -d

# Run Milvus standalone
./bin/milvus run standalone

# Or run specific component
./bin/milvus run rootcoord
./bin/milvus run datacoord
./bin/milvus run querycoord
./bin/milvus run proxy
./bin/milvus run datanode
./bin/milvus run querynode
```

### Common Development Workflows

**Adding a new RPC method:**
1. Add method to proto definition
2. Regenerate proto: `make generated-proto`
3. Implement in component's service implementation
4. Add client call in distributed client (`internal/distributed/`)
5. Add tests

**Fixing a bug:**
1. Add regression test first
2. Fix the bug
3. Verify test passes
4. Run full test suite: `make test-<component>`

**Performance optimization:**
1. Profile using built-in metrics (Prometheus)
2. For query performance: optimize C++ code in `internal/core/`
3. For coordination performance: optimize Go code in coordinators
4. Benchmark before/after changes

## Important Notes

- **Protobuf**: Never manually edit generated `*.pb.go` or `*.pb.h` files - always regenerate from `.proto` sources
- **Mocks**: Regenerate mocks after interface changes using `make generate-mockery-<component>`
- **CGO**: When modifying Go-C++ boundaries, ensure proper memory management (Go GC doesn't manage C++ memory)
- **Build Tags**: Milvus uses build tags `dynamic` and `sonic` - ensure they're included when building
- **RPATH**: The build sets library RPATH for finding shared libraries - see `scripts/setenv.sh`
- **Coordinator HA**: RootCoord/DataCoord/QueryCoord support active-standby failover via etcd
- **Segment States**: Segments transition through states (Growing -> Sealed -> Flushed -> Indexed) - understand this lifecycle when working on data path
- **Time Travel**: Milvus supports MVCC time travel - be aware of timestamp semantics in metadata operations

## Resources

- **Documentation**: https://milvus.io/docs
- **Architecture**: https://milvus.io/docs/architecture_overview.md
- **Development Guide**: See `DEVELOPMENT.md` in repo root
- **Contributing**: See `CONTRIBUTING.md` for PR process and coding standards
- **Discord Community**: https://discord.gg/mKc3R95yE5
