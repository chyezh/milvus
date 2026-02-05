# WAL State Synchronization Design

## Overview

本文档描述了 WAL 管理操作与 RPC 超时解耦的设计方案。通过新增 Server Streaming RPC 实现 StreamingNode 和 StreamingCoord 之间的状态同步，替换原有的 Assign Unary RPC 接口。

## Background

### Problem

当前 WAL assign/remove 操作受 `streaming.operationTimeout` 控制。当 StreamingNode 需要从 recovery-storage 恢复大量消息时，恢复过程可能超过 RPC 超时限制，触发 `recover-timeout-backoff` 循环，导致：

1. 恢复期间 WAL 分配失败
2. 重启后集群不稳定
3. 不必要的重试和延迟

### Root Cause

WAL 管理操作与 RPC 超时语义耦合。Recovery 操作是长时间运行的，其持续时间取决于数据量而非网络延迟。

### Solution

使用 Server Streaming RPC 实现状态上报机制：
- Assign 操作改用 Stream RPC，Node 持续上报恢复进度
- Stream 生命周期与 Assign 流程解耦，Stream 断开不影响 Recovery
- Remove 保持 Unary RPC 不变

## Design

### 1. Interface Design Summary

| Item | Decision |
|------|----------|
| RPC Type | Server Streaming |
| Granularity | Per-Channel |
| New Interface | `AssignWithStateReport` |
| Legacy Interface | Keep `Assign` Unary RPC (compatibility) |
| Remove Interface | Keep Unary RPC unchanged |

### 2. Proto Definition

```protobuf
service StreamingNodeManagerService {
    // ===== Legacy Interfaces (Compatibility) =====

    // Assign - Legacy Unary RPC, kept for cross-version compatibility
    rpc Assign(StreamingNodeManagerAssignRequest)
        returns (StreamingNodeManagerAssignResponse) {};

    // Remove - Unchanged
    rpc Remove(StreamingNodeManagerRemoveRequest)
        returns (StreamingNodeManagerRemoveResponse) {};

    // CollectStatus - Unchanged
    rpc CollectStatus(StreamingNodeManagerCollectStatusRequest)
        returns (StreamingNodeManagerCollectStatusResponse) {};

    // ===== New Interface =====

    // AssignWithStateReport - Server Streaming RPC
    // Node reports Ready or Error then sends EOF to close Stream
    rpc AssignWithStateReport(StreamingNodeManagerAssignRequest)
        returns (stream AssignmentStateResponse) {};
}

// ===== New Messages =====

message AssignmentStateResponse {
    oneof response {
        AssignmentProgress progress = 1;  // Intermediate state: recovery progress
        StreamingError error = 2;         // Terminal state: failed
        AssignmentReady ready = 3;        // Terminal state: ready
    }
}

enum AssignmentState {
    ASSIGNMENT_STATE_UNKNOWN = 0;
    ASSIGNMENT_STATE_FENCING = 1;              // Fencing old WAL Owner
    ASSIGNMENT_STATE_PERSIST_RECOVERING = 2;   // Loading metadata from Catalog
    ASSIGNMENT_STATE_STREAM_RECOVERING = 3;    // Replaying messages from WAL stream
}

message AssignmentProgress {
    AssignmentState state = 1;
    StreamRecoveringProgress stream_recovering_progress = 2;  // Valid only in STREAM_RECOVERING
}

message StreamRecoveringProgress {
    int64 recovered_bytes = 1;
    int64 total_bytes = 2;           // -1 if unknown
    int64 recovered_messages = 3;
    int64 total_messages = 4;        // -1 if unknown
}

message AssignmentReady {}
```

### 3. State Flow

```
┌─────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌───────┐
│ FENCING │───▶│ PERSIST_RECOVERING│───▶│ STREAM_RECOVERING│───▶│ READY │──▶ EOF
└─────────┘    └──────────────────┘    └──────────────────┘    └───────┘

Error at any stage ──▶ StreamingError ──▶ EOF
```

### 4. State Reporting Mechanism

| Item | Decision |
|------|----------|
| Trigger | Timer + Event driven |
| Terminal State | Ready or Error, Server sends EOF after reporting |
| Term Validation Failure | Return StreamingError + EOF |

### 5. ManagerClient Behavior

| Item | Decision |
|------|----------|
| External Interface | Unchanged (`Assign()`, `Remove()`) |
| Internal Implementation | `Assign()` uses `AssignWithStateReport` internally |
| Auto Reconnect | Supported, exponential backoff (1s~30s), unlimited retries |
| Version Compatibility | Try new interface first, fallback to legacy if Unimplemented |
| Internal State | Not maintained, caller guarantees serial calls |

### 6. Coordinator Restart Recovery

| Item | Decision |
|------|----------|
| Recovery Logic Location | Balancer layer |
| Recovery Method | Check ETCD state, re-call Assign() for Assigning channels |

### 7. Node-side Behavior

| WAL State when receiving AssignWithStateReport | Behavior |
|-----------------------------------------------|----------|
| Not exists | Start new Assign flow |
| Recovering | Continue reporting current progress |
| Already Ready | Return Ready + EOF directly |
| Term mismatch | Return StreamingError + EOF |

## Sequence Diagrams

### Normal Assign Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│              AssignWithStateReport (Server Streaming)                    │
│                                                                          │
│   Coord                                                         Node     │
│     │                                                             │      │
│     │─── AssignRequest (pchannel) ───────────────────────────────▶│      │
│     │                                                             │      │
│     │◀── progress: {state: FENCING} ──────────────────────────────│      │
│     │◀── progress: {state: PERSIST_RECOVERING} ───────────────────│      │
│     │◀── progress: {state: STREAM_RECOVERING, progress: 10%} ─────│      │
│     │◀── progress: {state: STREAM_RECOVERING, progress: 50%} ─────│      │
│     │◀── progress: {state: STREAM_RECOVERING, progress: 100%} ────│      │
│     │◀── ready: {} ───────────────────────────────────────────────│      │
│     │◀── EOF ─────────────────────────────────────────────────────│      │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Coordinator Restart During Recovery

```
┌─────────────────────────────────────────────────────────────────────────┐
│              Stream Decoupled from Assign Flow                           │
│                                                                          │
│   ETCD State: Assigning                                                  │
│       │                                                                  │
│       │    Coord                        Stream                  Node     │
│       │      │                                                    │      │
│       │      │────── AssignRequest ──────────────────────────────▶│      │
│       │      │◀───── progress: STREAM_RECOVERING (50%) ───────────│      │
│       │      │                                                    │      │
│       │      ╳ Coord crashes                                      │      │
│       │                                                           │      │
│       │                              (Node continues Recovery)    │      │
│       │                                                           │      │
│       │    New Coord                                              │      │
│       │      │                                                    │      │
│       │      │────── Reconnect AssignRequest ────────────────────▶│      │
│       │      │◀───── progress: STREAM_RECOVERING (80%) ───────────│      │
│       │      │◀───── ready ───────────────────────────────────────│      │
│       │      │◀───── EOF ─────────────────────────────────────────│      │
│       │                                                           │      │
│       ▼                                                                  │
│   ETCD State: Assigned                                                   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### ManagerClient Auto Reconnect

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    ManagerClient.Assign() Internal Logic                 │
│                                                                          │
│   Balancer                    ManagerClient                     Node     │
│     │                              │                              │      │
│     │── Assign(pchannel) ─────────▶│                              │      │
│     │                              │                              │      │
│     │                              │─── Stream RPC ──────────────▶│      │
│     │                              │◀── progress ─────────────────│      │
│     │                              │◀── progress ─────────────────│      │
│     │                              │                              │      │
│     │                              │    ╳ Stream disconnected     │      │
│     │                              │                              │      │
│     │                              │─── Auto reconnect Stream ───▶│      │
│     │                              │◀── progress ─────────────────│      │
│     │                              │◀── ready ────────────────────│      │
│     │                              │◀── EOF ──────────────────────│      │
│     │                              │                              │      │
│     │◀── return nil ──────────────│                              │      │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Reconnect Strategy

| Config | Value | Description |
|--------|-------|-------------|
| Reconnect Interval | Exponential backoff (1s, 2s, 4s, ... max 30s) | Avoid frequent reconnects |
| Max Retries | Unlimited | Until Ready/Error or ctx cancelled |

### Reconnect Behavior

| Scenario | Behavior |
|----------|----------|
| ctx.Done() triggered | Stop reconnect, return ctx.Err() |
| Node returns StreamingError | Stop reconnect, return error |
| Stream EOF without Ready/Error | Treat as abnormal disconnect, trigger reconnect |

## Version Compatibility

ManagerClient supports both new and legacy StreamingNode versions:

| Node Version | Behavior |
|--------------|----------|
| New Version | Use `AssignWithStateReport` Stream RPC |
| Legacy Version | Fallback to legacy `Assign` Unary RPC |

Detection method: Try new interface first, fallback if Unimplemented error returned.

## Related Components

| Component | File Path |
|-----------|-----------|
| Proto Definition | `pkg/proto/streaming.proto` |
| ManagerClient Interface | `internal/streamingnode/client/manager/manager_client.go` |
| ManagerClient Implementation | `internal/streamingnode/client/manager/manager_client_impl.go` |
| ManagerService (Server) | `internal/streamingnode/server/service/manager.go` |
| Balancer Implementation | `internal/streamingcoord/server/balancer/balancer_impl.go` |
| WAL Opener | `internal/streamingnode/server/wal/adaptor/opener.go` |
| Recovery Storage | `internal/streamingnode/server/wal/recovery/recovery_storage_impl.go` |

## Key Configuration

- `streaming.operationTimeout` - Legacy timeout config, still used for Remove RPC
- Term field in `PChannelInfo` - Monotonically increasing, ensures operation consistency

## Implementation Status

- [x] Proto definitions added (Task 1)
- [x] Server-side AssignWithStateReport RPC (Task 2)
- [x] WAL Manager OpenOption with StateReporter (Task 3-4)
- [x] Client-side with auto-reconnect and fallback (Task 5)
- [x] Unit tests for server components (Task 6)
- [x] Unit tests for client components (Task 7)
- [x] WAL opener state reporting (Task 9)
- [x] Final verification (Task 10)

Implementation Date: 2026-02-05

## Notes

- Created: 2026-02-05
- Related Issue: #46370
- Milestone: Milvus 3.0
