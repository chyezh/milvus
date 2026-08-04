# Fact Collection 与 Aggregate Materialized View 设计

> 状态：Proposed
>
> 本文从用户能力和数据布局重新定义事实表型 Collection、Aggregate Materialized View（AMV）、高聚合 Balance 和嵌套查询。本文不继承旧方案中通过大量 `create_collection` 可选参数组合能力的设计。

## 1. 功能目标与动机

### 1.1 典型用户需求场景：Agent Tracing

本文重点面向 [Agent Tracing](https://zilliverse.feishu.cn/docx/Sb6FdTFz1oipDpxOw3ncgp38ned) 场景。一次 Agent 运行通常是一棵由 Trace、Span、Event 和 Score 组成的调用树：

```text
Trace
  -> Agent Span
       -> Generation Span
       -> Tool / Retrieval Span
       -> Event / Score
```

数据同时包含 Trace/Span 标识、父子关系、时间、模型、Token、成本、多租户 Metadata、大文本、Embedding 和质量评分。大量分布式 SDK 持续追加这些事实，同一个 Trace 的记录可能迟到、乱序、重复或缺失结束事件。

### 1.2 典型查询场景

以下查询使用类 SQL 语法描述业务语义，不表示 Milvus 需要提供 SQL 接口。

按 `trace_id` 读取全部记录，并通过 `parent_id` 和时间还原调用树：

```sql
SELECT *
FROM agent_events
WHERE trace_id = 'trace_123'
ORDER BY started_at, span_id;
```

按 Prompt、Response、Tool Output 或 Error 做全文检索，先定位命中的 Trace，再读取这些 Trace 的全部记录：

```sql
SELECT *
FROM agent_events
WHERE trace_id IN (
    SELECT trace_id
    FROM agent_events
    WHERE event_type IN ('PROMPT', 'RESPONSE', 'TOOL_OUTPUT', 'ERROR')
      AND (
          TEXT_MATCH(content, 'rate limit exceeded')
          OR TEXT_MATCH(error_message, 'rate limit exceeded')
      )
    GROUP BY trace_id
    ORDER BY MAX(started_at) DESC
    LIMIT 100
)
ORDER BY trace_id, started_at, span_id;
```

按时间和模型统计调用量、Token、成本、错误率和 P95 延迟：

```sql
SELECT
    TIME_BUCKET('1 hour', started_at) AS hour,
    model,
    COUNT(*) AS call_count,
    SUM(total_tokens) AS total_tokens,
    SUM(cost) AS total_cost,
    COUNT_IF(status = 'ERROR') AS error_count,
    PERCENTILE(latency_ms, 0.95) AS p95_latency
FROM agent_events
WHERE started_at >= '2026-08-01'
GROUP BY hour, model
ORDER BY hour, model;
```

筛选高成本且没有错误的 Trace，再在对应 Span 中执行向量搜索：

```sql
SELECT trace_id, span_id, content
FROM agent_events
WHERE trace_id IN (
    SELECT trace_id
    FROM agent_events
    GROUP BY trace_id
    HAVING SUM(total_tokens) > 100000
       AND COUNT_IF(status = 'ERROR') = 0
)
ORDER BY VECTOR_DISTANCE(embedding, :query_vector)
LIMIT 100;
```

先筛选高消耗项目，再筛选其中的异常 Trace，最后查询 Trace 明细：

```sql
SELECT *
FROM agent_events
WHERE trace_id IN (
    SELECT trace_id
    FROM agent_events
    WHERE project_id IN (
        SELECT project_id
        FROM agent_events
        GROUP BY project_id
        HAVING SUM(total_tokens) > 10000000
    )
    GROUP BY trace_id
    HAVING SUM(total_tokens) > 100000
       AND COUNT_IF(status = 'ERROR') > 0
)
ORDER BY trace_id, started_at;
```

### 1.3 Milvus 需要提供的能力

传统 Milvus 向量检索通常面向一组相互独立的 Entity：用户直接提供标量过滤条件和查询向量，Milvus 在单个 Collection 中完成过滤与 ANN TopK，查询目标就是返回匹配的 Entity。

Agent Tracing 面向持续追加的事实数据，同一个 Trace 由多条 Event 共同组成。查询条件经常不是请求中直接给出的字段值，而是由全文检索、聚合计算或内层查询动态产生的 Trace、User、Project 等 Key 集合；查询需要先筛选这些逻辑对象，再下钻事实明细或执行向量搜索。因此，向量搜索只是复杂查询链路中的一个算子，而不是完整的查询入口：

```text
传统向量检索：Scalar Filter + Query Vector -> ANN TopK Entity

Agent Tracing：Text / Aggregate / Subquery
                -> Key Set
                -> Fact Filter / Vector Search
                -> Trace Detail or TopK Event
```

为了支持这种查询形态，Milvus 需要提供以下两项能力：

1. 高效的嵌套子查询能力

   - 内层查询产生 Key 集合，作为外层查询条件。
   - 支持多层嵌套。
   - 可以组合标量过滤、全文检索、聚合过滤和向量搜索。
   - 中间结果不需要返回客户端再发起查询。

2. 高效、低成本的聚合查询能力

   - 聚合结果既可能是低基数，也可能是高基数，查询引擎不能假设聚合结果集总是很小。
   - 引入 AMV 增量维护聚合数据，避免每次查询重新扫描全部事实数据。
   - 数据分区不能再沿用当前 Milvus 的通用布局策略，需要按照指定属性组织数据并执行聚簇 Compaction。

## 2. Key Feature 与术语

### 2.1 Fact Collection

Fact Collection 具有以下基础写入语义：

1. **No Unique Primary Key**

   Fact Collection 不要求传统数据库中的唯一主键，多条事实可以具有相同的 Primary Key。

2. **Append-only**

   Fact Collection 只允许 Insert，不允许用户执行 Update、Upsert 或行级 Delete。

   Append-only 避免了历史事实修改产生的 Tombstone、Retract 和版本覆盖语义，使 WAL 消费、Segment 写入、Compaction 和聚合中间状态的增量维护都可以沿单向数据流执行，从而降低系统复杂度和写放大。

#### 2.1.1 Fact Collection 的分片

Fact Collection 复用 Milvus 的 Shard。Shard 是 WAL 顺序、写入扩展和 Shard 级一致性的边界，Shard Key 通过 Hash 决定目标 Shard，未来再决策是否需要支持 Range-based 路由。

- 典型的 Agent Tracing 场景可以使用 `trace_id` 作为 Shard Key；
- Shard Key 通常是高基数字段；
- Shard Key 等值或 `IN` 条件可以用于 Shard Pruning；
- 是否支持由多个字段组成的复合 Shard Key 仍待决策。

#### 2.1.2 Fact Collection 的分区

Fact Collection 复用 Milvus 的 Partition。Partition 是 Shard 内用于切分数据的逻辑管理单元，Partition Key 决定数据进入哪个目标 Partition。

- 典型的 Agent Tracing 场景可以使用日期作为 Partition Key；
- Partition Key 通常是日期、枚举等低基数字段；
- Partition 可以用于查询剪枝、冷热管理和历史数据清理；
- Partition 应由 Milvus 根据 Partition Definition 自动生成，避免用户手动创建和维护。

#### 2.1.3 单个 Shard 里的单个 Partition 的数据组织形式

Shard 和 Partition 只决定数据的路由位置与管理边界。一个 `(Shard, Partition)` 内的数据分为 Growing 和 Sealed 两部分；Sealed 部分再分为直接 Flush 产生的 L1 Segment，以及经过 Clustering Compaction 处理的 L2 Segment：

```text
Shard
  -> Partition
       -> Growing Data
       |    -> Growing Segments
       |
       -> Sealed Data
            -> L1 Sealed Segments: Segment 内有序
            -> L2 Compacted Segments: Partition 内多个 Segment 有序
```

**Growing Data**

- Growing Segment 由 StreamingNode 管理，按 WAL 顺序持续接收 Append-only 数据；
- 写入只需要满足 Shard 和 Partition 路由，不要求数据已经按照 Sort Key 排序；
- Growing 数据在 Flush 前即可查询，并与 Sealed 数据共同参与最终结果合并；
- Flush 对单个 Growing Segment 按 Sort Key 排序，并将其持久化为不可变的 L1 Sealed Segment。

**Sealed Data**

Sealed 数据存储在对象存储中，并由 QueryNode 加载和查询，其中包含两类 Segment：

1. L1 Sealed Segment

   L1 Segment 是 Growing Segment 的直接 Flush 产物。单个 L1 Segment 内部按照 Sort Key 有序，并保存自己的 Sort Key Range；但不同 L1 Segment 分别由不同写入批次产生，其 Sort Key Range 可以相互重叠，不保证多个 Segment 在 Partition 内具有统一顺序。

2. L2 Compacted Segment

   L2 Clustering Compaction 在同一个 `(Shard, Partition)` 范围内读取 L1 Segment 和已有 L2 Segment，按照 Sort Key 执行归并排序，再根据连续的 Sort Key Range 切分为新的 L2 Segment。

   单个 L2 Segment 内部仍然有序；同时，一个 Partition 下的多个 L2 Segment 按 Sort Key Range 有序排列，Range 通常不重叠。每个 Segment 保存对应字段的 Min/Max、时间范围、行数和数据量等统计信息，查询可以根据目标 Sort Key Range 快速定位连续的 Segment 范围，再访问标量、全文或向量索引。

Compaction 完成后，新的 L2 Segment 替代对应的旧 Sealed Segment；替换需要通过 DataView 原子发布，保证查询在同一个 Snapshot 下不会遗漏或重复读取数据。一个 Partition 可以同时存在 Growing Segment、尚未参与 Compaction 的 L1 Segment 和已经完成 Compaction 的 L2 Segment。

**Sort Key**

Sort Key 是用户定义的有序字段列表，语义类似 ClickHouse MergeTree 的 `ORDER BY`。它不负责决定 Shard 或 Partition，也不要求字段值唯一。Flush 使用它建立 L1 Segment 内部顺序，L2 Compaction 再使用它建立 Partition 范围内多个 Segment 的整体顺序。

Sort Key 的前缀应该优先选择高频等值过滤、范围过滤、Group-By 或下钻读取使用的字段。以 Agent Tracing 为例：

```text
Shard Key     = trace_id
Partition Key = event_date
Sort Key      = (trace_id, timestamp)
```

同一天、同一个 Shard 内的数据进入同一个 Partition；L2 Compaction 再按照 `trace_id` 和 `timestamp` 排序，使同一个 Trace 的 Event 按时间连续存储。

查询需要读取当前 Snapshot 下可见的 Growing、L1 和 L2 数据。Growing 数据主要依赖已有索引和扫描；L1 可以利用单个 Segment 的 Sort Key Range 做独立裁剪，但多个 L1 Range 可能相互重叠；L2 可以利用 Partition 级有序且通常不重叠的 Sort Key Range 快速定位连续的 Segment 范围：

```text
Shard Key Filter
  -> Shard Pruning
  -> Partition Key Filter
  -> Partition Pruning
  -> Sort Key Range
  -> Segment Pruning
  -> Scalar / Text / Vector Index
```

三个部分的查询结果最终统一合并：

```text
Growing Segment Result
  + L1 Sealed Segment Result
  + L2 Compacted Segment Result
  -> Merge / Reduce
  -> Final Result
```

- 同时指定 `trace_id` 和日期时，可以定位到特定 Shard 下的特定 Partition，再根据 Segment Range 继续裁剪；
- 只指定 `trace_id` 时，可以裁剪 Shard，但仍可能访问该 Shard 下的多个日期 Partition；
- 只指定日期时，需要访问每个 Shard 中对应的日期 Partition，但不需要扫描其他日期的数据。

这种布局将写入路由、数据生命周期和物理排序分成三个独立层次：Shard 负责分布式写入与一致性，Partition 负责时间或低基数维度的数据管理，Sort Key 负责 L1 Segment 内部有序以及 L2 Partition 级有序，并用于 Segment 级查询裁剪。

### 2.2 Aggregate Materialized View

Aggregate Materialized View（AMV）是由 Fact Collection 自动维护的只读 Derived Collection。

AMV 具有以下基础语义：

1. **Read-only Derived Collection**

   只有 Fact Collection 可以创建 AMV。AMV 通过消费 Source Fact Collection 的数据增量自动维护，用户不能直接向 AMV 执行任何 DML。

2. **Independent Collection Lifecycle**

   AMV 拥有独立的 Collection ID，可以独立 Load、Release、Query 和 Drop。一个 Fact Collection 可以创建多个 AMV，每个 AMV 可以使用不同的 Group-By Fields 和 Aggregate Functions。

3. **Aggregate State Storage**

   AMV Schema 只包含 Group-By Fields 和 Aggregate Fields。AMV 对用户暴露 Group Key 和最终 Aggregate Result，内部保存可持续合并的 Aggregate State，而不是反复覆盖一行最终聚合结果。

   Group Key 由一个或多个 Group-By Fields 按定义顺序编码形成。例如 `(trace_id, event_date)` 是一个复合 Group Key，只有两个字段值都相同的 Event 才属于同一个 Group。

   `trace_summary` 按 `(trace_id, event_date)` 统计 Event 数量和 Token 总量。用户将它作为普通只读 Collection 查询：

   ```python
   rows = client.query(
       collection_name="trace_summary",
       filter="total_tokens > 10000",
       output_fields=["trace_id", "event_date", "event_count", "total_tokens"],
   )
   ```

   内部可以同时保存同一个 Group Key 的多份 Partial State：

   ```text
   L2 State      (trace_123, 2026-08-04) -> (count=100, token_sum=12000)
   L1 State      (trace_123, 2026-08-04) -> (count=2,   token_sum=300)
   Growing State (trace_123, 2026-08-04) -> (count=1,   token_sum=50)
   ```

   查询时合并全部可见 State，再生成最终结果：

   ```text
   Merge State
     -> (count=103, token_sum=12350)
     -> Finalize
     -> (event_count=103, total_tokens=12350)
     -> Apply Filter: total_tokens > 10000
   ```

   Aggregate Result Filter 必须在 Merge 和 Finalize 后执行，不能直接作用于单份 Partial State。

#### 2.2.1 Aggregate Materialized View 的分片

AMV 不创建独立的 Shard Layout。它的 Shard Count、VChannel 和 Shard Ownership 必须与 Source Fact Collection 完全一致，Source Event 在哪个 Shard 写入，对应的 Aggregate Contribution 就在哪个 AMV Shard 中累计。AMV 不执行写入时的跨 Shard Shuffle，也不拥有独立的 Shard Router 或 AMV WAL。

Group-By Fields 决定 AMV 是否是 Shard-local：

1. Shard-local AMV

   当 Group-By Fields 包含 Source Shard Key 时，同一个 Group 的全部 State 只会出现在一个 AMV Shard，可以在该 Shard 内完成 Merge 和 Finalize，不需要执行 Cross-shard Merge。

2. Cross-shard AMV

   当 Group-By Fields 不包含 Source Shard Key 时，同一个 Group 可能出现在多个 Source Shard。每个 AMV Shard 只维护本 Source Shard 产生的 Partial State，查询时再跨 Shard 按 Group Key Merge 和 Finalize。

#### 2.2.2 Aggregate Materialized View 的分区

AMV 的分区是可选的，也不要求与 Fact Collection 使用相同规则。

- 用户没有指定分区规则时，每个 AMV Shard 只有一个分区；
- 用户指定分区规则时，只能使用 Group-By Fields；
- Aggregate Fields 的值会持续变化，因此不能用于分区。

Partition-local AMV 是 Shard-local AMV 的进一步优化：

```text
Shard-local AMV
  + AMV 与 Fact Collection 使用相同分区规则
  -> Partition-local AMV
```

此时同一个 Group 的 State 位于一个确定的 Source/AMV Partition 中，AMV 构建、Compaction、Merge 和 Finalize 都可以限制在该 Partition 内执行。

如果 AMV 不是 Shard-local，或者两者的分区规则不同，就不能使用 Partition-local 优化。

#### 2.2.3 单个 Shard 里的单个 Partition 的数据组织形式

Group-By Fields 编码后形成 Group Key。AMV 在单个 `(Source Shard, AMV Partition)` 内按照 Group Key 组织 Partial Aggregate State，数据同样分为 Growing 和 Sealed 两部分。未指定分区规则时，以下 AMV Partition 就是当前 Shard 中的唯一分区：

```text
AMV Shard
  -> AMV Partition
       -> Growing Aggregate State
       |
       -> Sealed Aggregate State
            -> L1 State Segments: Segment 内按 Group Key 有序
            -> L2 State Segments: Partition 内多个 Segment 有序
```

**Growing Aggregate State**

- Source Event 到达后，StreamingNode 根据 Group Key 查找或创建对应的 Aggregate State；
- 每条 Event 只产生一次 Aggregate Contribution，并增量更新 COUNT、SUM、AVG、MIN、MAX 等状态；
- Growing State 在 Flush 前即可参与查询；
- Flush 将当前状态写成不可变的 L1 State Segment，单个 Segment 内按照 Group Key 有序。

**Sealed Aggregate State**

- 不同 L1 State Segment 可能包含相同 Group Key，其 Group Key Range 也可以相互重叠；
- L2 State Compaction 在同一个 `(Shard, Partition)` 内归并相同 Group Key 的 Aggregate State，并按连续的 Group Key Range 切分新的 L2 State Segment；
- 一个 Partition 下的多个 L2 State Segment 按 Group Key Range 有序排列，Range 通常不重叠；
- Compaction 输出仍然是可继续 Merge 的 Aggregate State，而不是不可修改的最终结果。

Aggregate State 的具体内容由聚合函数决定：

```text
COUNT -> count
SUM   -> sum
AVG   -> (sum, count)
MIN   -> min
MAX   -> max
```

Aggregate State 必须满足可结合的 Merge 语义：

```text
Merge(Merge(A, B), C) == Merge(A, Merge(B, C))
```

查询需要合并当前 Snapshot 下可见的 Growing、L1 和 L2 State，再生成用户可见的聚合结果：

```text
Growing State
  + L1 State Segments
  + L2 State Segments
  -> Merge by Group Key
  -> Finalize
  -> Aggregate Result Filter
  -> Final Result
```

用户查询看到的是 `Finalize(State)`，而不是 State 的内部编码。依赖最终聚合值的 Filter 必须在同一个 Group 的全部可见 State 完成 Merge 和 Finalize 后执行，不能直接作用于单个 Partial State。

## 3. Fact Collection

### 3.1 创建语义

FC 使用独立创建 API，避免向普通 `create_collection` 继续平铺互斥选项：

```python
client.create_fact_collection(
    collection_name="events",
    schema=event_schema,
    shard_key_field="run_id",
    partition_key_field="project_id",
    num_shards=16,
    partitions_per_shard=16,
    order_by_fields=["project_id", "run_id", "event_time", "event_id"],
)
```

该 API 隐含 Append-only、系统管理的 Shard-local Partition 和 Key-aware Compaction，不再要求用户设置 `mutation_mode` 或 `routing_mode`。用户分别指定 Shard Key 和 Partition Key，但不能直接创建、删除或选择物理 Partition。

RootCoord 为 FC 保存不可变布局定义：

```text
FactCollectionDefinition {
  shard_key_field_id
  shard_hash_version
  shard_count
  partition_key_field_id
  partition_hash_version
  partitions_per_shard
  partition_owners[]
  sort_key_field_ids[]
  sort_encoding_version
}
```

### 3.2 Append-only 与重复 Primary Key

FC 只接受 Insert。以下请求在进入 WAL 前失败：

- Update；
- Partial Update；
- Upsert；
- Delete by Primary Key；
- Delete by Expression。

FC 不执行普通 Collection 的 Primary Key 最新版本覆盖语义。重复 Primary Key 的每次 Insert 都是一条独立、可见的事实，也都会产生一次 AMV Contribution。

底层使用隐藏物理行身份保证每条事实唯一：

```text
PhysicalRowID = (source_vchannel, wal_message_id, row_offset)
```

因此：

- Primary Key Filter 可以返回多行；
- Get by Primary Key 不再假设最多返回一行；
- Primary Key 不能作为 Delete 或 Upsert 的定位依据；
- Segment 内部索引、Search Hit、Requery、Pagination 和 Proxy Reduce 使用 PhysicalRowID 区分事实行，不能按用户 Primary Key 去重；
- 查询结果仍输出用户 Primary Key，因此同一请求可以返回多个相同 Primary Key、但 PhysicalRowID 不同的 Hit；
- 调用方若需要业务幂等，必须提供独立 Idempotency Token，或者自行保证不重复提交。

TTL 可以清理 FC 的事实行，但不会向 AMV 发送 Retract。AMV 表示 Lifetime Aggregate，而不是当前仍保留事实行的聚合。

### 3.3 写入路由

Proxy 对每一行分别读取 Shard Key 和 Partition Key，并使用 Collection 持久化的 Hash Version 执行两级路由：

```text
ShardKey
  -> ShardRouteToken
  -> VChannel

PartitionKey
  -> PartitionRouteToken
  -> LocalPartitionID

(VChannel, LocalPartitionID)
  -> PartitionID
```

Proxy 按 `(vchannel, partition_id)` 对 Insert 行重新分组。不能继续按 Primary Key 选择 VChannel，也不能使用跨 Shard 共享的全局 Partition。

StreamingNode 在 WAL Append 前执行权威校验：

1. Collection 是 FC；
2. 请求只包含 Insert；
3. Shard Key 和 Partition Key 存在且非 NULL；
4. 重新计算的 VChannel 与消息 VChannel 一致；
5. 根据 Partition Key 重新计算的 Local Partition 与消息 Partition 一致；
6. Partition Owner 是当前 VChannel；
7. Schema Version、Hash Version 和 Sort Definition 与 Collection 元数据一致。

校验失败的消息不能进入 Source WAL。

### 3.4 Segment 与 Compaction

FC 仍使用 Growing -> Sealed Segment 生命周期。

Growing 阶段只要求正确路由，不要求新写入已经满足全局排序。Flush 产生 L1 Segment；L2 Clustering Compaction 在以下范围内执行：

```text
(collection_id, vchannel, partition)
```

Compaction 执行：

```text
Read input segments
  -> Sort by configured Sort Key
  -> Apply TTL physical cleanup
  -> Split by ordered range
  -> Write L2 segments
```

在同一个 Partition 内，Segment 边界应尽量避免切开相同 Shard Key。单个 Shard Key 超过 Segment Hard Limit 时允许拆分为多个 Whale Segment，并记录相同的 Shard Key Range 和连续 Part Number。如果数据不满足 `ShardKey -> PartitionKey`，同一个 Shard Key 可以出现在同一 Shard 的多个 Partition 中。

每个 L2 Segment 至少记录：

- Partition Key min/max；
- Shard Key min/max；
- 完整 Sort Key min/max；
- Partition ID 和 Owner VChannel；
- Sort Encoding Version；
- 是否为 Whale Shard Key Segment；
- Row Count、Storage Size 和时间范围。

修改 Shard Key、Partition Key、Shard Count、Partitions Per Shard、Partition Ownership 或 Sort Key 都需要全量 Rewrite，不允许普通 Alter 原地完成。

### 3.5 Query Pruning

Shard Key 和 Partition Key 提供不同层级的查询剪枝：

- 只有 Shard Key 等值或 `IN` 条件时，Proxy 可以裁剪到目标 Shard，但仍需访问该 Shard 内的多个 Partition；
- 只有 Partition Key 等值或 `IN` 条件时，查询仍需访问所有 Shard，但每个 Shard 只需访问匹配的 Local Partition；
- 同时具有 Shard Key 和 Partition Key 条件时，可以直接计算目标 `(vchannel, partition_id)`；
- 不包含两种 Key 条件的请求可能访问全部 Shard 和 Partition。

QueryNode 再根据 Segment 的 Partition Key Range、Shard Key Range 和 Sort Key Range 做二次裁剪。Hash Bucket 可能包含多个 Partition Key 值，因此 Partition Pruning 后仍需执行精确标量过滤。

函数依赖 `ShardKey -> PartitionKey` 本身不能从 Shard Key 计算出 Partition Key；除非查询同时提供 Partition Key 或系统具有额外映射，否则不能据此跳过该 Shard 内的其他 Partition。

### 3.6 Import 与恢复

Bulk Import 必须使用与 Insert 相同的 Shard Key Router、Partition Key Router 和 Sort Encoding。存在 AMV 时，Import 只有两种合法实现：

1. 转换为 Source WAL Insert；
2. 同时生成 FC Segment 和所有 AMV State Segment，并在一个可恢复 Manifest 中原子发布 Source Range。

在 AMV-aware Import 可用前，对已经存在 AMV 的 FC 拒绝传统 Direct Segment Import。

恢复时必须使用 Collection 持久化的 Shard/Partition Hash Version、Partition Ownership 和 Sort Encoding Version，不能使用升级后的集群默认值重新计算旧布局。

## 4. Aggregate Materialized View

### 4.1 定义与创建

只有 FC 可以作为 AMV Source。普通 Collection、External Collection 和另一个 AMV 不能直接创建 AMV。

AMV 使用独立创建 API：

```python
client.create_aggregate_materialized_view(
    collection_name="run_summary",
    source_collection_name="events",
    group_by_fields=["run_id"],
    aggregates={
        "event_count": Aggregate.count("*"),
        "error_count": Aggregate.count_if("event_type == 'error'"),
        "total_tokens": Aggregate.sum("tokens"),
        "avg_tokens": Aggregate.avg("tokens"),
        "first_event_time": Aggregate.min("event_time"),
        "last_event_time": Aggregate.max("event_time"),
    },
)
```

`partition_by_fields` 是可选参数。未指定时，AMV 在每个 Source Shard 中使用一个 Default Partition；指定时，字段必须来自 `group_by_fields`。内部请求还可以表达基于 Group-By Fields 的确定性 Partition Expression。

Aggregate 不是新的字段数据类型。RootCoord 根据 Group-By Fields 和 Aggregate Definition 生成只读 Result Schema，并从普通 Collection ID 空间分配 AMV Collection ID。

AMV 复用普通 Collection 的：

- 名称空间和权限；
- Describe、List、Load、Release、Query 和 Drop；
- DataView、QueryView 和 Segment 生命周期。

AMV 不允许用户写入，也不拥有用户可见的 Partition 管理接口。

一个 FC 可以创建多个 AMV；每个 AMV 具有独立的 Definition Version、Aggregate State、DataView 和 Load 生命周期。

### 4.2 Aggregate-State LSM

AMV 使用 Aggregate-State LSM 存储模型，而不是不断覆盖一行最终结果：

```text
Key:
  (amv_id, definition_version, group_by_key)

Value:
  AggregateStateTuple
```

Source Insert 生成 Merge Operand：

```text
Source Event
  -> Accumulate Contribution
  -> Growing Aggregate State
  -> Flush Immutable State Segment
  -> L2 State Compaction
  -> Query-time Merge and Finalize
```

AMV Growing State 类似 LSM MemTable；AMV State Segment 类似不可变 SSTable；Compaction 合并相同 Group Key 的 State，但输出仍然是可继续 Merge 的 State。

查询顺序固定为：

```text
Scan visible State
  -> Node-local Merge
  -> Shard-level Merge
  -> Cross-shard Merge when required
  -> Finalize
  -> Apply Aggregate Result Filter
  -> Order / Limit / Render
```

依赖最终聚合值的 Filter 不能在单个 Partial State 上提前执行。

Pebble 的 Go-native Merge Operator 可以用于 StreamingNode 本地 Growing State、Spill 或原型验证，但它不是分布式 AMV 的完整实现。Milvus 仍负责 Source WAL Frontier、State Segment 发布、对象存储、DataView、QueryView 和跨节点 Merge。

### 4.3 Source-aligned AMV

所有 AMV 都继承 Source Fact Collection 的 Shard Count、VChannel 和 Shard Ownership。AMV 不创建独立 Shard，不建立写入时的跨 Shard Shuffle，也不拥有独立的 AMV WAL。

Source StreamingNode 在消费 Source WAL 时同时生成 AMV Aggregate Contribution，再根据 AMV Partition Definition 在当前 Source Shard 内选择目标 AMV Partition。未指定 Partition Definition 时，所有 State 写入当前 AMV Shard 的 Default Partition。

State Segment 使用：

```text
(derived_collection_id, source_vchannel, amv_partition_id, group_key_range)
```

每个 Contribution 携带唯一 Source Identity：

```text
(source_collection_id, source_vchannel, wal_message_id, row_offset, amv_definition_version)
```

AMV Checkpoint 使用 Source WAL Frontier 和 Source Identity 避免 Replay 重复累计 COUNT/SUM。Fact 和 AMV 共享 Source WAL MVCC，但拥有独立 DataVersion 和 QueryView。

AMV 的 Locality 决定查询合并范围：

- Group-By Fields 包含 Source Shard Key 时，AMV 是 Shard-local，可以在单个 AMV Shard 内完成 Merge 和 Finalize；
- Group-By Fields 不包含 Source Shard Key 时，AMV 是 Cross-shard，需要查询时执行跨 Source Shard Merge。

AMV Partition Definition 只能引用 Group-By Fields，因此同一个 Group 在一个 Source Shard 内会稳定进入同一个 AMV Partition。

当一个 Shard-local AMV 与 Fact Collection 使用相同的分区规则时，它进一步成为 Partition-local AMV。Source Contribution 可以直接进入对应 AMV Partition，Backfill、State Compaction、Merge 和 Finalize 都可以限制在单个 Partition 内执行。

其他情况不能使用 Partition-local 优化，StreamingNode 按照 AMV 自己的分区规则组织 State。

### 4.4 Cross-shard AMV 查询合并

当 Group-By Fields 不包含 Source Shard Key 时，每个 Source-aligned AMV Shard 只保存本 Source Shard 产生的 Partial State：

```text
Source Shard 0 -> PartialState(group_a, shard_0)
Source Shard 1 -> PartialState(group_a, shard_1)
Source Shard 2 -> PartialState(group_a, shard_2)
```

查询时按照目标 Snapshot 扫描所有 Source Shard 的可见 State，再按 Group Key 执行分层 Merge：

```text
Source-aligned AMV Shards
  -> AMV Partition State Merge
  -> Shard-local State Merge
  -> Cross-shard Merge by Group Key
  -> Finalize
  -> Aggregate Result Filter
```

任一 Source Shard 尚未处理到目标 Snapshot 时，不能把不完整的 Cross-shard AMV 结果声明为最终结果。依赖聚合值的 Filter、Order 和 Limit 必须在完整的 Cross-shard Merge 和 Finalize 后执行。

该路径避免了写入时 Shuffle、独立 AMV WAL 和第二套 Shard Layout，但把成本移动到查询侧。高基数 Group-By 可能产生较大的跨节点 Merge 状态，查询引擎不能假设 Cross-shard AMV 的结果集总是很小。

### 4.5 创建、Backfill 与恢复

创建 AMV 时，RootCoord 在 Source WAL 建立 Barrier `T0`：

```text
Backfill       = Source data <= T0
Streaming Delta = Source WAL insert > T0
```

DataNode 按 Source Shard 和 Partition 扫描 FC 的有序 Segment，并按照 AMV Partition Definition 生成初始 State Segment；StreamingNode 同时累计 `T0` 之后的 Delta。Backfill 和 Delta Catch-up 都完成后，AMV 才进入 Available。

AMV Checkpoint 必须记录：

- Definition Version；
- State Encoding Version；
- 每个 Source VChannel 的 Durable Source Frontier；
- State Segment Manifest；
- AMV Partition Definition 和 Encoding Version；
- Fact/AMV Partition Rule Match Result；
- 未完成 Flush 的 Replay 去重边界。

恢复不能依赖 Aggregate Function 自身幂等。COUNT 和 SUM 的重复 Accumulate 会直接产生错误结果。

## 5. FC 与 AMV 的高聚合 Balance

### 5.1 目标

当前 QueryView Balancer 以 Segment 为分配单元，并综合 Stickiness、Node Load 和 Shard Fanout。FC 和 AMV 需要在此基础上增加 Partition、Sort Range 和 Source/View Affinity。

Balance 的目标不是把 Segment 尽量均匀散开，而是在容量允许时减少一次查询需要访问的节点数：

```text
同 Shard
  -> Partition-local AMV 同 Partition
       -> 相邻 Sort Key / Group Key Range
            -> FC 和 AMV 对应 State Range
```

### 5.2 硬约束与软约束

硬约束：

- Segment 只能进入目标 Replica 的健康 QueryNode；
- Partition 的 Owner Shard 不可改变；
- QueryView 不能把 Segment 放入另一个 VChannel 的 Shard View；
- 节点必须支持 Segment 和 Aggregate State Encoding Version。

软约束按优先级参与评分：

- 复用当前节点，避免无意义迁移；
- 控制节点总 Row/Bytes/Memory Load；
- 减少一个 Shard 打开的 QueryNode 数；
- 减少一个 Partition 打开的 QueryNode 数；
- 把相邻 Sort Key Range 放在同一节点；
- 当 FC 和 AMV 都已 Load 时，优先共置相同 Source Shard 的数据；Partition-local AMV 进一步共置对应 Partition Range；
- 避免 Whale Shard Key 把单个节点压垮。

### 5.3 Placement Group 与切分

Balancer 首先按 Placement Group 分配，而不是逐 Segment 独立贪心：

```text
PlacementGroup {
  lineage_id
  replica_id
  source_vchannel
  partition_id
  sort_key_range
  fact_segments[]
  amv_state_segments[]
}
```

如果整个 Partition 可以放入一个节点，则优先整组放置。若 Partition 超过节点目标容量，则按不重叠 Sort Key Range 切成多个 Placement Slice。

同一个 Partition 内的相同 Shard Key 正常情况下不能跨 Slice；Whale Shard Key 是例外。Whale Slice 必须允许并行查询和最终 Merge，不能为了共置形成不可调度的超大硬约束。

### 5.4 Source 与 AMV Affinity

AMV 与 Source FC 可以独立 Load 和 Release，正确性不依赖共置。所有 AMV Shard 都与 Source 对齐，因此始终可以建立 Shard-level Placement Affinity。

Partition-local AMV 可以进一步建立 Partition-level Affinity；其他 AMV 只建立 Shard-level Affinity，不能假设 Source PartitionID 与 AMV PartitionID 一一对应。

当两者都已 Load 时，Balancer 使用相同的 `lineage_id` 和 Source Range 建立软 Affinity：

```text
events / shard-3 / partition-50 / run_id[a, m]
run_summary / shard-3 / partition-50 / run_id[a, m]
  -> prefer same QueryNode
```

共置成功时，Shard-local Nested Query 可以减少跨节点 Build Result 传输；Cross-shard AMV 也可以先在各 Source Shard 内完成 Local Merge，再执行全局 Merge。Partition-local AMV 还可以把构建、Compaction 和查询合并限制在对应 Partition。共置失败时，Proxy 仍按 QueryView 执行正确的查询，只是 Fanout 和网络开销更高。

### 5.5 Balance 可观测性

至少暴露：

- 每个 Shard 和 Partition 的 QueryNode Fanout；
- Placement Group/Slice 数量；
- FC/AMV Shard-level Affinity 命中率；
- Partition-local AMV 的 Partition-level Affinity 命中率；
- 因容量、节点状态或版本不兼容导致的 Affinity Break；
- 每次 Balance 的迁移 Bytes 和预期 Fanout 变化；
- Whale Shard Key 数量及其节点分布。

## 6. 用户交互

### 6.1 创建 FC

```python
from pymilvus import DataType, MilvusClient

client = MilvusClient(uri="http://localhost:19530")

event_schema = client.create_schema(
    auto_id=False,
    enable_dynamic_field=False,
)
event_schema.add_field(
    field_name="event_id",
    datatype=DataType.VARCHAR,
    is_primary=True,
    max_length=256,
)
event_schema.add_field(
    field_name="run_id",
    datatype=DataType.VARCHAR,
    max_length=256,
)
event_schema.add_field("project_id", DataType.VARCHAR, max_length=256)
event_schema.add_field("event_type", DataType.VARCHAR, max_length=64)
event_schema.add_field("tokens", DataType.INT64)
event_schema.add_field("event_time", DataType.INT64)
event_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=1536)

client.create_fact_collection(
    collection_name="events",
    schema=event_schema,
    shard_key_field="run_id",
    partition_key_field="project_id",
    num_shards=16,
    partitions_per_shard=16,
    order_by_fields=["project_id", "run_id", "event_time", "event_id"],
)
```

重复 `event_id` 的 Insert 会生成多条独立事实：

```python
client.insert(
    collection_name="events",
    data=[
        {"event_id": "e1", "run_id": "r1", "project_id": "p1", "event_time": 100, "tokens": 10, "embedding": v1},
        {"event_id": "e1", "run_id": "r1", "project_id": "p1", "event_time": 101, "tokens": 20, "embedding": v2},
    ],
)
```

### 6.2 创建 AMV

```python
from pymilvus import Aggregate

client.create_aggregate_materialized_view(
    collection_name="run_summary",
    source_collection_name="events",
    group_by_fields=["run_id", "project_id"],
    partition_by_fields=["project_id"],
    aggregates={
        "event_count": Aggregate.count("*"),
        "error_count": Aggregate.count_if("event_type == 'error'"),
        "total_tokens": Aggregate.sum("tokens"),
        "last_event_time": Aggregate.max("event_time"),
    },
)

client.create_aggregate_materialized_view(
    collection_name="project_summary",
    source_collection_name="events",
    group_by_fields=["project_id"],
    aggregates={
        "event_count": Aggregate.count("*"),
        "total_tokens": Aggregate.sum("tokens"),
    },
)
```

`run_summary` 的 Group-By Fields 包含 Source Shard Key `run_id`，并且使用与 Fact Collection 相同的 `project_id` 分区规则，因此它是 Partition-local AMV。`project_summary` 未指定分区规则，并且 Group-By Fields 不包含 Source Shard Key，因此它是 Cross-shard AMV，查询时需要按 `project_id` 执行跨 Shard Merge。

AMV 使用普通 Collection 查询生命周期：

```python
client.load_collection("run_summary")

rows = client.query(
    collection_name="run_summary",
    filter="total_tokens > {min_tokens} and error_count == 0",
    filter_params={"min_tokens": 100000},
    output_fields=["run_id", "project_id", "event_count", "total_tokens"],
)
```

### 6.3 多层嵌套子查询

嵌套查询使用服务端 `Subquery` Runtime Operand，不把中间 ID List 展开到客户端：

```python
from pymilvus import Subquery

large_projects = Subquery(
    collection_name="project_summary",
    filter="total_tokens > {project_min_tokens}",
    filter_params={"project_min_tokens": 10000000},
    output_field="project_id",
)

healthy_runs = Subquery(
    collection_name="run_summary",
    filter=(
        "project_id in {large_projects} "
        "and total_tokens > {run_min_tokens} "
        "and error_count == 0"
    ),
    filter_params={
        "large_projects": large_projects,
        "run_min_tokens": 100000,
    },
    output_field="run_id",
)

results = client.search(
    collection_name="events",
    data=[query_vector],
    anns_field="embedding",
    filter="run_id in {healthy_runs} and event_type == {event_type}",
    filter_params={
        "healthy_runs": healthy_runs,
        "event_type": "observation",
    },
    limit=100,
    output_fields=["event_id", "run_id", "project_id"],
)
```

Planner 把嵌套关系编译为 Query DAG，并从最深层 Build 开始执行：

```text
project_summary Build
  -> project_id Set
  -> run_summary Build
  -> run_id Set
  -> events ANN Probe
```

规则如下：

- 嵌套深度由 `max_nested_subquery_depth` 限制；
- 不支持 Correlated Subquery；
- 中间 Value Set 必须保持精确语义；
- 小结果可以 Inline，大结果写入带 Snapshot 和 TTL 的 Exact Artifact；
- Bloom Filter 只能作为预过滤，False Positive 必须通过 Exact Artifact 消除；
- Aggregate Result Filter 必须在 State Merge 和 Finalize 后执行；
- ANN 必须在完整 AMV 条件生成精确 Filter Bitset 后执行；
- 同一 Source Lineage 且 Shard Key 对齐的 Build/Probe 使用相同 Source WAL MVCC，并按 Shard 流式执行；Shard Key 和 Partition Key 都对齐时可以进一步按 Partition 执行；
- Cross-shard AMV 或不同 Lineage 的子查询需要在完整 State Merge 和 Finalize 后执行 Global Build/Exchange，不能把单个 Shard 的 Partial State 当作最终结果。

### 6.4 API 与内部请求边界

普通 Collection、FC 和 AMV 使用互斥的创建请求：

```text
CreateCollectionRequest
CreateFactCollectionRequest
CreateAggregateMaterializedViewRequest
```

三者最终都生成统一 Collection Identity，但各自的请求类型只能表达合法参数组合。RootCoord 内部使用判别联合保存定义：

```text
CollectionDefinition {
  collection_id
  name
  schema

  oneof kind {
    RegularCollectionDefinition
    FactCollectionDefinition
    AggregateMaterializedViewDefinition
  }
}
```

这样不会让 Shard Key、Partition Key、Source Collection、Aggregate Definition、External Storage 等互斥能力继续在一个平铺请求中形成组合爆炸。

### 6.5 当前实现适配点

- Proxy Insert 当前按 Primary Key 选择 VChannel、按普通 Collection 的 Partition Key 选择全局 Partition；FC 需要改为 Shard Key 决定 VChannel、Partition Key 决定 Shard 内 Local Partition 的两级 Router：
  - `internal/proxy/task_insert_streaming.go`
  - `pkg/util/typeutil/hash.go`
- 当前查询链路普遍把 Primary Key 当作 Row Identity；FC 需要把内部 PhysicalRowID 贯穿 Segment、Search Hit、Requery 和 Reduce，同时允许用户 Primary Key 重复输出。
- StreamingNode Shard Interceptor 当前按 Collection/Partition 管理 Segment Assignment；需要增加 FC 路由校验和固定 Partition Owner：
  - `internal/streamingnode/server/wal/interceptors/shard/`
- RecoveryStorage 已按 PChannel/VChannel/Segment 模块维护 WAL 状态，可扩展 AMV State Runtime 和 Frontier：
  - `internal/streamingnode/server/wal/recovery/`
  - `internal/streamingnode/server/wal/vchannel/`
- AMV State Runtime 必须绑定 Source VChannel，直接消费 Source WAL，并按照可选的 AMV Partition Definition 生成 State Segment；不能新增独立 AMV Dispatcher、跨 Shard Shuffle Runtime 或 AMV WAL。
- DataView 已按 VChannel/Partition 组织 Segment Membership，可以直接表达 Partition，但必须增加 Partition Owner 和 Sort Range 不变量：
  - `pkg/proto/view.proto`
  - `docs/design-docs/design_docs/qviews/data_view.md`
- QueryView Balancer 当前具有 Stickiness、Node Load 和 Fanout 评分，需要增加 Partition/Range/Lineage Affinity：
  - `internal/views/coord/balancer/`
  - `docs/design-docs/design_docs/qviews/balancer_design.md`
- 当前 Query 聚合和 Search Aggregation 可作为 Aggregate Function 与嵌套定义的 SDK 先例，但尚未提供持久化 AMV：
  - `tests/python_client/testcases/test_query_aggregation.py`
  - `tests/python_client/milvus_client/test_milvus_client_search_aggregation.py`

### 6.6 验收条件

1. 相同 Shard Key 的任意 Insert 始终进入同一个 VChannel；相同 Partition Key 在每个 Shard 内始终映射到相同 Local Partition。
2. 每个物理 Partition 由 `(ShardID, LocalPartitionID)` 唯一标识并只归属于一个 Shard，Partitions Per Shard 和 Owner 创建后不可修改。
3. FC 拒绝 Update、Upsert 和 Delete，但允许多条相同 Primary Key 的事实同时可见。
4. L2 Segment 按用户 Sort Key 有序，并输出可用于 Partition Key、Shard Key 和 Sort Range Pruning 的统计信息。
5. 只有 FC 可以创建 AMV，一个 FC 可以拥有多个独立 AMV。
6. AMV 使用 Mergeable Aggregate State，不通过随机覆盖最终结果行维护统计信息。
7. 所有 AMV 的 Shard Count、VChannel 和 Shard Ownership 都与 Source FC 对齐，不建立写入时跨 Shard Shuffle 或独立 AMV WAL。
8. AMV Partition Definition 是可选的；未指定时每个 AMV Shard 使用一个 Default Partition，指定时只能引用 Group-By Fields 或基于这些字段的确定性表达式。
9. Group-By Fields 包含 Source Shard Key 时为 Shard-local AMV，不包含时为 Cross-shard AMV，并在查询时执行完整 Cross-shard State Merge。
10. Shard-local AMV 与 Fact Collection 使用相同分区规则时成为 Partition-local AMV，其构建、Compaction、Merge 和 Finalize 可以限制在单个 Partition 内执行。
11. Source WAL Replay 不会让同一 Aggregate Contribution 重复累计。
12. Balance 在容量允许时减少 Shard/Partition Fanout，并优先建立 FC/AMV Shard-level Affinity；Partition-local AMV 进一步建立 Partition-level Affinity。
13. 共置失败只影响性能，不影响 QueryView 查询正确性。
14. Aggregate Filter 只在完整 Merge 和 Finalize 后执行。
15. 多层 Subquery 在服务端形成 Query DAG，中间 Key Set 保持精确语义，ANN 在完整标量过滤之后执行。
