# Fact Collection 与 Aggregate Materialized View 设计

> 状态：Proposed

## 1. 功能目标与动机

### 1.1 典型用户需求场景：Agent Tracing

本文重点面向 [Agent Tracing](https://zilliverse.feishu.cn/docx/Sb6FdTFz1oipDpxOw3ncgp38ned) 场景等具备明确的聚合需求与联合查询的场景。比如一次 Agent 运行通常是一棵由 Trace、Span、Event 和 Score 组成的调用树：

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

- Partition Key 可以直接引用 Schema Field，也可以由 Milvus 内置的确定性函数生成；
- 函数输出是 Collection 布局中的逻辑 Partition Key，不要求成为 Fact Schema Field；
- 典型的 Agent Tracing 场景可以使用 `toDate(timestamp)` 生成日期 Partition Key；
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

Compaction 完成后，新的 L2 Segment 通过 DataView 原子替代旧 Sealed Segment，保证查询不会遗漏或重复读取数据。一个 Partition 可以同时存在 Growing Segment、尚未参与 Compaction 的 L1 Segment 和已经完成 Compaction 的 L2 Segment。

**Sort Key**

Sort Key 是用户定义的有序字段列表，语义类似 ClickHouse MergeTree 的 `ORDER BY`。它不负责决定 Shard 或 Partition，也不要求字段值唯一。Flush 使用它建立 L1 Segment 内部顺序，L2 Compaction 再使用它建立 Partition 范围内多个 Segment 的整体顺序。

Sort Key 的前缀应该优先选择高频等值过滤、范围过滤、Group-By 或下钻读取使用的字段。以 Agent Tracing 为例：

```text
Shard Key     = trace_id
Partition Key = toDate(timestamp)
Sort Key      = (trace_id, timestamp)
```

同一天、同一个 Shard 内的数据进入同一个 Partition；L2 Compaction 再按照 `trace_id` 和 `timestamp` 排序，使同一个 Trace 的 Event 按时间连续存储。

查询需要读取同一读边界下的 Growing、L1 和 L2 数据。Growing 数据主要依赖已有索引和扫描；L1 可以利用单个 Segment 的 Sort Key Range 做独立裁剪，但多个 L1 Range 可能相互重叠；L2 可以利用 Partition 级有序且通常不重叠的 Sort Key Range 快速定位连续的 Segment 范围：

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

3. **Eventual Consistency**

   AMV 不与 Fact Collection 提供基于 TimeTick 的强一致性。查询读取执行时已经生成的最新可用 Aggregate State，不保证 Fact、不同 AMV 或不同 AMV Shard 位于同一个 Source TimeTick。AMV 只保证在 Source 数据停止变化且 Backfill 与增量消费追平后，最终结果与完整聚合 Fact Collection 等价。

4. **Aggregate State Storage**

   AMV Schema 由用户创建，只允许包含两类字段：

   - 普通标量字段，作为 Group-By Fields；
   - `DataType.AGGREGATE_STATE` 字段，保存 Aggregate State。

   `function`、`argument_types` 等是 `DataType.AGGREGATE_STATE` 的类型属性，共同确定 State 的兼容性和最终结果类型。`countState`、`sumState` 等只描述对应的 State 语义，不是独立的 Schema DataType。

   Describe Collection 返回 `DataType.AGGREGATE_STATE`、类型属性及其 Result Type；普通 Query 对该字段执行隐式 Merge 和 Finalize，不向用户暴露二进制 State，也不要求用户调用 `sumMerge` 一类函数。

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

此时同一个 Group 的所有原始数据位于一个确定的 Source Partition 中，AMV 的初始化 Backfill 可以按照 Partition 粒度来执行。

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

- QueryRuntime 按 WAL 顺序将 Source Event Apply 到 AMV Growing Segment；
- AMV 按 Partition 和 Group Key 聚合，并允许跨 WAL Apply Batch 持续 Merge 当前 Aggregate State；
- Query 直接读取 Growing State，Sync 只负责将 Dirty State 持久化为按 Group Key 有序的 State Chunk；
- AMV 直接使用当前 `(Shard, Partition)` 的 Growing Segment，不需要为每个 Group Key 预分配 Segment。

**Sealed Aggregate State**

- Seal 按 Group Key 合并 State Chunk，并提交为不可变的 L1 State Segment；
- 单个 L1 Segment 内相同 Group Key 被折叠，不同 L1 Segment 仍可能包含相同 Group Key；
- L2 State Compaction 在同一个 `(Shard, Partition)` 内归并相同 Group Key 的 Aggregate State，并按连续的 Group Key Range 切分新的 L2 State Segment；
- 一个 Partition 下的多个 L2 State Segment 按 Group Key Range 有序排列，Range 通常不重叠；
- Compaction 输出仍然是可继续 Merge 的 Aggregate State，而不是不可修改的最终结果。

查询需要合并当前 AMV DataView 和 Growing Segment 中最新可用的 State，再生成用户可见的聚合结果：

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

#### 2.2.4 Aggregate Function 支持范围

Aggregate State 和 Aggregate Merge 分别对齐 ClickHouse 的 `-State` 和 `-Merge` 语义：`xxxState` 表示一种具体的强类型 State，`xxxMerge` 合并相同 State Type 并返回最终结果。

可能支持的算子范围如下，具体首版范围由实现阶段确定：

| Aggregate Function | Aggregate State | Aggregate Merge |
|---|---|---|
| `COUNT(*)`：统计行数 | `countState()` | `countMerge(state)` |
| `COUNT_IF(condition)`：统计满足条件的行数 | `countIfState(condition)` | `countIfMerge(state)` |
| `SUM(value)`：计算数值总和 | `sumState(value)` | `sumMerge(state)` |
| `SUM_IF(value, condition)`：计算满足条件的数值总和 | `sumIfState(value, condition)` | `sumIfMerge(state)` |
| `AVG(value)`：计算数值平均值 | `avgState(value)` | `avgMerge(state)` |
| `AVG_IF(value, condition)`：计算满足条件的数值平均值 | `avgIfState(value, condition)` | `avgIfMerge(state)` |
| `MIN(value)`：返回最小值 | `minState(value)` | `minMerge(state)` |
| `MAX(value)`：返回最大值 | `maxState(value)` | `maxMerge(state)` |
| `FIRST_BY(value, order_key)`：返回 Order Key 最小的 Value | `firstByState(value, order_key)` | `firstByMerge(state)` |
| `LAST_BY(value, order_key)`：返回 Order Key 最大的 Value | `lastByState(value, order_key)` | `lastByMerge(state)` |
| `VECTOR_SUM(vector)`：返回逐维求和向量 | `vectorSumState(vector)` | `vectorSumMerge(state)` |
| `VECTOR_AVG(vector)`：返回逐维算术平均向量 | `vectorAvgState(vector)` | `vectorAvgMerge(state)` |
| `APPROX_COUNT_DISTINCT(value)`：返回近似去重数量 | `approxCountDistinctState(value)` | `approxCountDistinctMerge(state)` |
| `PERCENTILE(value, p)`：返回指定百分位值 | `percentileState(value, p)` | `percentileMerge(state)` |
| `QUANTILE(value, q)`：返回指定分位值 | `quantileState(value, q)` | `quantileMerge(state)` |
| `TOP_K(value, k)`：返回出现频率最高的 K 个值 | `topKState(value, k)` | `topKMerge(state)` |

`FIRST_BY` 和 `LAST_BY` 的 Order Key 必须形成确定性顺序；`VECTOR_SUM` 和 `VECTOR_AVG` 只接受维度及元素类型一致的向量。

## 3. 表的创建语义

1. **统一创建 API**

   普通 Collection、Fact Collection 和 AMV 统一使用 `create_collection`，复用 CreateCollection RPC 和 WAL DDL 链路。

2. **显式 Collection 类型**

   - 未指定或设置为 `REGULAR`：普通 Collection；
   - 设置为 `FACT`：Fact Collection；
   - 设置为 `AGGREGATE_MATERIALIZED_VIEW`：AMV。

3. **参数校验**

   SDK 和服务端不能根据参数组合推断 Collection 类型，并且必须拒绝不属于当前类型的参数。

### 3.1 创建 Fact Collection

Fact Collection 在现有 API 上增加类型、分片、分区和排序参数：

```python
from pymilvus import DataType

event_schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
event_schema.add_field("span_id", DataType.VARCHAR, is_primary=True, max_length=256)
event_schema.add_field("trace_id", DataType.VARCHAR, max_length=256)
event_schema.add_field("status", DataType.VARCHAR, max_length=64)
event_schema.add_field("tokens", DataType.INT64)
event_schema.add_field("timestamp", DataType.INT64)
event_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=1536)

client.create_collection(
    collection_name="events",
    schema=event_schema,
    collection_type="FACT",
    shard_key_field="trace_id",
    partition_by="toDate(timestamp)",
    num_shards=16,
    order_by_fields=["trace_id", "timestamp"],
)
```

1. **Schema**

   Source Schema 只保存 `timestamp`，不保存 `event_date`。

2. **Partition Function**

   - `toDate(timestamp)` 在服务端生成日期 Partition Key；
   - 生成值不作为 Fact Field 存储，也不要求客户端写入；
   - `toDate` 是 Milvus Built-in Function，由服务端解析、执行并持久化定义；
   - 不支持客户端回调或 UDF。

3. **写入语义**

   固定为 Append-only 和 No Unique Primary Key。

4. **布局约束**

   - Shard Count、Shard Key、Partition Function、Sort Key 及其编码版本创建后不可原地修改；
   - 物理 Partition 根据 Partition Function 的输出按需创建，并由系统管理。

### 3.2 创建 AMV

AMV 的 Source 必须是 Fact Collection，且 AMV 不能继续作为另一个 AMV 的 Source：

```python
from pymilvus import DataType

trace_summary_schema = client.create_schema(
    auto_id=False,
    enable_dynamic_field=False,
)
trace_summary_schema.add_field("trace_id", DataType.VARCHAR, max_length=256)
trace_summary_schema.add_field("event_date", DataType.DATE)
trace_summary_schema.add_field(
    "event_count",
    DataType.AGGREGATE_STATE,
    function="count",
)
trace_summary_schema.add_field(
    "error_count",
    DataType.AGGREGATE_STATE,
    function="countIf",
    argument_types=[DataType.BOOL],
)
trace_summary_schema.add_field(
    "total_tokens",
    DataType.AGGREGATE_STATE,
    function="sum",
    argument_types=[DataType.INT64],
)
trace_summary_schema.add_field(
    "last_event_time",
    DataType.AGGREGATE_STATE,
    function="max",
    argument_types=[DataType.INT64],
)

client.create_collection(
    collection_name="trace_summary",
    schema=trace_summary_schema,
    collection_type="AGGREGATE_MATERIALIZED_VIEW",
    source_collection_name="events",
    field_mappings={
        "trace_id": "trace_id",
        "event_date": "toDate(timestamp)",
        "event_count": "count(*)",
        "error_count": "countIf(status == 'ERROR')",
        "total_tokens": "sum(tokens)",
        "last_event_time": "max(timestamp)",
    },
    partition_by="event_date",
)
```

1. **Schema**

   - 普通标量字段自动成为 Group-By Fields；
   - `DataType.AGGREGATE_STATE` 字段成为 Aggregate Fields；
   - 用户不定义 Primary Key；内部使用 Group Key、Source Range 和 State Segment 标识 Partial State。

2. **Field Mapping**

   - `field_mappings` 必须覆盖每个 AMV Field；
   - Aggregate Mapping 的函数和输入类型必须与目标字段的类型属性一致；
   - `event_date` 可以由 `toDate(timestamp)` 生成，不要求存在于 Source Schema。

3. **Partition**

   - `partition_by` 可选，且只能引用 Group-By Field；
   - 未指定时，每个 Source Shard 只有一个 AMV Partition；
   - 与 Fact Collection 使用相同的规范化分区表达式时，可以使用 Partition-local 优化。

4. **生命周期**

   - AMV 拥有独立 Collection ID，可以独立 Load、Release、Query 和 Drop；
   - AMV 不允许用户写入或手工管理 Partition。

5. **历史数据构建**

   - 系统在每个 Source Shard 建立 Barrier `T0`；
   - `T0` 之前的数据由 DataNode Backfill，之后的数据由 StreamingNode 增量处理；
   - 两部分追平后，系统原子发布 AMV 为 Available；
   - 构建期间不阻塞 Fact Collection 写入，AMV 在 Available 前不可查询；
   - 任务切分和聚合优化详见 [5.2 Backfill](#52-backfill)。

## 4. 写路径

### 4.1 Fact 写入与路由

Proxy 按行执行两级路由：

```text
Shard Key     -> VChannel
Partition Key -> 当前 VChannel 内的 Partition
```

StreamingNode 在写入前重新校验 Collection 类型、Append-only 语义、路由结果和布局版本。校验通过后只写入 Source WAL，不创建独立 AMV WAL，也不执行跨 Shard Shuffle。

### 4.2 Growing Segment 到 L1 Sealed Segment

```text
Source WAL
  -> Apply
  -> Growing Segment
       -> Sync -> Persisted L1 Chunk
       -> Seal / Commit -> DataView
```

1. **WAL Apply**

   - Fact 将原始行追加到当前 Growing Segment；
   - AMV 按 Partition 和 Group Key 原地合并 State，并允许跨 Apply Batch 持续更新当前 State；
   - AMV 直接写入当前 `(Shard, Partition)` 的 Growing Segment，不经过 Segment 预分配。

2. **Sync**

   - Fact 将新增行写成 Data Chunk；
   - AMV 将 Dirty State 写成按 Group Key 有序的 State Chunk；
   - Chunk 持久化后推进对应的 Persisted TimeTick；
   - 每个 Source Shard 的安全 Checkpoint 为：

   ```text
   min(Fact Persisted TimeTick, AMV 1 Persisted TimeTick, ...)
   ```

3. **Seal 与 Commit**

   - Seal 冻结 Growing Segment，并完成最后一次 Sync；
   - Fact 按 Sort Key 对 Chunk 做 Segment-local Merge Sort；
   - AMV 按 Group Key 合并 State Chunk，并将相同 Group Key 折叠为一份 State；
   - Fact 和 AMV 分别提交为不可变的 L1 Sealed Segment；
   - L1 加入 DataView 后，推进对应 Collection 的 DataVersion。

### 4.3 Import

Import 在后台直接生成 Fact 和全部 AMV 的 L1 Segment，无需写入 Source WAL：

```text
Imported Rows
  -> Shard / Partition Routing
  -> Fact L1 Segments
  -> AMV L1 State Segments
  -> Import Commit
  -> Add to DataView
```

1. **后台构建**

   - 使用与普通 Insert 相同的 Shard、Partition 和 Sort Encoding；
   - Fact L1 Segment 按 Fact Sort Key 排序；
   - 每个 AMV 的 L1 State Segment 按 Group Key 排序；
   - 构建可以使用 per-shard、partition-local 和有序聚合优化。

2. **Commit 前**

   - Fact 和 AMV Segment 保持 Importing 状态；
   - Segment 不进入 DataView；
   - 构建失败或重试不执行 Import Commit。

3. **Import Commit**

   - 同一个 Import Commit 同时提交 Fact 和全部 AMV 的 L1 Segment；
   - Commit 持久化 Segment Metadata，清除 Importing 状态，并将 Segment 加入各自 Collection 的 DataView；
   - 新 Segment 作为 loadable membership 推进对应 DataView 的 `streaming_version`；
   - 不允许只发布 Fact 或部分 AMV Segment；重复 Commit 是 No-op。

## 5. Compaction 与 Backfill

### 5.1 正常 Compaction

#### 5.1.1 Fact Compaction

Fact Compaction 沿用第 2.1.3 节的数据组织方式：Flush 生成 Segment 内按 Sort Key 有序的 L1 Segment；L2 Compaction 在单个 `(Shard, Partition)` 内归并排序，并按连续 Sort Key Range 输出 L2 Segment。

Segment 记录 Shard Key、Partition Key、Sort Key 和时间范围等统计信息。相同 Shard Key 应尽量保持连续；单个 Key 超过 Segment 大小限制时允许拆分。

Compaction 只是已有事实的物理重写，不能再次产生 AMV Contribution。TTL 只清理 Fact 数据，不向 AMV 发送 Retract，因此 AMV 表示 Lifetime Aggregate。

#### 5.1.2 AMV State Compaction

AMV L1 Segment 内按 Group Key 聚合和排序，不同 L1 Segment 仍可能包含相同 Group Key。L2 State Compaction 在单个 `(Shard, AMV Partition)` 内合并相同 Group Key 的 Aggregate State，并按 Group Key Range 输出新的 State Segment。

Aggregate Function 及其 State 定义见 [2.2.4 Aggregate Function 支持范围](#224-aggregate-function-支持范围)。Compaction 输出仍是可继续合并的 State，不是最终聚合值。输出 State 的内部 TimeTick 取所有输入 State TimeTick 的最大值。Cross-shard AMV 只在各 Source Shard 内压缩 Partial State，跨 Shard 合并留在查询阶段。

### 5.2 Backfill

1. **任务边界**

   - 以 Source Shard 为基本执行边界；
   - Partition-local AMV 可以进一步按 `(Source Shard, Source Partition)` 构建。

2. **有序聚合**

   当 AMV Group Key 相对于 Fact Sort Key 单调非递减时，可以顺序扫描并直接生成 Aggregate State：

   ```text
   Ordered Source Scan
     -> Accumulate Current Group
     -> Group Key Changes
     -> Emit Aggregate State
   ```

   例如，在 `toDate(timestamp)` Partition 内：

   ```text
   Fact Sort Key = (trace_id, timestamp)
   AMV Group Key = (trace_id, event_date)
   event_date     = toDate(timestamp)
   ```

   `event_date` 在 Partition 内为常量，因此相同 Group Key 连续。Fact L2 可以直接顺序扫描；Range 重叠的 Fact L1 需要先 K-way Merge，或者分别生成 Partial State。无法证明保序时，回退到 Hash/Spill Aggregation。

3. **构建产物**

   - Backfill 生成 AMV L1 State Segment；
   - 产物由 AMV Definition Version 和 Source Range 标识；
   - 失败重试不能重复累计同一批 Source 数据。

## 6. AMV 最终一致性与 TimeTick

### 6.1 最终一致性

- AMV 不与 Fact Collection 保证相同的 Source TimeTick；
- AMV Query 不等待或使用 QueryPlanMVCC，也不按 TimeTick 过滤 Aggregate State；
- AMV 不保存历史 Aggregate State，不支持历史快照查询；
- 每个 AMV Shard 读取本地最新可用 State，Cross-shard Query 不要求所有 Shard 位于同一 Source TimeTick；
- 当 Source 数据停止变化且 Backfill 与增量消费追平后，AMV 最终结果必须与完整聚合 Fact Collection 等价。

### 6.2 TimeTick

AMV 保留 Milvus 的内部 TimeTick 列。它表示 Aggregate State 已经包含的最大 Source TimeTick，只用于恢复、进度跟踪和问题排查，不参与查询可见性判断。

- Source Row 生成 State 时，TimeTick 取输入行的最大值；
- Growing State 原地 Merge 时，TimeTick 取当前 State 与新增 State 的最大值；
- Sync 和 Seal 输出 State 时，TimeTick 取所有输入 State 的最大值；
- Compaction 输出 State 时，TimeTick 取所有输入 State 的最大值。

```text
Output State TimeTick = MAX(Input State TimeTick)
```

### 6.3 DataView

AMV 仍使用 DataView 原子管理 Segment Membership。Query 固定当前 AMV DataView，并读取执行时最新可用的 Growing State；Flush、Import 和 Compaction 的 DataView 切换保证 Segment 不会重复或遗漏，但不提供 Fact 与 AMV 的 TimeTick 一致性。

## 7. 读路径

### 7.1 AMV 查询

用户将 AMV 作为普通只读 Collection 查询。AMV Query 不使用 QueryPlanMVCC，也不依赖 Sync 或 DataCheckpoint，直接读取各 Shard 执行时最新可用的 Aggregate State。执行顺序固定为：

```text
Growing + L1 + L2 State
  -> Merge by Group Key
  -> Cross-shard Merge when required
  -> Finalize
  -> Aggregate Result Filter
  -> Order / Limit / Result
```

Shard-local AMV 在单个 Source Shard 内完成 Merge；Cross-shard AMV 必须先汇总所有 Source Shard 的 Partial State，但不同 Shard 的 State 不保证对应同一个 Source TimeTick。依赖聚合结果的 Filter、Order 和 Limit 只能在完整 Merge 和 Finalize 后执行。

### 7.2 嵌套子查询

嵌套查询由服务端编译为 Query DAG，中间 Key Set 不返回客户端。例如先筛选 Trace，再下钻 Event 做向量搜索：

```python
selected_traces = Subquery(
    collection_name="trace_summary",
    filter="total_tokens > {min_tokens} and error_count == 0",
    filter_params={"min_tokens": 100000},
    output_field="trace_id",
)

results = client.search(
    collection_name="events",
    data=[query_vector],
    anns_field="embedding",
    filter="trace_id in {selected_traces}",
    filter_params={"selected_traces": selected_traces},
    limit=100,
)
```

```text
trace_summary Build
  -> Exact trace_id Set
  -> events Filter
  -> ANN Search
```

系统支持多层非关联子查询。AMV 子查询读取最新可用聚合状态，Fact 子查询或外层查询使用自身的 MVCC，两者不保证处于同一个 Source TimeTick。小型 Key Set 可以随请求传递；高基数 Key Set 由服务端分区或 Spill，避免要求单个 Proxy 常驻全部结果。Bloom Filter 只能用于预过滤，最终结果必须由精确 Key Set 校验。ANN 必须在完整聚合条件生成精确过滤结果后执行。
