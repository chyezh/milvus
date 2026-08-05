# Fact Collection 与 Aggregate Materialized View 设计

> 状态：Proposed

本文定义面向持续追加事实数据的 Fact Collection，以及由 Fact Collection 增量维护的 Aggregate Materialized View（AMV）。设计重点是支持 Agent Tracing 等同时需要聚合过滤、明细下钻、全文检索和向量搜索的场景。

## 1. 功能目标与典型场景

### 1.1 Agent Tracing

一次 Agent 运行通常由 Trace、Span、Event 和 Score 组成：

```text
Trace
  -> Agent Span
       -> Generation Span
       -> Tool / Retrieval Span
       -> Event / Score
```

数据包含 Trace/Span 标识、父子关系、时间、模型、Token、成本、Metadata、大文本、Embedding 和质量评分。分布式 SDK 持续追加这些事实，同一个 Trace 的数据可能迟到、乱序、重复，也可能没有明确的结束事件。

### 1.2 典型查询

以下查询使用类 SQL 表达业务语义，不表示 Milvus 需要提供 SQL 接口。

按 `trace_id` 读取全部记录，并还原 Trace 调用树：

```sql
SELECT *
FROM agent_events
WHERE trace_id = 'trace_123'
ORDER BY timestamp, span_id;
```

全文检索命中的内容，并下钻对应 Trace 的全部记录：

```sql
SELECT *
FROM agent_events
WHERE trace_id IN (
    SELECT trace_id
    FROM agent_events
    WHERE TEXT_MATCH(content, 'rate limit exceeded')
       OR TEXT_MATCH(error_message, 'rate limit exceeded')
    GROUP BY trace_id
    ORDER BY MAX(timestamp) DESC
    LIMIT 100
)
ORDER BY trace_id, timestamp, span_id;
```

按时间和模型统计调用量、Token、成本、错误率和 P95 延迟：

```sql
SELECT
    TIME_BUCKET('1 hour', timestamp) AS hour,
    model,
    COUNT(*) AS call_count,
    SUM(tokens) AS total_tokens,
    SUM(cost) AS total_cost,
    COUNT_IF(status = 'ERROR') AS error_count,
    PERCENTILE(latency_ms, 0.95) AS p95_latency
FROM agent_events
WHERE timestamp >= '2026-08-01'
GROUP BY hour, model;
```

筛选高成本且没有错误的 Trace，再在对应 Span 中执行向量搜索：

```sql
SELECT trace_id, span_id, content
FROM agent_events
WHERE trace_id IN (
    SELECT trace_id
    FROM agent_events
    GROUP BY trace_id
    HAVING SUM(tokens) > 100000
       AND COUNT_IF(status = 'ERROR') = 0
)
ORDER BY VECTOR_DISTANCE(embedding, :query_vector)
LIMIT 100;
```

### 1.3 Milvus 需要提供的能力

传统 Milvus 查询通常直接在一组相互独立的 Entity 上执行标量过滤和 ANN Search。Agent Tracing 的查询条件则经常来自多个 Event 的聚合结果或内层查询：

```text
传统向量检索：Scalar Filter + Query Vector
                -> ANN TopK Entity

Agent Tracing：Text / Aggregate / Subquery
                -> Key Set
                -> Fact Filter / Vector Search
                -> Trace Detail or TopK Event
```

Milvus 需要提供以下能力：

1. **高效的嵌套子查询能力**

   - 内层查询产生 Key 集合，作为外层查询条件；
   - 支持多层嵌套；
   - 可以组合标量过滤、全文检索、聚合过滤和向量搜索；
   - 中间结果不需要返回客户端再发起查询。

2. **高效、低成本的聚合查询能力**

   - 聚合结果既可能是低基数，也可能是高基数；
   - 查询引擎不能假设聚合结果集总是很小；
   - AMV 增量维护聚合数据，避免每次查询重新扫描全部事实数据。

## 2. 设计概览与核心模型

### 2.1 总体设计

**Fact Collection** 是保存原始事实数据的 Collection。每次 Insert 都追加一条独立事实，例如一个 Span 或 Event；这些数据是后续明细查询、全文检索、向量搜索和聚合计算的 Ground Truth。

**Aggregate Materialized View（AMV）** 是由 Fact Collection 派生的只读 Collection。它按照用户定义的 Group-By Fields 增量维护 Aggregate State，用于直接查询聚合结果，避免每次查询重新扫描全部事实数据。

一个 Fact Collection 可以创建多个 AMV。Fact 与 AMV 消费同一份 Source WAL，共享 Shard Layout，但仍作为独立 Collection 管理。

```text
                         Source WAL
                             |
                  +----------+----------+
                  |                     |
           Fact Collection              AMV
        Growing -> L1 -> L2    Growing State -> L1 -> L2
                  |                     |
             Fact Query              AMV Query
                  \                     /
                   +--- Nested Query --+
```

Fact Query 返回原始事实。AMV Query 返回聚合结果。嵌套查询可以先从 Fact 或 AMV 产生 Key Set，再用该 Key Set 下钻 Fact 明细或执行向量搜索。

### 2.2 核心设计决策

1. **Fact Collection 是 Append-only**

   用户只能 Insert，不能执行 Update、Upsert 或行级 Delete，也不能为 Fact Collection 启用 TTL。事实数据一旦写入就不会产生 Retract，AMV 因此只需要沿单向数据流持续累积 State。

2. **Fact Collection 不要求 Primary Key 唯一**

   Schema 仍保留 Milvus Primary Field，但多条事实允许使用相同 Primary Key，系统不执行覆盖或去重。

3. **Fact 与 AMV 共享 Shard Layout**

   Fact Collection 必须指定 Shard By，相同 Shard By 结果的数据进入同一个 Shard。AMV 强制跟随 Source Fact Collection 的 Shard Layout，用户不能为 AMV 单独配置 Shard。

4. **提供灵活的 Partition By**

   Fact Collection 和 AMV 都允许用户定义自己的 Partition By，用于组织数据，以及管理数据和资源。AMV 的 Partition By 不要求与 Fact Collection 相同。

5. **AMV 存储 Aggregate State，并提供最终一致性**

   AMV 是存储可合并 Aggregate State 的只读 Collection。查询返回最新可用的聚合结果，不保证与 Fact Collection 强一致。

### 2.3 Fact Collection

Fact Collection 是 AMV 唯一允许的 Source，其数据布局由以下概念定义：

| 术语 | 含义 | 设计意图 |
|---|---|---|
| Shard | WAL 顺序、写入扩展和 Shard 级一致性的边界 | 将同一逻辑对象的数据固定到同一个分布式写入边界 |
| Shard By | 计算稳定 Hash 路由值的表达式 | 支持 Shard Pruning，并避免同一逻辑对象的写入跨 Shard Shuffle |
| Partition | Shard 内的数据管理单元 | 支持时间或低基数维度的裁剪、冷热管理和清理 |
| Partition By | 使用 Fact Field 或内置确定性表达式定义目标 Partition | 支持派生分区规则，避免客户端预先生成和维护分区字段 |
| Order By | 用户定义的有序表达式列表 | 建立 L1 Segment 内有序和 L2 Partition 级聚簇布局 |

#### 2.3.1 创建 Fact Collection

普通 Collection、Fact Collection 和 AMV 统一复用 `create_collection`。用户通过 `collection_type` 显式指定 Collection 类型，SDK 和服务端拒绝与该类型不兼容的参数。

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
    shard_by="hash(trace_id)",
    partition_by="toDate(timestamp)",
    num_shards=16,
    order_by=["trace_id", "timestamp"],
)
```

- Source Schema 只保存 `timestamp`，不保存 `event_date`；
- Shard By、Partition By 和 Order By 统一按表达式定义并持久化；
- Shard By 首版支持 `hash(field)`，Partition By 首版支持 Milvus 内置确定性函数；
- Order By 首版可以只支持字段引用，但接口与元数据预留函数表达式能力；
- 物理 Partition 根据 Partition By 的结果按需创建，由系统管理；
- Shard Count、Shard By、Partition By、Order By 及其编码版本创建后不可原地修改。

#### 2.3.2 Fact 数据组织

一个 `(Shard, Partition)` 内的数据分为 Growing、L1 和 L2：

```text
Shard
  -> Partition
       -> Growing Segments
       -> L1 Segments: Segment 内按 Order By 有序
       -> L2 Segments: Partition 内按 Order By Range 有序
```

- Growing Segment 按 WAL 顺序接收数据，不要求写入时有序；
- L1 Segment 由 Growing Segment 直接 Flush 产生，单个 Segment 内按 Order By 有序，不同 L1 的 Range 可以重叠；
- L2 Segment 由 Clustering Compaction 产生，同一 Partition 内的多个 L2 Range 通常不重叠。

### 2.4 Aggregate Materialized View

用户不能直接写入 AMV 或手工管理其 Partition。AMV Schema 只包含两类字段：

1. **Group-By Field**

   普通标量字段。一个 AMV 可以定义一个或多个 Group-By Fields，它们按 Schema 顺序编码形成复合 Group Key。

2. **Aggregate Field**

   `DataType.AGGREGATE_STATE` 字段。`function` 和 `argument_types` 等类型属性共同确定 State 的强类型、兼容性和最终结果类型。

例如，`trace_summary` 按 `(trace_id, event_date)` 聚合：

```text
Group-By Fields = (trace_id, event_date)
Aggregate Fields = (event_count, error_count, total_tokens, last_event_time)
```

Group-By Fields 与 Source Shard By 的关系决定聚合范围：

- Group-By Fields 能唯一确定 Source Shard By 结果时，同一个 Group 只存在于一个 Shard，是 Shard-local AMV；
- Group-By Fields 不能唯一确定 Source Shard By 结果时，同一个 Group 可能存在于多个 Shard，是 Cross-shard AMV。

AMV 的 Shard 强制跟随 Source Fact Collection，但 Partition By 可以独立定义：

- 未指定分区规则时，每个 AMV Shard 只有一个 Partition；
- 指定分区规则时，表达式只能引用 Group-By Fields；
- Aggregate Field 会持续变化，不能参与分区；
- AMV 与 Fact 使用相同的规范化 Partition By 时，可以按相同的 `(Shard, Partition)` 边界组织 Backfill、Compaction 和加载，形成 Partition-local AMV。

#### 2.4.1 创建 AMV

用户先定义 AMV Schema，再通过 `create_collection` 声明 Source 和 Field Mapping：

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
        "event_count": "countState()",
        "error_count": "countIfState(status == 'ERROR')",
        "total_tokens": "sumState(tokens)",
        "last_event_time": "maxState(timestamp)",
    },
    partition_by="event_date",
)
```

- `field_mappings` 必须覆盖所有 AMV Fields；
- Aggregate Mapping 必须使用 `xxxState` 函数，并与目标 State Field 的 Function 和输入类型一致；
- 用户不为 AMV 定义 Primary Key；
- 创建 AMV 时，以 Source Shard 为边界处理历史数据和增量数据；
- 两部分追平后，AMV 原子切换为 Available；详细过程见第 5.3 节。

#### 2.4.2 AMV 数据组织

AMV 在单个 `(Source Shard, AMV Partition)` 内按 Group Key 组织 Aggregate State：

```text
AMV Shard
  -> AMV Partition
       -> Growing State
       -> L1 State Segments: Segment 内按 Group Key 聚合和排序
       -> L2 State Segments: Partition 内按 Group Key Range 有序
```

- Growing State 持续合并新增事实产生的 Aggregate State；
- 单个 L1 内相同 Group Key 被折叠，不同 L1 仍可以包含相同 Group Key；
- L2 在 Partition 内进一步合并相同 Group Key，并按 Group Key Range 组织 Segment；
- Growing、L1 和 L2 中可以同时存在同一个 Group Key 的 Partial State。

```text
L2 State      (trace_123, 2026-08-04) -> countState(100), sumState(12000)
L1 State      (trace_123, 2026-08-04) -> countState(2),   sumState(300)
Growing State (trace_123, 2026-08-04) -> countState(1),   sumState(50)
```

这些 Partial State 必须通过 `xxxMerge` 函数查询，详见第 3.1 和 7.2 节。

### 2.5 Aggregate State 模型与算子

Aggregate State 和 Aggregate Merge 分别对齐 ClickHouse 的 `-State` 和 `-Merge` 语义：`xxxState` 表示某种 Aggregate Function 的中间状态，`xxxMerge` 合并相同类型的 State 并返回最终结果。

`countState`、`sumState` 等不是独立的 Schema DataType。Schema 统一使用 `DataType.AGGREGATE_STATE`，并通过类型属性声明具体 State。

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

`FIRST_BY` 和 `LAST_BY` 的 Order Key 必须形成确定性顺序。`VECTOR_SUM` 和 `VECTOR_AVG` 只接受维度及元素类型一致的向量。

## 3. 查询交互

### 3.1 查询 AMV

Group-By Field 可以作为普通标量字段使用。Aggregate State Field 不能被直接输出、过滤或排序，用户必须调用与 State 类型匹配的 `xxxMerge` 函数获得最终结果：

```python
rows = client.query(
    collection_name="trace_summary",
    filter="sumMerge(total_tokens) > 10000",
    output_fields=[
        "trace_id",
        "event_date",
        "countMerge(event_count) AS event_count",
        "sumMerge(total_tokens) AS total_tokens",
        "maxMerge(last_event_time) AS last_event_time",
    ],
)
```

- `countState` 字段只能使用 `countMerge`，`sumState` 字段只能使用 `sumMerge`；
- 不匹配的 Merge Function 必须在 Query Plan 校验阶段被拒绝；
- `xxxMerge` 按 AMV Group Key 合并 Growing、L1 和 L2 中的全部 Partial State；
- Filter、Order 和 Limit 只能作用于 Merge 后的结果；
- 普通 Query 不允许返回 Aggregate State 的内部编码。

### 3.2 嵌套子查询

嵌套子查询是通用查询能力，不限定 Collection 类型。Regular Collection、Fact Collection 和 AMV 都可以作为内层或外层查询；AMV Stage 额外要求使用 `xxxMerge`。中间 Key Set 由服务端传递，不返回客户端。

这个语法本质上只是支持了 Semi-Join：内层查询必须能独立执行（无法关联其他表），通过 `output_field` 生成 Key Set，外层查询使用该 Key Set 过滤数据。内层的其他输出字段无法带入最终结果。

例如先从 AMV 筛选 Trace，再在 Fact Collection 中执行向量搜索：

```python
selected_traces = Subquery(
    collection_name="trace_summary",
    filter="sumMerge(total_tokens) > {min_tokens} and countIfMerge(error_count) == 0",
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

三层嵌套查询可以先通过全文检索定位 Trace，再通过 AMV 筛选高成本 Trace，最后执行向量搜索：

```python
text_matched_traces = Subquery(
    collection_name="events",
    filter="TEXT_MATCH(content, {keyword})",
    filter_params={"keyword": "rate limit exceeded"},
    output_field="trace_id",
)

high_cost_traces = Subquery(
    collection_name="trace_summary",
    filter=(
        "trace_id in {text_matched_traces} "
        "and sumMerge(total_tokens) > {min_tokens}"
    ),
    filter_params={
        "text_matched_traces": text_matched_traces,
        "min_tokens": 100000,
    },
    output_field="trace_id",
)

results = client.search(
    collection_name="events",
    data=[query_vector],
    anns_field="embedding",
    filter="trace_id in {high_cost_traces}",
    filter_params={"high_cost_traces": high_cost_traces},
    limit=100,
)
```

```text
Event Full-Text Search
  -> trace_id Key Set
  -> AMV Aggregate Filter
  -> trace_id Key Set
  -> Event ANN Search
```

多个独立子查询可以通过 UNION 合并 Key Set：

```python
high_token_traces = Subquery(
    collection_name="trace_summary",
    filter="sumMerge(total_tokens) > 100000",
    output_field="trace_id",
)

error_traces = Subquery(
    collection_name="trace_summary",
    filter="countIfMerge(error_count) > 0",
    output_field="trace_id",
)

selected_traces = Subquery.union(high_token_traces, error_traces)
```

集合运算的输入必须输出兼容类型的 Key，各输入子查询可以并行执行：

- `UNION`：返回任意输入中出现的 Key，并精确去重；
- `INTERSECT`：返回所有输入中都出现的 Key；
- `EXCEPT`：返回第一个输入中存在、但后续输入中不存在的 Key。

上例使用 UNION。集合运算只处理 Key，不携带内层查询的其他输出字段，也不支持依赖外层当前行的关联子查询。

## 4. 写路径

### 4.1 Fact 写入与路由

Proxy 按行执行两级路由：

```text
Shard By     -> Hash Routing Value -> VChannel
Partition By -> 当前 VChannel 内的 Partition
```

StreamingNode 在写入前校验 Collection Type、Append-only 语义、路由结果和布局版本。校验通过后只写入 Source WAL，不创建独立 AMV WAL，也不执行跨 Shard Shuffle。

### 4.2 Source WAL Apply

Source WAL 中的每条事实由同一个 Shard 内的 Fact Collection 和全部 AMV 消费：

```text
Source WAL
  -> Fact Growing Segment
  -> AMV 1 Growing State
  -> AMV 2 Growing State
  -> ...
```

- Fact 将原始行追加到目标 Growing Segment；
- AMV 按自身 Partition By 和 Group Key 原地合并 Aggregate State；
- AMV 可以跨 Apply Batch 持续更新当前 Growing State；
- 所有 AMV Contribution 都保留在 Source Shard 内。

### 4.3 Growing Segment 到 L1 Segment

```text
Growing Segment
  -> Sync
  -> Persisted Chunk
  -> Seal
  -> L1 Segment
  -> DataView Commit
```

1. **Sync**

   - Fact 将新增行持久化为 Data Chunk；
   - AMV 将 Dirty State 持久化为按 Group Key 有序的 State Chunk；
   - Chunk 持久化后推进对应的 Persisted TimeTick。

2. **Seal**

   - Seal 冻结 Growing Segment，并完成最后一次 Sync；
   - Fact 按 Order By 对 Chunk 执行 Segment-local Merge Sort；
   - AMV 合并 State Chunk，并将相同 Group Key 折叠为一份 State。

3. **Commit**

   - Fact 和 AMV 分别生成不可变的 L1 Segment；
   - L1 Segment 加入对应 Collection 的 DataView；
   - DataView Commit 后推进对应 Collection 的 DataVersion。

### 4.4 Import

Import 在后台直接生成 Fact 和全部现有 AMV 的 L1 Segment，无需写入 Source WAL：

```text
Imported Rows
  -> Shard / Partition Routing
  -> Fact L1 Segments
  -> AMV L1 State Segments
  -> Import Commit
  -> Add to DataView
```

- Fact L1 Segment 按 Fact Order By 排序；
- AMV L1 State Segment 按 Group Key 聚合和排序；
- Commit 前的 Segment 保持 Importing 状态，不进入 DataView；
- 同一个 Import Commit 同时提交 Fact 和全部 AMV Segment；
- 不允许只发布 Fact 或部分 AMV Segment；
- 重复执行同一个 Import Commit 是 No-op。

## 5. Compaction 与 Backfill

### 5.1 Fact Compaction

L2 Clustering Compaction 在同一个 `(Shard, Partition)` 内读取 L1 和已有 L2，按照 Order By 归并排序，再按连续 Order By Range 切分新的 L2 Segment。

Segment 保存 Order By Range、时间范围、行数和数据量等统计信息。Compaction 只重写物理布局，不能删除事实数据，也不能再次产生 AMV Contribution。

### 5.2 AMV State Compaction

L2 State Compaction 在单个 `(Source Shard, AMV Partition)` 内读取 L1 和已有 L2，合并相同 Group Key 的 Aggregate State，并按连续 Group Key Range 输出新的 L2 State Segment。

Compaction 输出仍是可继续 Merge 的 State，不是最终聚合值。Cross-shard AMV 只在各 Source Shard 内压缩 Partial State，不在 Compaction 中执行跨 Shard Merge。

### 5.3 Backfill

创建 AMV 时，系统在每个 Source Shard 建立 Barrier `T0`：

- `T0` 之前的数据由 DataNode Backfill；
- `T0` 之后的数据由 StreamingNode 增量处理；
- 两部分追平后，AMV 原子切换为 Available；
- 构建期间不阻塞 Fact Collection 写入，AMV 在 Available 前不可查询。

Backfill 以 Source Shard 为基本任务边界。Partition-local AMV 可以进一步按 `(Source Shard, Source Partition)` 切分任务。

当 AMV Group Key 相对于 Fact Order By 可以证明单调非递减时，可以顺序聚合：

```text
Ordered Source Scan
  -> Accumulate Current Group
  -> Group Key Changes
  -> Emit Aggregate State
```

例如：

```text
Fact Partition By = toDate(timestamp)
Fact Order By      = (trace_id, timestamp)
AMV Group Key      = (trace_id, event_date)
event_date         = toDate(timestamp)
```

在单个日期 Partition 内，`event_date` 是常量，相同 Group Key 连续。Fact L2 可以直接顺序扫描；Range 重叠的 L1 需要先 K-way Merge，或者分别产生 Partial State。无法证明保序时，回退到 Hash/Spill Aggregation。

Backfill 产物由 AMV Definition Version 和 Source Range 标识，失败重试不能重复累计同一批 Source 数据。

## 6. 一致性模型

### 6.1 Fact MVCC

Fact Collection 沿用 Milvus 的正常 MVCC 语义。Fact Query 按 QueryPlan 的 TimeTick 决定数据可见性，并在同一个读边界下合并 Growing 和 Sealed 数据。

### 6.2 AMV 最终一致性

AMV 不与 Fact Collection 提供基于 Source TimeTick 的强一致性：

- AMV Query 不等待或使用 QueryPlanMVCC；
- AMV 不按 TimeTick 过滤 Aggregate State；
- AMV 不保存历史 Aggregate State，也不支持历史快照查询；
- 不同 AMV、不同 AMV Shard 可以位于不同 Source TimeTick；
- Source 数据停止变化且 Backfill 与增量消费追平后，AMV 最终结果必须与完整聚合 Fact Collection 等价。

嵌套查询中，AMV 子查询读取最新可用 State，Fact 子查询或外层查询使用自身 MVCC，两者不保证对应同一个 Source TimeTick。

### 6.3 AMV TimeTick

AMV 保留 Milvus 的内部 TimeTick 列。它表示当前 Aggregate State 已包含的最大 Source TimeTick，只用于恢复、进度跟踪和问题排查，不参与查询可见性判断。

所有 State Merge 使用相同规则：

```text
Output State TimeTick = MAX(Input State TimeTick)
```

该规则适用于 Growing State Merge、Sync、Seal、Compaction 和 Backfill 输出。

### 6.4 DataView

Fact 和 AMV 分别使用自己的 DataView 原子管理 Segment Membership。

- Flush、Import 和 Compaction 通过 DataView Commit 替换 Segment；
- 同一个 DataView 中的 Segment 不会重复或遗漏；
- Query 固定执行时的 DataView；
- DataView 不提供 Fact 与 AMV 的 Source TimeTick 一致性。

## 7. 读路径

### 7.1 Fact 数据裁剪

Fact Query 根据已知条件逐级缩小读取范围：

```text
Shard By Filter
  -> Shard Pruning
  -> Partition By Filter
  -> Partition Pruning
  -> Order By Range
  -> Segment Pruning
  -> Scalar / Text / Vector Index
```

- 同时指定 `trace_id` 和日期时，可以定位到特定 Shard 和 Partition；
- 只指定 `trace_id` 时，可以裁剪 Shard，但可能访问多个日期 Partition；
- 只指定日期时，需要访问每个 Shard 中对应的日期 Partition；
- L1 只能使用各 Segment 独立的 Order By Range；
- L2 可以利用 Partition 内通常不重叠的 Range 定位连续 Segment。

### 7.2 AMV State Merge

Query Plan 中的 `xxxMerge` 表达式驱动 AMV State Merge：

```text
xxxMerge(Aggregate State Field)
  -> Read Growing + L1 + L2 State
  -> Shard-local State Merge by Group Key
  -> Cross-shard State Merge when required
  -> Final Aggregate Result
  -> Aggregate Result Filter
  -> Order / Limit / Result
```

- Shard-local AMV 在单个 Source Shard 内完成 Merge；
- Cross-shard AMV 在查询阶段汇总各 Source Shard 的 Partial State；
- 每个 `xxxMerge` 必须与 Aggregate State Field 的 Function 和输入类型匹配；
- Filter、Order 和 Limit 只能作用于完整 Merge 后的最终结果。

### 7.3 嵌套子查询执行

Proxy 将嵌套子查询编译为 Query DAG，每个 Stage 复用现有 QueryView 两阶段查询：

```text
Proxy Query DAG
  -> Execute Inner Stages in Parallel
       -> QueryView Plan and Execution
       -> Exact Key Sets
  -> Partition by Downstream Shard By
  -> Set Operation
  -> Build / Spill KeySetHandle
  -> Execute Outer Stage with KeySet Predicate
  -> Final Reduce
```

1. Proxy 校验内层输出字段与外层过滤字段的类型，并生成 Stage 依赖关系；
2. 每个 Stage 独立选择 Collection、QueryPlan 和执行算子；
3. 内层 Stage 生成精确 Key Set，AMV Stage 在生成 Key 前完成 `xxxMerge` 和聚合过滤；
4. 多个输入 Key Set 按下游 Collection 的 Shard By 分区，路由一致时保持 Shard-local，否则重新分发；
5. 在每个下游 Shard 内执行 UNION、INTERSECT 或 EXCEPT；
6. 小型结果随请求传递，高基数结果通过请求级 `KeySetHandle` 分区或 Spill；
7. 外层 Stage 将 `KeySetHandle` 转换为精确 Membership Filter，再执行 Scalar、Text、Aggregate 或 ANN；
8. 多层子查询按 DAG 拓扑顺序执行，失败时只重试受影响的 Stage 和 Shard。

Bloom Filter 只能用于预过滤，最终结果必须由精确 Key Set 校验。ANN 必须等待对应 Shard 的完整 Key Set 生成后再执行。

## 8. 加载路径与 Balance

### 8.1 Load 语义

Fact Collection 和 AMV 拥有独立 Collection ID，可以独立 Load 和 Release。

- Fact Query 只要求目标 Fact Collection 已加载；
- AMV Query 只要求目标 AMV 已加载；
- 嵌套查询要求每个 Stage 使用的 Collection 都已加载；
- AMV Shard Count 和 VChannel Layout 始终跟随 Source Fact Collection。

### 8.2 Locality 与 Balance

Balance 仍以 Collection、Replica 和 Shard 为调度边界，但应尽量保持以下 Locality：

1. 同一个 Collection 的同一 Shard 数据尽量集中加载，降低单次查询的节点 Fanout；
2. Fact 与其 AMV 的对应 Shard 尽量位于相同或网络邻近的 QueryNode；
3. Partition-local AMV 的对应 Fact Partition 和 AMV Partition 尽量协同加载；
4. Locality 是 Balance 的优化目标，不作为影响可用性的硬约束；
5. 资源不均衡或节点故障时允许打破 Locality，查询通过正常分布式 Reduce 保证结果正确。
