# Lab 03 深度设计规格：流式增量聚合黄金组合

## 1. 架构对决：新手陷阱 vs 工业级方案

| 方案对比 | 仅使用 `ProcessWindowFunction` | 仅使用 `AggregateFunction` | 黄金组合：`AggregateFunction` + `ProcessWindowFunction` |
|---|---|---|---|
| **状态存储** | 缓存窗口期内的**全部原始事件明细** | 仅缓存单一的累加器（Accumulator） | 仅缓存单一的累加器（Accumulator） |
| **内存复杂度** | $O(N)$（高频每秒百万笔时必 OOM） | $O(1)$（极致高效） | $O(1)$（极致高效） |
| **元数据感知** | 极其丰富（能获取 `Window` 起止时间、Key） | **无**（完全不知道所属窗口与 Key） | **兼得**（触发时将累加器传入 Process 函数补充元数据） |
| **生产适用性** | ❌ 仅适合极小数据量/全量排序 | ⚠️ 缺乏窗口时间戳，下游无法落湖分桶 | ✅ **工业界统一标准最佳实践** |

## 2. 算子结构设计

### 累加器实体 (Accumulator)
```java
public class TxSummaryAccumulator {
    private BigDecimal totalExpense = BigDecimal.ZERO;
    private long txCount = 0L;
    private BigDecimal maxSingleAmount = BigDecimal.ZERO;
    private String maxMerchant = "";
    // 纯粹的 in-place 更新方法
    public void add(BigDecimal amount, String merchant) { ... }
}
```

### 增量函数接口：`AggregateFunction<IN, ACC, OUT>`
- `createAccumulator()`: 初始化轻量累加器。
- `add(value, accumulator)`: **每来一条数据立即更新，不保留原事件引用**。
- `getResult(accumulator)`: 窗口闭合时将累加器提炼为中间聚合结果。
- `merge(a, b)`: 会话窗口合并时使用。

### 窗口上下文包装：`ProcessWindowFunction<IN, OUT, KEY, W>`
- 输入的 `Iterable<AggResult> elements` **被 Flink 保证仅有 1 个元素**（即 `AggregateFunction` 产出的增量结果）。
- 专职将 `context.window().getStart()` 与 `context.window().getEnd()` 填充进领域模型。

## 3. 实验验证目标与断言
1. **增量调用次数断言**：断言输入 $N$ 条数据时，`add()` 精确被调用 $N$ 次。
2. **上下文透传断言**：最终下游接收到的输出行不仅包含精确的累计金额，还包含窗口的精确起止时间戳。
3. **高吞吐压力断言**：在本地模拟单 Key 注入 100,000 条动账事件，验证堆内存无明显爬升，瞬间完成聚合。
