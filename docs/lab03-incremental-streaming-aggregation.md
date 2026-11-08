# Lab 03 深度设计规格：流式增量聚合黄金组合

## 1. 架构对决：新手陷阱 vs 工业级方案

| 方案对比 | 仅使用 `ProcessWindowFunction` | 仅使用 `AggregateFunction` | 黄金组合：`AggregateFunction` + `ProcessWindowFunction` |
|---|---|---|---|
| **状态存储** | 缓存窗口期内的**全部原始事件明细** | 仅缓存单一的累加器（Accumulator） | 仅缓存单一的累加器（Accumulator） |
| **内存复杂度** | $O(N)$（高频每秒百万笔时必 OOM） | $O(1)$（极致高效） | $O(1)$（极致高效） |
| **元数据感知** | 极其丰富（能获取 `Window` 起止时间、Key） | **无**（完全不知道所属窗口与 Key） | **兼得**（触发时将累加器传入 Process 函数补充元数据） |
| **生产适用性** | 仅适合极小数据量/全量排序 | 缺乏窗口时间戳，下游无法落湖分桶 | **工业界统一标准最佳实践** |

## 2. 算子结构设计

### 累加器实体 (Accumulator)
```java
public class ExpenseRiskAccumulator implements Serializable {
    private long totalTxCount = 0L;                          // 5分钟窗口内支出项总数
    private BigDecimal totalExpenseAmount = BigDecimal.ZERO; // 5分钟窗口内支出总金额
    private long transferTxCount = 0L;                       // 其中：个人转账 (PERSONAL_TRANSFER) 笔数
    private BigDecimal transferAmount = BigDecimal.ZERO;     // 其中：个人转账累计金额 (> 5000 触发报警)

    // 纯粹的 O(1) in-place 更新方法
    public void add(BigDecimal amount, String category) {
        this.totalTxCount++;
        this.totalExpenseAmount = this.totalExpenseAmount.add(amount);
        if ("PERSONAL_TRANSFER".equalsIgnoreCase(category)) {
            this.transferTxCount++;
            this.transferAmount = this.transferAmount.add(amount);
        }
    }
}
```

### 增量函数接口：`AggregateFunction<IN, ACC, OUT>`
- `createAccumulator()`: 初始化轻量累加器。
- `add(value, accumulator)`: **每来一条数据立即更新，不保留原事件引用**。
- `getResult(accumulator)`: 窗口闭合时将累加器提炼为中间聚合结果。
- `merge(a, b)`: 会话窗口合并时使用。

### 窗口上下文包装：`ProcessWindowFunction<IN, OUT, KEY, W>`
- 输入的 `Iterable<ExpenseRiskAccumulator> elements` **被 Flink 保证仅有 1 个元素**（即 `AggregateFunction` 产出的增量结果）。
- 专职将 `context.window().getStart()` 与 `context.window().getEnd()` 填充进领域模型，并执行 `acc.getTransferAmount().compareTo(new BigDecimal("5000.00")) > 0` 阈值判定，打上 `transferAlert = true/false` 报警标签。

## 3. 实验验证目标与断言（5分钟刷 10 万次卡也不撑爆内存）
1. **增量调用次数断言**：断言单张卡（如 `cardTail = "6666"`）在 5 分钟窗口内输入 $N$ 条支出明细时，`add()` 精确被调用 $N$ 次，每来一笔立即累加后释放原事件引用。
2. **转账风控与上下文透传断言**：最终下游接收到的 `WindowAggResult` 不仅包含精确的 5 分钟总支出笔数、总支出金额、转账总额与 `transferAlert = true`（当转账 > 5000 元时），还包含窗口的精确起止时间戳 `[10:00:00, 10:05:00)`。
3. **高吞吐压力断言**：在本地模拟单卡 5 分钟内注入 10,000+ 条动账事件，验证堆内存始终保持 $O(1)$ 无爬升，瞬间完成聚合与报警裁决。
