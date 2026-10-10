# Lab 04 深度设计规格：多并发与空闲通道 (Idleness)

## 1. 生产隐患：分布式时钟的木桶效应 (The Weakest Link)
在 Flink 中，当一个算子接收来自上游多个并行分区（Partitions / Subtasks）的数据流时，其当前的时间推进遵循**木桶原理（Min-Rule）**：
$$\text{CurrentWatermark}_{\text{downstream}} = \min_{i \in \text{Channels}}(\text{Watermark}_i)$$

### 事故模型
设采集任务有 2 个并行度：
- **Subtask 0 (SIM_1)**: 主卡，每秒产生 10 笔消费，Watermark 飞速推进到 `10:05:00`；
- **Subtask 1 (SIM_2)**: 副卡，静默备用，长时间没有动账短信，Watermark 停滞在初始值 $-\infty$。

**灾难后果**：下游所有基于 Event Time 的窗口**被彻底冻结，永远不会触发**！整个湖仓的实时大盘静止不动，直到 SIM_2 突然来了一条短信。

## 2. 破局之道：`withIdleness`
Flink 引入了**空闲通道标记（Watermark Idleness Detection）**：
```java
WatermarkStrategy.<FinancialTxEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withIdleness(Duration.ofSeconds(3));
```

### 状态机推导
1. 当 Subtask 1 在指定的超时时间（如 3 秒）内没有收到新数据时，Flink 将其输入流状态由 `ACTIVE` 切换为 `IDLE`。
2. 一旦通道被标记为 `IDLE`，下游算子计算最小水位线时**自动剔除该通道**：
   $$\text{CurrentWatermark}_{\text{downstream}} = \min_{i \in \text{ACTIVE Channels}}(\text{Watermark}_i)$$
3. 当 Subtask 1 恢复发射数据时，该通道重新变为 `ACTIVE`，下游重新将其纳入 Min-Rule 裁决。

## 3. 实验验证目标与断言
1. **死锁复现断言**：在未配置 `withIdleness` 且 Subtask 1 静默时，证明下游 5 秒滚动窗口即便 Subtask 0 注入了跨越数十分钟的数据，也绝对不触发。
2. **破局解封断言**：配置 `withIdleness(Duration.ofSeconds(2))` 后，证明在等待 2 秒超时后，窗口顺利被触发并输出结果。
3. **动态恢复断言**：在被标记为 `IDLE` 后，再次向 Subtask 1 注入数据，验证通道无缝激活恢复。
