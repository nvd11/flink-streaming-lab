# Lab 04 深度设计规格：多并发与空闲通道 (Idleness)

## 1. 生产隐患：分布式时钟的木桶效应（主卡疯狂转账，副卡在抽屉吃灰，为什么连主卡警报都被卡死了？）
在 Flink 中，当下游窗口算子接收来自上游多个并行分区（如 Kafka `sms-tx-events` 的 2 个 Partition）的数据流时，其当前的时间推进遵循死板的**木桶原理（Min-Rule：必须等所有通道的时间都走到 `10:05`，才敢合上 `10:00~10:05` 的账本）**：
$$\text{CurrentWatermark}_{\text{downstream}} = \min_{i \in \text{Channels}}(\text{Watermark}_i)$$

### 事故模型（主卡被副卡拖死）
设 Kafka 主题 `sms-tx-events` 拥有 2 个物理分区，Flink 消费并发度为 2：
- **Subtask 0 (Partition 0 · 主卡 `3342`)**: 短短 2 分钟内连续转出 `3000 + 5000 = 8000 元`（远超 `5000 元` 报警红线！），且后续消费已将 Subtask 0 的 Watermark 推进到 `10:10:00`；
- **Subtask 1 (Partition 1 · 副卡 `8888`)**: 锁在抽屉里备用的副卡，一上午一分钱没花，一条短信都没有，Watermark 死死停滞在初始值 $-\infty$。

**灾难后果**：因为 $\min(\text{10:10:00}, -\infty) = -\infty$，下游所有基于 Event Time 的 5 分钟窗口**被彻底冻结，永远不会结账触发**！主卡 `3342` 那 `8000 元` 的救命转账警报被死死卡在内存里发不出来，直到副卡 `8888` 碰巧刷了一笔钱为止！

## 2. 破局之道：`withIdleness`（如果哪条通道睡着了，就暂时别等它！）
Flink 引入了**空闲通道检测（Watermark Idleness Detection）**：
```java
WatermarkStrategy.<FinancialTxEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withIdleness(Duration.ofSeconds(3));
```

### 状态机推导
1. 当 Subtask 1（副卡通道）在指定的超时时间（如 3 秒）内没有收到任何新短信时，Flink 自动将其通道状态由 `ACTIVE` 切换为 `IDLE`（标记为睡着了）。
2. 一旦通道被标记为 `IDLE`，下游算子计算最小水位线时**自动剔除该睡觉通道**，只看醒着的通道：
   $$\text{CurrentWatermark}_{\text{downstream}} = \min_{i \in \text{ACTIVE Channels}}(\text{Watermark}_i)$$
3. 此时主卡 `3342` 的时间 `10:10:00` 瞬间生效，`[10:00, 10:05)` 账本立刻合上，**准时发出 `8000 > 5000` 的大额转账警报**！
4. 当哪天副卡 `8888` 重新刷卡发来短信时，该通道自动恢复为 `ACTIVE`，重新纳入全局时钟管理。

## 3. 实验验证目标与断言
1. **死锁复现断言**：在未配置 `withIdleness` 且 Partition 1（副卡）静默无数据时，证明即便 Partition 0（主卡 `3342`）注入了 `8000 元` 转账并将事件时间推过 `10:10:00`，下游 5 分钟窗口也绝对不触发，`transferAlert` 警报被彻底卡死。
2. **破局解封断言**：配置 `withIdleness` 后，证明在经过空闲超时等待后，睡着的副卡通道被自动剔除，主卡 `3342` 的 5 分钟统计与 `transferAlert = true` 报警立刻破局输出！
3. **动态恢复断言**：在副卡通道被标记为 `IDLE` 后，向 Partition 1 注入副卡 `8888` 的转账数据，验证通道无缝重新激活并独立产出副卡的 5 分钟账单。
