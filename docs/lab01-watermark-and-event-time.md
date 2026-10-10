# Lab 01 深度设计规格：Watermark 推进与乱序时空

## 1. 实验背景与业务痛点
在短信或银行 Webhook 动账采集场景中，由于客户端重试、4G/5G 弱网、运营商网关批处理排队，事件到达 Flink 的顺序往往与其实际物理发生时间严重脱节。
- 业务发生时间戳：`2026-10-10 10:00:00`
- 网络到达时间戳：`2026-10-10 10:00:08` (延迟 8 秒)

如果在流处理中采用机器的 Processing Time，会导致该笔消费被错误归类到后续分钟的统计大盘中，造成指标漂移。

## 2. 核心数学模型与水位线生成
Watermark 本质是单调递增的时间截断信号：
$$\text{Watermark}(t) = \max_{i \le t}(\text{event\_time}_i) - \text{out\_of\_orderness\_delay}$$

### 实验参数矩阵
- **容忍乱序延迟 (Bounded Out-Of-Orderness)**: 5 秒 (`Duration.ofSeconds(5)`)
- **时钟推进周期 (Watermark Interval)**: 200 毫秒 (`pipeline.auto-watermark-interval`)

### 模拟时序序列设计
| 事件序号 | 交易ID | 业务时间 (Event Time) | 到达顺序 | 期望推进的 Watermark | 说明 |
|---|---|---|---|---|---|
| E1 | TX-1 | 10:00:00 (0s) | 1 | -5000ms | 初始启动，最大事件时间 0s |
| E2 | TX-2 | 10:00:05 (5s) | 2 | 0ms (10:00:00) | 最大事件时间跃迁至 5s，Watermark 推进至 0s |
| E3 | TX-3 | 10:00:02 (2s) | 3 (乱序) | 0ms (保持) | 业务时间落后，Watermark 严格单调不回退 |
| E4 | TX-4 | 10:00:15 (15s)| 4 | 10:00:10 (10s) | 最大时间大幅推进，Watermark 跃升到 10s |
| E5 | TX-5 | 10:00:08 (8s) | 5 (迟到) | 10:00:10 (保持) | 此时 Event Time 8s < Watermark 10s，判定为迟到事件 |

## 3. 实验验证指标与断言
1. **单调递增性断言**：下游观察到的 Watermark 无论上游如何乱序，绝不出现 $W_{t+1} < W_t$。
2. **延迟容忍有效性断言**：在容忍阈值范围内的乱序数据（如 E3），其处理行为与正常数据无异。
3. **迟到判定断言**：超出容忍阈值的数据（如 E5），能够被 `context.timestamp() < context.timerService().currentWatermark()` 准确识别。
