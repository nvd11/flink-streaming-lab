# flink-streaming-lab (Flink 无界流计算深度实验室)

> “批是流的特例，理解水位线与无界聚合才是流计算的灵魂。”  
> 本项目是专为深入掌握 **Apache Flink 实时流计算引擎** 量身打造的实验场与深度研究平台。旨在以严密、可复现、纯函数式的测试用例与实验设计，彻底吃透 **Event Time 时钟推进、Watermark 倾斜死锁、Window 触发与重开生命周期、O(1) 增量聚合以及迟到数据兜底治理**。

---

## 实验室核心使命 (Mission)

在批处理中，世界是有界的、静态的，数据可以等待齐备后再做全量切片。但真实世界的金融动账、交易流水与物联传感是**无界的（Unbounded）**，数据必然面临**网络抖动、跨时区延迟、分布式乱序与空闲通道倾斜**。

本项目旨在通过 5 大体系化实验（Labs），完成从“批处理工程师”到“真正精通 Flink 流计算架构师”的认知蜕变：

1. **时钟哲学 (Time & Watermark)**：彻底厘清 Processing Time、Ingestion Time 与 Event Time，亲眼见证单调推进的物理承诺。
2. **迟到治理 (Late Data Governance)**：掌握迟到数据的“三道防线”（乱序容忍、AllowedLateness 重开修正、SideOutput 侧输出绝对兜底）。
3. **聚合心法 (Streaming Aggregation)**：告别新手全量缓存的 OOM 陷阱，掌握 `AggregateFunction` (增量 $O(1)$) + `ProcessWindowFunction` (窗口上下文) 的生产级黄金组合。
4. **木桶死锁 (Watermark Idleness)**：复现并攻克多并发多卡槽下的空闲通道（Idle Source）时钟冻结生产事故。
5. **动态行为 (Session & Complex Event)**：掌握非固定时间跨度的会话窗口（Session Window），捕捉连续高频动账与行为归因。

---

## 核心业务与聚合运算规则 (Unified Business & Aggregation Rules)

本实验室以一套统一的**个人金融动账监控与大额转账风控规则**贯穿全部 7 大实验目标（Lab 00 基准 ~ Lab 05 进阶 + Lab 06 端到端 R2 Iceberg 入湖）：

* **分组维度 (`keyBy`)**：按银行卡尾号 **`cardTail`** 分组（每张卡独立维护状态与窗口）；
* **时间窗口基准**：**5 分钟事件时间窗口**（`Duration.ofMinutes(5)`，支持滚动窗口与动态会话窗口对比）；
* **增量聚合指标 ($O(1)$ 状态)**：
  1. **支出项总数 (`totalTxCount`)**：该卡在 5 分钟窗口内的全部支出笔数；
  2. **支出总金额 (`totalExpenseAmount`)**：该卡在 5 分钟窗口内的全部支出累计金额；
  3. **转账支出累计 (`transferAmount`)**：专门累加类别为 `PERSONAL_TRANSFER`（个人转账）的支出金额；
* **实时风控报警规则 (`transferAlert`)**：
  * **若单张卡在 5 分钟窗口内累计转账支出 `transferAmount > 5000` 元，立即触发大额转账风控报警（`transferAlert = true`）！**

---

## 七大核心实验体系 (The 7 Labs Matrix: Lab 00 ~ Lab 06)

| 实验编号 | 实验主题 | 核心攻克技术点 | 对应“5分钟统计+超5000转账报警”验证场景（大白话目标） | 详细规格 |
|---|---|---|---|---|
| **Lab 00** | **Happy Flow 基准阳光路径** | `keyBy(cardTail)`、5分钟滚动窗口、正反双向阈值报警基线 | **天下太平时的标准算账**：所有短信按顺序到达，卡`3342`转 1000 元安全不报警，卡`8888`转 5500 元准点拉响警报 | [Lab 00 规格文档](docs/lab00-happy-flow-baseline.md) |
| **Lab 01** | **Watermark 推进与乱序时空** | `BoundedOutOfOrderness`、事件时间戳提取、单调递增承诺 | **手机信号差晚到几秒也不漏报**：进电梯晚到 5 秒的 2500 元转账仍归入原 5 分钟账本，凑够 5500 元成功拉响警报 | [Lab 01 规格文档](docs/lab01-watermark-and-event-time.md) |
| **Lab 02** | **窗口生命周期与迟到三部曲** | Tumbling Window、`allowedLateness` 二次触发、`SideOutput` 兜底 | **结完账才来旧短信的三道防线**：首次结账 4000 元未报警；1分钟内补来 2000 元翻开重算补发警报(`v2`)；半小时后才来则记入错账本 | [Lab 02 规格文档](docs/lab02-window-lifecycle-and-late-data.md) |
| **Lab 03** | **流式聚合黄金组合** | `AggregateFunction` (增量 $O(1)$ 内存) + `ProcessWindowFunction` (元数据透传) | **狂刷 10 万次卡也不撑爆内存**：内存只放一张写着 3 个数字的草稿纸原地累加，5 分钟到点看一眼转账超没超 5000 并盖上时间章 | [Lab 03 规格文档](docs/lab03-incremental-streaming-aggregation.md) |
| **Lab 04** | **多并发与空闲通道 (Idleness)** | 并发分区 Min-Watermark 木桶效应死锁、`withIdleness` 破局机制 | **副卡锁抽屉不拖死主卡警报**：2 个 Kafka 通道中副卡一毛不拔，自动标记睡觉跳过，确保主卡 8000 元转账警报准时发出 | [Lab 04 规格文档](docs/lab04-parallelism-and-idle-sources.md) |
| **Lab 05** | **会话窗口与跨边界防拆单逃逸** | `EventTimeSessionWindows` (5分钟动态间隙 `merge`) vs 固定滚动窗口 | **识破骗子卡点拆单转账**：骗子在 `04:50` 和 `05:10` 各转 3000 元企图跨整点逃避监控，会话窗口将间隔不足 5 分钟的转账粘合成 6000 元当场抓获 | [Lab 05 规格文档](docs/lab05-session-windows-and-patterns.md) |
| **Lab 06** | **端到端 Kafka $\to$ R2 Iceberg 入湖** | Kafka SASL 消费、2PC 两阶段提交 R2 Iceberg、`IcebergGenerics` 进程内断言 | **真刀真枪上 NUC 算账入库**：拉取真实手机 Kafka 短信，算完的 5 分钟账单、超 5000 警报与迟到错账本全部存进 Cloudflare R2 数据湖并自验 | [技术选型与架构](docs/technical-stack-selection.md) |

---

## 技术基座与实验兵器库 (Tech Stack)

* **核心引擎**: Apache Flink `1.20.0` (Stream API)
* **语言标准**: Java `21` (LTS, 采用现代 Switch 模式匹配与虚拟线程友好语法)
* **单测框架**: JUnit `5.10.2` + AssertJ `3.25.3` (流畅流式断言)
* **实验测试秘器**: 
  - `OneInputStreamOperatorTestHarness`: 毫秒级单算子时间控制器，**无需启动 MiniCluster**，以微秒级直接步进 Watermark 与处理时间！
  - `TwoInputStreamOperatorTestHarness`: 双流交互与广播流毫秒验证器。
* **日志体系**: SLF4J + Logback (高对比度彩色时序日志，清晰观测 Watermark 跃迁)

---

## 极速开始 (Quick Start)

### 1. 运行全部实验用例
```bash
mvn clean test
```

### 2. 针对指定 Lab 进行交互式观测
```bash
# 运行 Lab 01：观测 Watermark 在乱序流中的推进
mvn test -Dtest=Lab01WatermarkProgressionTest

# 运行 Lab 02：观测迟到数据如何二次修正窗口与落入侧输出
mvn test -Dtest=Lab02WindowLifecycleAndLateDataTest
```

---

## 目录索引与架构规划

```text
flink-streaming-lab/
├── README.md                                    # 实验总纲与架构需求说明 (本文件)
├── pom.xml                                      # 纯净 Maven 构建骨架 (Java 21 + Flink 1.20)
├── docs/                                        # 体系化架构推导与深度设计规格
│   ├── technical-stack-selection.md             # 核心技术选型：Java 21 + Flink 1.20 + Kafka + R2 Iceberg 流湖一体
│   ├── flink-streaming-vs-batch-dynamic-scaling-deep-dive.md # 深度架构长文：Flink 流批双引擎动态扩缩容机制演进与推导
│   ├── gcp-pubsub-vs-kafka-message-delay-mechanisms.md # 深度架构长文：Pub/Sub vs Kafka 延迟与卡死治理全解
│   ├── kafka-core-abstractions-architecture-deep-dive.md # 深度架构长文：Topic, Partition, Consumer Group 拓扑关系全解
│   ├── lab00-happy-flow-baseline.md             # Lab 00 深度设计规格 (Happy Flow 基准阳光路径)
│   ├── lab01-watermark-and-event-time.md        # Lab 01 深度设计规格
│   ├── lab02-window-lifecycle-and-late-data.md  # Lab 02 深度设计规格
│   ├── lab03-incremental-streaming-aggregation.md # Lab 03 深度设计规格
│   ├── lab04-parallelism-and-idle-sources.md    # Lab 04 深度设计规格
│   └── lab05-session-windows-and-patterns.md    # Lab 05 深度设计规格
└── src/
    ├── main/java/com/finance/lab/
    │   ├── model/                               # 动账事件领域实体 (FinancialTxEvent)
    │   ├── watermark/                           # 自定义时钟与提取器策略
    │   ├── window/                              # 增量聚合与窗口处理函数
    │   └── common/                              # 彩色日志与测试工具辅助
    └── test/java/com/finance/lab/
        ├── lab01/                               # Lab 01 单元测试与时钟推进实验
        ├── lab02/                               # Lab 02 窗口与迟到三道防线实验
        ├── lab03/                               # Lab 03 增量预聚合 O(1) 验证实验
        ├── lab04/                               # Lab 04 多通道 Idleness 木桶破局实验
        └── lab05/                               # Lab 05 会话窗口动态合并实验
```

