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

## 五大核心实验体系 (The 5 Labs Matrix)

| 实验编号 | 实验主题 | 核心攻克技术点 | 生产对应真实场景 | 详细规格 |
|---|---|---|---|---|
| **Lab 01** | **Watermark 推进与乱序时空** | `BoundedOutOfOrderness`、事件时间戳提取、单调递增承诺 | 移动端离线重连、弱网动账乱序上报 | [Lab 01 规格文档](docs/lab01-watermark-and-event-time.md) |
| **Lab 02** | **窗口生命周期与迟到三部曲** | Tumbling/Sliding Window、`allowedLateness` 二次触发、`SideOutput` 兜底 | 银联/跨行跨日冲正、退款超晚到达平账 | [Lab 02 规格文档](docs/lab02-window-lifecycle-and-late-data.md) |
| **Lab 03** | **流式聚合黄金组合** | `AggregateFunction` (增量 $O(1)$ 内存) + `ProcessWindowFunction` (元数据透传) | 实时大屏百亿级动账实时指标、抗 OOM | [Lab 03 规格文档](docs/lab03-incremental-streaming-aggregation.md) |
| **Lab 04** | **多并发与空闲通道 (Idleness)** | 并发分区 Min-Watermark 木桶效应死锁、`withIdleness` 破局机制 | 双 SIM 卡采集通道，备用卡槽长期静默导致全局死锁 | [Lab 04 规格文档](docs/lab04-parallelism-and-idle-sources.md) |
| **Lab 05** | **会话窗口与动态动账分析** | `ProcessingTimeSessionWindows` / `EventTimeSessionWindows` (动态间隙) | 连续刷卡盗刷防风控、同店高频连续支付会话合并 | [Lab 05 规格文档](docs/lab05-session-windows-and-patterns.md) |

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
│   ├── gcp-pubsub-vs-kafka-message-delay-mechanisms.md # 深度架构长文：Pub/Sub vs Kafka 延迟与卡死治理全解
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

