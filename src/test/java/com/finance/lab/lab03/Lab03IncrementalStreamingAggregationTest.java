package com.finance.lab.lab03;

import com.finance.lab.model.FinancialTxEvent;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Lab 03: 流式增量聚合黄金组合 (AggregateFunction + ProcessWindowFunction) 实验")
class Lab03IncrementalStreamingAggregationTest {
    private static final Logger LOG = LoggerFactory.getLogger(Lab03IncrementalStreamingAggregationTest.class);

    // 共享计数器，用于严格断言 add() 的增量调用特性
    private static final AtomicInteger ADD_CALL_COUNT = new AtomicInteger(0);

    /**
     * 轻量累加器: 内存 O(1) 紧凑存储，拒绝全量明细缓存
     */
    public static class TxSummaryAccumulator implements Serializable {
        private static final long serialVersionUID = 1L;

        private BigDecimal totalExpense = BigDecimal.ZERO;
        private long txCount = 0L;
        private BigDecimal maxSingleAmount = BigDecimal.ZERO;
        private String maxMerchant = "";

        public TxSummaryAccumulator() {}

        public void add(BigDecimal amount, String merchant) {
            this.totalExpense = this.totalExpense.add(amount);
            this.txCount++;
            if (amount.compareTo(this.maxSingleAmount) > 0) {
                this.maxSingleAmount = amount;
                this.maxMerchant = merchant;
            }
        }

        public BigDecimal getTotalExpense() {
            return totalExpense;
        }

        public void setTotalExpense(BigDecimal totalExpense) {
            this.totalExpense = totalExpense;
        }

        public long getTxCount() {
            return txCount;
        }

        public void setTxCount(long txCount) {
            this.txCount = txCount;
        }

        public BigDecimal getMaxSingleAmount() {
            return maxSingleAmount;
        }

        public void setMaxSingleAmount(BigDecimal maxSingleAmount) {
            this.maxSingleAmount = maxSingleAmount;
        }

        public String getMaxMerchant() {
            return maxMerchant;
        }

        public void setMaxMerchant(String maxMerchant) {
            this.maxMerchant = maxMerchant;
        }
    }

    /**
     * 最终聚合产出结果: 兼具业务统计数据与窗口元数据 (Window Start/End)
     */
    public static class WindowAggResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private long windowStart;
        private long windowEnd;
        private String cardTail;
        private long txCount;
        private BigDecimal totalExpense;
        private BigDecimal maxSingleAmount;
        private String maxMerchant;

        public WindowAggResult() {}

        public WindowAggResult(long windowStart, long windowEnd, String cardTail,
                               long txCount, BigDecimal totalExpense,
                               BigDecimal maxSingleAmount, String maxMerchant) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.cardTail = cardTail;
            this.txCount = txCount;
            this.totalExpense = totalExpense;
            this.maxSingleAmount = maxSingleAmount;
            this.maxMerchant = maxMerchant;
        }

        public long getWindowStart() { return windowStart; }
        public long getWindowEnd() { return windowEnd; }
        public String getCardTail() { return cardTail; }
        public long getTxCount() { return txCount; }
        public BigDecimal getTotalExpense() { return totalExpense; }
        public BigDecimal getMaxSingleAmount() { return maxSingleAmount; }
        public String getMaxMerchant() { return maxMerchant; }

        @Override
        public String toString() {
            return String.format("Window[%d-%d] Card:%s Count:%d Sum:%s MaxTx:(%s, %s)",
                    windowStart, windowEnd, cardTail, txCount, totalExpense.toPlainString(),
                    maxMerchant, maxSingleAmount.toPlainString());
        }
    }

    /**
     * 增量计算核心: AggregateFunction
     */
    public static class TxAggFunction implements AggregateFunction<FinancialTxEvent, TxSummaryAccumulator, TxSummaryAccumulator> {
        private static final long serialVersionUID = 1L;

        @Override
        public TxSummaryAccumulator createAccumulator() {
            return new TxSummaryAccumulator();
        }

        @Override
        public TxSummaryAccumulator add(FinancialTxEvent value, TxSummaryAccumulator accumulator) {
            ADD_CALL_COUNT.incrementAndGet();
            accumulator.add(value.getAmount(), value.getMerchant());
            return accumulator;
        }

        @Override
        public TxSummaryAccumulator getResult(TxSummaryAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public TxSummaryAccumulator merge(TxSummaryAccumulator a, TxSummaryAccumulator b) {
            TxSummaryAccumulator merged = new TxSummaryAccumulator();
            merged.totalExpense = a.totalExpense.add(b.totalExpense);
            merged.txCount = a.txCount + b.txCount;
            if (a.maxSingleAmount.compareTo(b.maxSingleAmount) >= 0) {
                merged.maxSingleAmount = a.maxSingleAmount;
                merged.maxMerchant = a.maxMerchant;
            } else {
                merged.maxSingleAmount = b.maxSingleAmount;
                merged.maxMerchant = b.maxMerchant;
            }
            return merged;
        }
    }

    /**
     * 窗口上下文赋予: ProcessWindowFunction (输入被保证仅有 1 个累加器提炼结果)
     */
    public static class TxProcessWindowFunction extends ProcessWindowFunction<TxSummaryAccumulator, WindowAggResult, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(String cardTail,
                            Context context,
                            Iterable<TxSummaryAccumulator> elements,
                            Collector<WindowAggResult> out) {
            // 关键工业界黄金法则断言: elements 迭代器绝对只有 1 个累加器产出结果!
            TxSummaryAccumulator acc = elements.iterator().next();
            WindowAggResult result = new WindowAggResult(
                    context.window().getStart(),
                    context.window().getEnd(),
                    cardTail,
                    acc.getTxCount(),
                    acc.getTotalExpense(),
                    acc.getMaxSingleAmount(),
                    acc.getMaxMerchant()
            );
            out.collect(result);
        }
    }

    @Test
    @DisplayName("测试增量聚合与窗口元数据结合: O(1) 内存更新 + 触发时透传 Window 信息")
    void testIncrementalAggregationWithWindowContext() throws Exception {
        ADD_CALL_COUNT.set(0);

        Instant baseTime = Instant.parse("2026-10-10T10:00:00Z");
        long t0 = baseTime.toEpochMilli();

        WindowOperatorBuilder<FinancialTxEvent, String, TimeWindow> builder =
                new WindowOperatorBuilder<>(
                        TumblingEventTimeWindows.of(Duration.ofSeconds(10)),
                        EventTimeTrigger.create(),
                        new ExecutionConfig(),
                        Types.POJO(FinancialTxEvent.class),
                        FinancialTxEvent::getCardTail,
                        Types.STRING
                );

        WindowOperator<String, FinancialTxEvent, ?, WindowAggResult, TimeWindow> windowOperator =
                builder.aggregate(
                        new TxAggFunction(),
                        new TxProcessWindowFunction(),
                        Types.POJO(TxSummaryAccumulator.class)
                );

        KeyedOneInputStreamOperatorTestHarness<String, FinancialTxEvent, WindowAggResult> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        windowOperator,
                        FinancialTxEvent::getCardTail,
                        Types.STRING
                );

        harness.open();

        // 注入 3 笔连续动账交易
        FinancialTxEvent e1 = new FinancialTxEvent("TX-01", "CARD-6666", baseTime.plusSeconds(1), new BigDecimal("15.50"), "瑞幸咖啡", "FOOD");
        FinancialTxEvent e2 = new FinancialTxEvent("TX-02", "CARD-6666", baseTime.plusSeconds(3), new BigDecimal("88.00"), "山姆会员店", "SHOPPING");
        FinancialTxEvent e3 = new FinancialTxEvent("TX-03", "CARD-6666", baseTime.plusSeconds(8), new BigDecimal("32.50"), "美团外卖", "FOOD");

        harness.processElement(new StreamRecord<>(e1, t0 + 1000));
        harness.processElement(new StreamRecord<>(e2, t0 + 3000));
        harness.processElement(new StreamRecord<>(e3, t0 + 8000));

        // 验证断言 1: add() 精确被调用 3 次 (每来一条立刻增量计算，绝不堆积原始明细)
        assertThat(ADD_CALL_COUNT.get()).isEqualTo(3);

        // 此时窗口尚未闭合，主流输出为空
        assertThat(harness.extractOutputStreamRecords()).isEmpty();

        // 推进 Watermark 至窗口结束时间 10:00:10
        harness.processWatermark(t0 + 10000);

        // 验证断言 2: 窗口触发且仅输出 1 条聚合结果，同时携带了完整的窗口起止元数据
        List<?> records = harness.extractOutputStreamRecords();
        assertThat(records).hasSize(1);

        StreamRecord<?> record = (StreamRecord<?>) records.get(0);
        WindowAggResult result = (WindowAggResult) record.getValue();

        assertThat(result.getWindowStart()).isEqualTo(t0);
        assertThat(result.getWindowEnd()).isEqualTo(t0 + 10000);
        assertThat(result.getCardTail()).isEqualTo("CARD-6666");
        assertThat(result.getTxCount()).isEqualTo(3L);
        assertThat(result.getTotalExpense()).isEqualByComparingTo(new BigDecimal("136.00")); // 15.5 + 88.0 + 32.5 = 136.00
        assertThat(result.getMaxSingleAmount()).isEqualByComparingTo(new BigDecimal("88.00"));
        assertThat(result.getMaxMerchant()).isEqualTo("山姆会员店");

        LOG.info("[Lab 03] 增量聚合输出验证成功: {}", result);

        harness.close();
    }

    @Test
    @DisplayName("高吞吐压力验证: 单 Key 注入 10,000 笔事件，验证 O(1) 增量更新与瞬时触发")
    void testHighThroughputIncrementalAggregation() throws Exception {
        ADD_CALL_COUNT.set(0);

        Instant baseTime = Instant.parse("2026-10-10T10:00:00Z");
        long t0 = baseTime.toEpochMilli();

        WindowOperatorBuilder<FinancialTxEvent, String, TimeWindow> builder =
                new WindowOperatorBuilder<>(
                        TumblingEventTimeWindows.of(Duration.ofSeconds(60)),
                        EventTimeTrigger.create(),
                        new ExecutionConfig(),
                        Types.POJO(FinancialTxEvent.class),
                        FinancialTxEvent::getCardTail,
                        Types.STRING
                );

        WindowOperator<String, FinancialTxEvent, ?, WindowAggResult, TimeWindow> windowOperator =
                builder.aggregate(
                        new TxAggFunction(),
                        new TxProcessWindowFunction(),
                        Types.POJO(TxSummaryAccumulator.class)
                );

        KeyedOneInputStreamOperatorTestHarness<String, FinancialTxEvent, WindowAggResult> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        windowOperator,
                        FinancialTxEvent::getCardTail,
                        Types.STRING
                );

        harness.open();

        final int N = 10000;
        BigDecimal expectedTotal = BigDecimal.ZERO;
        for (int i = 0; i < N; i++) {
            BigDecimal amount = BigDecimal.valueOf(1.50);
            expectedTotal = expectedTotal.add(amount);
            FinancialTxEvent event = new FinancialTxEvent(
                    "BATCH-TX-" + i,
                    "CARD-9999",
                    baseTime.plusMillis(i),
                    amount,
                    "测试商户",
                    "DAILY"
            );
            harness.processElement(new StreamRecord<>(event, t0 + i));
        }

        // 验证增量执行 10000 次
        assertThat(ADD_CALL_COUNT.get()).isEqualTo(N);

        // 推进 Watermark 触发 60s 窗口
        harness.processWatermark(t0 + 60000);

        List<?> records = harness.extractOutputStreamRecords();
        assertThat(records).hasSize(1);
        WindowAggResult result = (WindowAggResult) ((StreamRecord<?>) records.get(0)).getValue();

        assertThat(result.getTxCount()).isEqualTo((long) N);
        assertThat(result.getTotalExpense()).isEqualByComparingTo(expectedTotal);
        LOG.info("[Lab 03] 高吞吐 10,000 笔增量聚合压测通过: TxCount={}, TotalExpense={}", result.getTxCount(), result.getTotalExpense());

        harness.close();
    }
}
