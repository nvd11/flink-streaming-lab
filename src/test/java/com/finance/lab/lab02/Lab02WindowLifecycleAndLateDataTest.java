package com.finance.lab.lab02;

import com.finance.lab.model.FinancialTxEvent;
import org.apache.flink.api.common.ExecutionConfig;
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
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Lab 02: 窗口生命周期与迟到数据三道防线实验 (TestHarness 纳秒级时钟控制)")
class Lab02WindowLifecycleAndLateDataTest {
    private static final Logger LOG = LoggerFactory.getLogger(Lab02WindowLifecycleAndLateDataTest.class);

    private static final OutputTag<FinancialTxEvent> LATE_OUTPUT_TAG =
            new OutputTag<FinancialTxEvent>("late-financial-events") {};

    @Test
    @DisplayName("测试三道防线: 乱序容忍 -> allowedLateness 二次触发修正 -> SideOutput 兜底捕获")
    void testWindowLifecycleWithLateData() throws Exception {
        Instant baseTime = Instant.parse("2026-10-10T10:00:00Z");
        long t0 = baseTime.toEpochMilli();

        // 窗口定义: 10 秒滚动窗口 [10:00:00, 10:00:10)
        // 第一道防线: Watermark 推进控制 (手动驱动 processWatermark)
        // 第二道防线: allowedLateness = 5000ms
        // 第三道防线: sideOutputLateData(LATE_OUTPUT_TAG)
        WindowOperatorBuilder<FinancialTxEvent, String, TimeWindow> builder =
                new WindowOperatorBuilder<>(
                        TumblingEventTimeWindows.of(Duration.ofSeconds(10)),
                        EventTimeTrigger.create(),
                        new ExecutionConfig(),
                        Types.POJO(FinancialTxEvent.class),
                        FinancialTxEvent::getCardTail,
                        Types.STRING
                );

        builder.allowedLateness(Duration.ofSeconds(5));
        builder.sideOutputLateData(LATE_OUTPUT_TAG);

        WindowOperator<String, FinancialTxEvent, ?, String, TimeWindow> windowOperator =
                builder.process(new ProcessWindowFunction<FinancialTxEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(String cardTail,
                                        Context context,
                                        Iterable<FinancialTxEvent> elements,
                                        Collector<String> out) {
                        BigDecimal totalAmount = BigDecimal.ZERO;
                        int count = 0;
                        for (FinancialTxEvent e : elements) {
                            totalAmount = totalAmount.add(e.getAmount());
                            count++;
                        }
                        String result = String.format("Window[%d-%d] Card:%s Count:%d Total:%s",
                                context.window().getStart(),
                                context.window().getEnd(),
                                cardTail,
                                count,
                                totalAmount.toPlainString());
                        out.collect(result);
                    }
                });

        KeyedOneInputStreamOperatorTestHarness<String, FinancialTxEvent, String> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        windowOperator,
                        FinancialTxEvent::getCardTail,
                        Types.STRING
                );

        harness.open();

        // 1. [正常接收] 10:00:01 消费 10.00
        FinancialTxEvent tx1 = new FinancialTxEvent("TX-01", "CARD-8888", baseTime.plusSeconds(1), new BigDecimal("10.00"), "星巴克", "FOOD");
        harness.processElement(new StreamRecord<>(tx1, t0 + 1000));

        // 2. [正常接收] 10:00:05 消费 20.00
        FinancialTxEvent tx2 = new FinancialTxEvent("TX-02", "CARD-8888", baseTime.plusSeconds(5), new BigDecimal("20.00"), "麦当劳", "FOOD");
        harness.processElement(new StreamRecord<>(tx2, t0 + 5000));

        // 此时窗口尚未触发
        assertThat(harness.extractOutputStreamRecords()).isEmpty();

        // 3. [时钟推进 - 触发首次计算]
        // 推进 Watermark 到 10:00:10 (t0 + 10000ms)
        // 窗口 [t0, t0 + 10000) 的 maxTimestamp 为 t0 + 9999ms <= 10000ms，触发首次 Fire!
        harness.processWatermark(t0 + 10000);

        List<?> recordsAfterFirstFire = harness.extractOutputStreamRecords();
        assertThat(recordsAfterFirstFire).hasSize(1);
        StreamRecord<?> rec1 = (StreamRecord<?>) recordsAfterFirstFire.get(0);
        assertThat(rec1.getValue()).isEqualTo(String.format("Window[%d-%d] Card:CARD-8888 Count:2 Total:30.00", t0, t0 + 10000));
        LOG.info("[防线一检验] 首次触发成功: {}", rec1.getValue());

        // 4. [第二道防线: allowedLateness 保护期]
        // 到达迟到数据 10:00:08 (t0 + 8000ms)，属于 [t0, t0 + 10000)
        // 此时 Watermark 为 10000ms，小于窗口销毁时间 (10000 + 5000 = 15000ms)
        // 窗口再次 Fire，输出累加后的修正值 80.00!
        FinancialTxEvent tx3Late = new FinancialTxEvent("TX-03", "CARD-8888", baseTime.plusSeconds(8), new BigDecimal("50.00"), "屈臣氏", "SHOPPING");
        harness.processElement(new StreamRecord<>(tx3Late, t0 + 8000));

        List<?> recordsAfterSecondFire = harness.extractOutputStreamRecords();
        assertThat(recordsAfterSecondFire).hasSize(2);
        StreamRecord<?> rec2 = (StreamRecord<?>) recordsAfterSecondFire.get(1);
        assertThat(rec2.getValue()).isEqualTo(String.format("Window[%d-%d] Card:CARD-8888 Count:3 Total:80.00", t0, t0 + 10000));
        LOG.info("[防线二检验] allowedLateness 修正触发成功: {}", rec2.getValue());

        // 5. [时钟推进 - 彻底销毁窗口状态]
        // 推进 Watermark 到 10:00:16 (超过 10000 + 5000 = 15000ms 销毁线)
        harness.processWatermark(t0 + 16000);

        // 6. [第三道防线: 彻底超期落入 SideOutput]
        // 到达属于 [t0, t0 + 10000) 的极度迟到数据 10:00:03 (t0 + 3000ms)
        FinancialTxEvent tx4Dead = new FinancialTxEvent("TX-04", "CARD-8888", baseTime.plusSeconds(3), new BigDecimal("99.00"), "超期补录交易", "AUDIT");
        harness.processElement(new StreamRecord<>(tx4Dead, t0 + 3000));

        // 此时主流仍然只有前 2 次输出，绝无第 3 次输出
        assertThat(harness.extractOutputStreamRecords()).hasSize(2);

        // 侧输出流精确捕获该事件
        Queue<StreamRecord<FinancialTxEvent>> lateRecords = harness.getSideOutput(LATE_OUTPUT_TAG);
        assertThat(lateRecords).hasSize(1);
        FinancialTxEvent captured = lateRecords.peek().getValue();
        assertThat(captured.getTxId()).isEqualTo("TX-04");
        assertThat(captured.getAmount()).isEqualByComparingTo(new BigDecimal("99.00"));
        LOG.info("[防线三检验] SideOutput 兜底捕获成功: TxId={}, Amount={}", captured.getTxId(), captured.getAmount());

        harness.close();
        LOG.info("[Lab 02] 窗口生命周期与三道防线 TestHarness 验证完美通过!");
    }
}
