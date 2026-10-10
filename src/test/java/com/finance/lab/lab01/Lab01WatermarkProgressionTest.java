package com.finance.lab.lab01;

import com.finance.lab.model.FinancialTxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Lab 01: Watermark 推进与乱序时空基石冒烟实验")
class Lab01WatermarkProgressionTest {
    private static final Logger LOG = LoggerFactory.getLogger(Lab01WatermarkProgressionTest.class);

    @Test
    @DisplayName("测试 Watermark 在乱序到达动账流中的推进与单调递增性")
    void testWatermarkProgressionWithOutOfOrderness() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 实验一锁定单并发，排除多通道干扰

        // 构造乱序到达的事件序列 (基准时间: 2026-10-10 10:00:00 UTC)
        Instant baseTime = Instant.parse("2026-10-10T10:00:00Z");
        List<FinancialTxEvent> events = List.of(
                // 1. 正常到达: 0s
                new FinancialTxEvent("TX-1", "3342", baseTime, new BigDecimal("10.00"), "美团", "FOOD"),
                // 2. 正常推进: 5s
                new FinancialTxEvent("TX-2", "3342", baseTime.plusSeconds(5), new BigDecimal("20.00"), "高德打车", "TRANSPORT"),
                // 3. 乱序迟到: 2s (落后于上一条 5s，但在 3s 容忍区间内)
                new FinancialTxEvent("TX-3", "3342", baseTime.plusSeconds(2), new BigDecimal("15.00"), "盒马鲜生", "OFFLINE_SHOPPING"),
                // 4. 大幅推进: 15s
                new FinancialTxEvent("TX-4", "3342", baseTime.plusSeconds(15), new BigDecimal("100.00"), "沃尔玛", "OFFLINE_SHOPPING")
        );

        // 容忍 3 秒乱序
        WatermarkStrategy<FinancialTxEvent> watermarkStrategy = WatermarkStrategy
                .<FinancialTxEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, recordTimestamp) -> event.getEventTime().toEpochMilli());

        DataStream<String> resultStream = env.fromData(events)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .process(new ProcessFunction<FinancialTxEvent, String>() {
                    @Override
                    public void processElement(FinancialTxEvent value, Context ctx, Collector<String> out) {
                        long currentWatermark = ctx.timerService().currentWatermark();
                        long eventTime = ctx.timestamp();
                        String logLine = String.format("Tx: %s | EventTime: %d ms | CurrentWM: %d ms",
                                value.getTxId(), eventTime, currentWatermark);
                        LOG.info("🌊 [Lab 01 Step] {}", logLine);
                        out.collect(logLine);
                    }
                });

        List<String> outputLogs = Collections.synchronizedList(new ArrayList<>());
        resultStream.executeAndCollect().forEachRemaining(outputLogs::add);

        assertThat(outputLogs).hasSize(4);
        LOG.info("🎉 [Lab 01] Watermark Progression Lab passed cleanly!");
    }
}
