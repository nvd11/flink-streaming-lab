package com.finance.lab.integration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Integration: 真实远端 Kafka Broker 端到端联调测试 (kafka.jppwl.asia:9092)")
class KafkaRemoteBrokerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRemoteBrokerIntegrationTest.class);

    private static final String BOOTSTRAP_SERVERS = "kafka.jppwl.asia:9092";
    private static final String TOPIC_NAME = "sms-tx-events";

    // SASL PLAIN 认证凭据 (安全加固: admin / hsbc1234)
    private static final String SASL_JAAS_CONFIG =
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"hsbc1234\";";

    private Properties createBaseSecurityProps() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", SASL_JAAS_CONFIG);
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        return props;
    }

    @Test
    @DisplayName("测试通过域名经 Kong L4 透传连接手机 Kafka: SASL 认证、Topic 元数据探测、生产消息与消费确认")
    void testRemoteKafkaProduceAndConsume() throws Exception {
        LOG.info("开始连接远端 Kafka Broker (SASL_PLAINTEXT): {}", BOOTSTRAP_SERVERS);

        // 1. AdminClient 元数据探针 (带 SASL 账密认证)
        Properties adminProps = createBaseSecurityProps();

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> topicNames = listTopics.names().get(15, TimeUnit.SECONDS);
            LOG.info("[Metadata Probe] 远端 Broker 主题列表: {}", topicNames);
            assertThat(topicNames).contains(TOPIC_NAME);

            Map<String, TopicDescription> descMap = adminClient.describeTopics(Collections.singletonList(TOPIC_NAME))
                    .allTopicNames().get(15, TimeUnit.SECONDS);
            TopicDescription desc = descMap.get(TOPIC_NAME);
            LOG.info("[Metadata Probe] 主题 {} 分区数: {}", TOPIC_NAME, desc.partitions().size());
            assertThat(desc.partitions()).hasSize(2);
        }

        // 2. Producer 消息写入 (带 SASL 账密认证)
        Properties prodProps = createBaseSecurityProps();
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.ACKS_CONFIG, "all");

        String testKey = "CARD-7788";
        String testValue = "{\"txId\":\"TX-REMOTE-SASL-001\",\"cardTail\":\"7788\",\"amount\":88.88,\"merchant\":\"星巴克臻选\"}";

        long producedOffset;
        int producedPartition;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, testKey, testValue);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata meta = future.get(15, TimeUnit.SECONDS);
            producedOffset = meta.offset();
            producedPartition = meta.partition();
            LOG.info("[Producer Success] 写入成功 -> Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                    meta.topic(), producedPartition, producedOffset, meta.timestamp());
            assertThat(meta.offset()).isGreaterThanOrEqualTo(0);
        }

        // 3. Consumer 消息精确读取 (带 SASL 账密认证)
        Properties consProps = createBaseSecurityProps();
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-lab-verifier-" + UUID.randomUUID());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
            TopicPartition tp = new TopicPartition(TOPIC_NAME, producedPartition);
            consumer.assign(Collections.singletonList(tp));
            consumer.seek(tp, producedOffset);

            boolean found = false;
            long deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline && !found) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    if (r.offset() == producedOffset) {
                        LOG.info("[Consumer Success] 成功读回消息 -> Partition: {}, Offset: {}, Key: {}, Value: {}",
                                r.partition(), r.offset(), r.key(), r.value());
                        assertThat(r.key()).isEqualTo(testKey);
                        assertThat(r.value()).isEqualTo(testValue);
                        found = true;
                        break;
                    }
                }
            }
            assertThat(found).as("必须在 15 秒内消费到刚才由 Producer 写入的精确 Offset 消息").isTrue();
        }

        LOG.info("[Integration Test] 远端 Kafka (kafka.jppwl.asia:9092) 端到端生产与消费验证完全成功!");
    }
}
