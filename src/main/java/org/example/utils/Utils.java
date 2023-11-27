package org.example.utils;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.example.entity.Rate;
import org.example.extra.OrderSchema;
import org.example.entity.Order;
import org.example.extra.RateSchema;

import java.util.List;

public class Utils {
    public static KafkaSource<String> getKafkaSource(String bootstrapServers, List<String> topics, String groupID, Boolean isLatest) {
        OffsetsInitializer offsetsInitializer = isLatest ? OffsetsInitializer.latest() : OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(groupID)
                .setStartingOffsets(offsetsInitializer) // 从上次提交的位置开始消费，防止重复消费和丢失数据；如果该消费者组从未消费过那么从最开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "30000") // 每隔十秒监测是否有新分区，老版本kafka不支持
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "5000") // 防止作业断开时未checkpoint提交offset导致重启作业重复消费的情况，设置自动提交offset间隔
                .build();
    }

    public static KafkaSource<Order> getOrderKafkaSource(String bootstrapServers, List<String> topics, String groupID, Boolean isLatest) {
        OffsetsInitializer offsetsInitializer = isLatest ? OffsetsInitializer.latest() : OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
        return KafkaSource.<Order>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(groupID)
                .setStartingOffsets(offsetsInitializer) // 从上次提交的位置开始消费，防止重复消费和丢失数据；如果该消费者组从未消费过那么从最开始消费
                .setDeserializer(new OrderSchema())
                .setProperty("partition.discovery.interval.ms", "30000") // 每隔十秒监测是否有新分区，老版本kafka不支持
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "5000") // 防止作业断开时未checkpoint提交offset导致重启作业重复消费的情况，设置自动提交offset间隔
                .build();
    }

    public static KafkaSource<Rate> getRateKafkaSource(String bootstrapServers, List<String> topics, String groupID, Boolean isLatest) {
        OffsetsInitializer offsetsInitializer = isLatest ? OffsetsInitializer.latest() : OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
        return KafkaSource.<Rate>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(groupID)
                .setStartingOffsets(offsetsInitializer) // 从上次提交的位置开始消费，防止重复消费和丢失数据；如果该消费者组从未消费过那么从最开始消费
                .setDeserializer(new RateSchema())
                .setProperty("partition.discovery.interval.ms", "30000") // 每隔十秒监测是否有新分区，老版本kafka不支持
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "5000") // 防止作业断开时未checkpoint提交offset导致重启作业重复消费的情况，设置自动提交offset间隔
                .build();
    }

    public static KafkaSink<String> getKafkaSink(String bootstrapServers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static <T> String entityToJson(T entity) {
        return JSON.toJSONString(entity);
    }
}
