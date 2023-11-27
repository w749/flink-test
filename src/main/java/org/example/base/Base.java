package org.example.base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.Utils;
import org.example.entity.Order;

import java.time.Duration;
import java.util.Collections;

public interface Base {
    // 基础环境
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>noWatermarks().withIdleness(Duration.ofMinutes(1));
    WatermarkStrategy<Order> orderWatermarkStrategy = WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner(((element, recordTimestamp) -> element.getOrderTime()));

    // source
    KafkaSource<Order> source1 = Utils.getOrderKafkaSource(Constant.KAFKA_SERVERS, Collections.singletonList("source1"), Constant.KAFKA_GROUP_ID, true);
    KafkaSource<String> source2 = Utils.getKafkaSource(Constant.KAFKA_SERVERS, Collections.singletonList("source2"), Constant.KAFKA_GROUP_ID, true);

    // sink
    KafkaSink<String> sink = Utils.getKafkaSink(Constant.KAFKA_SERVERS, "sink");

    // dataStream
    SingleOutputStreamOperator<Order> dataStream1 = environment.fromSource(source1, orderWatermarkStrategy, "Source1").returns(TypeInformation.of(Order.class));
}
