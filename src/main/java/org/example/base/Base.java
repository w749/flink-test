package org.example.base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.entity.Rate;
import org.example.utils.Utils;
import org.example.entity.Order;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;

public interface Base {
    // 基础环境
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    WatermarkStrategy<Order> orderWatermarkStrategy = WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner(((element, recordTimestamp) -> element.getOrderTime()));
    WatermarkStrategy<Rate> rateWatermarkStrategy = WatermarkStrategy.<Rate>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner(((element, recordTimestamp) -> element.getRateTime()));

    // source
    KafkaSource<Order> source1 = Utils.getOrderKafkaSource(Constant.KAFKA_SERVERS, Collections.singletonList("source1"), Constant.KAFKA_GROUP_ID, true);
    KafkaSource<Rate> source2 = Utils.getRateKafkaSource(Constant.KAFKA_SERVERS, Collections.singletonList("source2"), Constant.KAFKA_GROUP_ID, true);

    // sink
    KafkaSink<String> sink = Utils.getKafkaSink(Constant.KAFKA_SERVERS, "sink");

    // dataStream
    SingleOutputStreamOperator<Order> orderDataStream = environment.fromSource(source1, orderWatermarkStrategy, "OrderDataStream").returns(TypeInformation.of(Order.class));
    SingleOutputStreamOperator<Rate> rateDataStream = environment.fromSource(source2, rateWatermarkStrategy, "RateDataStream").returns(TypeInformation.of(Rate.class));
}
