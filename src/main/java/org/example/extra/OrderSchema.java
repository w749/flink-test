package org.example.extra;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.entity.Order;

import java.io.IOException;

public class OrderSchema implements KafkaRecordDeserializationSchema<Order> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Order> out) throws IOException {
        String json = new String(record.value(), "UTF-8");
        Order order = JSON.parseObject(json, Order.class);
        if (order != null) {
            out.collect(order);
        }
    }

    @Override
    public TypeInformation<Order> getProducedType() {
        return null;
    }
}
