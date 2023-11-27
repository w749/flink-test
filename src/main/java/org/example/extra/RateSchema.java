package org.example.extra;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.entity.Order;
import org.example.entity.Rate;

import java.io.IOException;

public class RateSchema implements KafkaRecordDeserializationSchema<Rate> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Rate> out) throws IOException {
        String json = new String(record.value(), "UTF-8");
        Rate rate = JSON.parseObject(json, Rate.class);
        if (rate != null) {
            out.collect(rate);
        }
    }

    @Override
    public TypeInformation<Rate> getProducedType() {
        return null;
    }
}
