package org.example.join;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.base.Base;
import org.example.entity.Order;
import org.example.entity.Rate;
import org.example.entity.Result;

public class JoinTest implements Base {
    @SneakyThrows
    public static void main(String[] args) {
        orderDataStream.join(rateDataStream)
                        .where((KeySelector<Order, String>) Order::getItem)
                        .equalTo((KeySelector<Rate, String>) Rate::getItem)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .apply((JoinFunction<Order, Rate, Result>) (first, second) ->
                                new Result(first.getOrderTime(), first.getPrice() * second.getRate(), first.getItem())).print();

        environment.execute();
    }
}
