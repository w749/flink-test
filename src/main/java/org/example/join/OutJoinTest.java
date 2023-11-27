package org.example.join;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.base.Base;
import org.example.entity.Order;
import org.example.entity.Rate;
import org.example.entity.Result;

import java.util.Iterator;

public class OutJoinTest implements Base {
    @SneakyThrows
    public static void main(String[] args) {
        orderDataStream.coGroup(rateDataStream)
                        .where((KeySelector<Order, String>) Order::getItem)
                        .equalTo((KeySelector<Rate, String>) Rate::getItem)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .apply(new RichCoGroupFunction<Order, Rate, Result>() {

                            @Override
                            public void coGroup(Iterable<Order> first, Iterable<Rate> second, Collector<Result> out) throws Exception {
                                for (Order order : first) {
                                    if (!second.iterator().hasNext()) {
                                        out.collect(new Result(order.getOrderTime(), order.getPrice(), order.getItem()));
                                    }
                                    for (Rate rate : second) {
                                        out.collect(new Result(order.getOrderTime(), order.getPrice() * rate.getRate(), order.getItem()));
                                    }
                                }
                            }
                        }).print();

        environment.execute();
    }
}
