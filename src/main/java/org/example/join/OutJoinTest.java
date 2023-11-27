package org.example.join;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

/**
 * 外连接，实现若当前窗口没有rate时使用之前窗口存储的状态，类似于广播状态
 */
public class OutJoinTest implements Base {
    @SneakyThrows
    public static void main(String[] args) {
        orderDataStream.coGroup(rateDataStream)
                        .where((KeySelector<Order, String>) Order::getItem)
                        .equalTo((KeySelector<Rate, String>) Rate::getItem)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .apply(new RichCoGroupFunction<Order, Rate, Result>() {
                            private MapState<String, Float> itemRateState;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                itemRateState = getRuntimeContext().getMapState(
                                        new MapStateDescriptor<>("ItemRateState", TypeInformation.of(String.class), TypeInformation.of(Float.class)));
                            }
                            @Override
                            public void coGroup(Iterable<Order> first, Iterable<Rate> second, Collector<Result> out) throws Exception {
                                for (Order order : first) {
                                    long orderTime = order.getOrderTime();
                                    int orderPrice = order.getPrice();
                                    String item = order.getItem();
                                    if (!second.iterator().hasNext()) {
                                        float rate = itemRateState.contains(item) ? itemRateState.get(item) : 1.0f;
                                        out.collect(new Result(orderTime, orderPrice * rate, item));
                                    }
                                    for (Rate rate : second) {
                                        itemRateState.put(rate.getItem(), rate.getRate());
                                        out.collect(new Result(orderTime, orderPrice * rate.getRate(), item));
                                    }
                                }
                            }
                        }).print();

        environment.execute();
    }
}
