package org.example.join;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.base.Base;
import org.example.entity.Order;
import org.example.entity.Rate;
import org.example.entity.Result;
import org.example.utils.Utils;

/**
 * 时间间隔连接，匹配上下界时间范围内的数据，匹配不到不输出，无法做到外连接。可将迟到的数据输出到侧输出流
 */
public class IntervalJoinTest implements Base {
    @SneakyThrows
    public static void main(String[] args) {
        OutputTag<Order> orderOutputTag = new OutputTag<>("OrderOutputTag", TypeInformation.of(Order.class));
        OutputTag<Rate> rateOutputTag = new OutputTag<>("RateOutputTag", TypeInformation.of(Rate.class));
        SingleOutputStreamOperator<Result> intervalJoinDataStream = orderDataStream.keyBy(Order::getItem)
                .intervalJoin(rateDataStream.keyBy(Rate::getItem))
                .between(Time.seconds(-10), Time.seconds(10))
                .sideOutputLeftLateData(orderOutputTag)
                .sideOutputRightLateData(rateOutputTag)
                .process(new ProcessJoinFunction<Order, Rate, Result>() {
                    @Override
                    public void processElement(Order left, Rate right, ProcessJoinFunction<Order, Rate, Result>.Context ctx, Collector<Result> out) throws Exception {
                        long orderTime = left.getOrderTime();
                        int price = left.getPrice();
                        String item = left.getItem();
                        float rate = right.getRate();
                        out.collect(new Result(orderTime, price * rate, item));
                    }
                });
        intervalJoinDataStream.map(Utils::entityToJson).print();
        intervalJoinDataStream.getSideOutput(orderOutputTag).map(order -> "Order late: " + Utils.entityToJson(order)).print();
        intervalJoinDataStream.getSideOutput(rateOutputTag).map(rate -> "Rate late: " + Utils.entityToJson(rate)).print();

        environment.execute();
    }
}
