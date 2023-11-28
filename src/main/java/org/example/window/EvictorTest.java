package org.example.window;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.base.Base;
import org.example.entity.Order;
import org.example.utils.Utils;

import java.util.ArrayList;

public class EvictorTest implements Base {
    @SneakyThrows
    public static void main(String[] args) {
        WindowedStream<Order, String, TimeWindow> orderWindowDataStream = orderDataStream.keyBy(Order::getItem)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)));
        orderWindowDataStream.evictor(CountEvictor.of(2))
                .process(processWindowFunction("CountEvictor"));

        orderWindowDataStream.evictor(DeltaEvictor.of(5,
                        (DeltaFunction<Order>) (oldDataPoint, newDataPoint) -> oldDataPoint.getPrice() - newDataPoint.getPrice()))
                .process(processWindowFunction("DeltaEvictor"));

        orderWindowDataStream.evictor(TimeEvictor.of(Time.seconds(10)))
                        .process(processWindowFunction("TimeEvictor"));

        environment.execute();
    }

    public static ProcessWindowFunction<Order, Order, String, TimeWindow> processWindowFunction(String name) {
        return new ProcessWindowFunction<Order, Order, String, TimeWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Order, Order, String, TimeWindow>.Context context, Iterable<Order> elements, Collector<Order> out) throws Exception {
                ArrayList<String> orders = new ArrayList<>();
                long watermark = context.currentWatermark();
                long windowStart = context.window().getStart();
                long windowEnd = context.window().getEnd();
                elements.forEach(order -> orders.add(order.toString()));
                System.out.println(String.format("%s; Watermark: [%s]; Window: [%s -> %s]; data: %s",
                        name, Utils.timestampFormat(watermark), Utils.timestampFormat(windowStart), Utils.timestampFormat(windowEnd), orders));
            }
        };
    }
}
