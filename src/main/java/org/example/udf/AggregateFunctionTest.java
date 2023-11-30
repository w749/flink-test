package org.example.udf;

import org.apache.flink.table.functions.AggregateFunction;
import org.example.entity.ACC;

/**
 * 聚合函数，输入一个或者多个字段，输出一个或者多个字段，内部使用累加器实现
 */
public class AggregateFunctionTest extends AggregateFunction<Double, ACC> {
    @Override
    public Double getValue(ACC accumulator) {
        if (accumulator.getCount() == 0L || accumulator.getFilled() == 0L) {
            return 0.0d;
        }
        return accumulator.getFilled() * 1.0d / accumulator.getCount();
    }

    @Override
    public ACC createAccumulator() {
        return new ACC();
    }

    public void accumulate(ACC acc, Long count, Long filled) {
        acc.setCount(acc.getCount() + count);
        acc.setFilled(acc.getFilled() + filled);
    }

    public void retract(ACC acc, Long count, Long filled) {
        acc.setCount(acc.getCount() - count);
        acc.setFilled(acc.getFilled() - filled);
    }

    public void merge(ACC acc, Iterable<ACC> it) {
        for (ACC a : it) {
            acc.setCount(acc.getCount() + a.getCount());
            acc.setFilled(acc.getFilled() + a.getFilled());
        }
    }

    public void resetAccumulator(ACC acc) {
        acc.setCount(0L);
        acc.setFilled(0L);
    }
}
