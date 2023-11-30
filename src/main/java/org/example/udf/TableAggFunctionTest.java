package org.example.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.example.entity.Rank;

@FunctionHint(output = @DataTypeHint("ROW<value INT, rk INT>"))
public class TableAggFunctionTest extends TableAggregateFunction<Row, Rank> {
    @Override
    public Rank createAccumulator() {
        return new Rank();
    }

    public void accumulate(Rank rank, Integer value) {
        if (value > rank.getFirst()) {
            rank.setSecond(rank.getFirst());
            rank.setFirst(value);
        } else if (value > rank.getSecond()) {
            rank.setSecond(value);
        }
    }

    public void merge(Rank rank, Iterable<Rank> it) {
        for (Rank rk : it) {
            accumulate(rank, rk.getFirst());
            accumulate(rank, rk.getSecond());
        }
    }

    public void emitValue(Rank rank, Collector<Row> out) {
        if (rank.getFirst() != Integer.MIN_VALUE) {
            out.collect(Row.of(rank.getFirst(), 1));
        }
        if (rank.getSecond() != Integer.MIN_VALUE) {
            out.collect(Row.of(rank.getSecond(), 2));
        }
    }
}
