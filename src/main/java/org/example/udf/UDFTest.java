package org.example.udf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class UDFTest {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setString("sql-client.execution.result-mode", "tableau");
        configuration.setString("execution.runtime-mode", "batch");
        TableEnvironment environment = TableEnvironment.create(configuration);

        DataType dataType1 = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("words", DataTypes.STRING()));
        Table table1 = environment.fromValues(dataType1, Row.of(1, "hello,world"), Row.of(2, "hello,java"), Row.of(3, "hello,scala"));
        environment.createTemporaryView("test1", table1);
        DataType dataType2 = DataTypes.ROW(DataTypes.FIELD("item", DataTypes.STRING()), DataTypes.FIELD("is_filled", DataTypes.INT()));
        Table table2 = environment.fromValues(dataType2, Row.of("A", 1), Row.of("B", 1), Row.of("A", 1), Row.of("B", 0), Row.of("A", 0), Row.of("C", 0));
        environment.createTemporaryView("test2", table2);
        DataType dataType3 = DataTypes.ROW(DataTypes.FIELD("item", DataTypes.STRING()), DataTypes.FIELD("value", DataTypes.INT()));
        Table table3 = environment.fromValues(dataType3, Row.of("A", 3), Row.of("B", 5), Row.of("A", 1), Row.of("B", 0), Row.of("A", 10), Row.of("B", 7));
        environment.createTemporaryView("test3", table3);

        environment.createTemporarySystemFunction("date_format", ScalarFunctionTest.class);
        environment.createTemporarySystemFunction("split_func", new TableFunctionTest(","));
        environment.createTemporarySystemFunction("agg_func", AggregateFunctionTest.class);
        environment.createTemporarySystemFunction("table_agg_func", TableAggFunctionTest.class);

        environment.executeSql("SELECT date_format(1701311686969) AS format_long, date_format('1701311686969123') AS format_string").print();
        environment.executeSql("SELECT id, words, word, len FROM test1, LATERAL TABLE(split_func(words))").print();
        environment.executeSql("SELECT item, agg_func(1, is_filled) AS filled_rate FROM test2 GROUP BY item").print();
        environment.from("test3")
                .groupBy($("item"))
                .flatAggregate(call("table_agg_func", $("value")).as("value", "rk"))
                .select($("item"), $("value"), $("rk"))
                .execute()
                .print();
    }
}
