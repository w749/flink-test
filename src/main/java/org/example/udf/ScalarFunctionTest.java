package org.example.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 标量函数，传入一个或多个字段返回一个或多个字段，只返回一行
 */
public class ScalarFunctionTest extends ScalarFunction {
    private final SimpleDateFormat dateFormat;
    public ScalarFunctionTest() {
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    public ScalarFunctionTest(String format) {
        dateFormat = new SimpleDateFormat(format);
    }

    public String eval(Long timestamp) {
        return dateFormat.format(new Date(timestamp));
    }

    public String eval(String timestamp) {
        return dateFormat.format(new Date(Long.parseLong(timestamp.substring(0, 13))));
    }
}
