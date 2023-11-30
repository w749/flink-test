package org.example.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 表函数，输入一个字段或者多个字段，需要配合 LATERAL TABLE使用，返回多个字段并且返回一行或者多行
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, len INT>"))
public class TableFunctionTest extends TableFunction<Row> {
    private final String separator;
    public TableFunctionTest() {
        this.separator = " ";
    }
    public TableFunctionTest(String separator) {
        this.separator = separator;
    }
    public void eval(String str) {
        for (String s : StringUtils.splitPreserveAllTokens(str, separator)) {
            collect(Row.of(s, s.length()));
        }
    }
}
