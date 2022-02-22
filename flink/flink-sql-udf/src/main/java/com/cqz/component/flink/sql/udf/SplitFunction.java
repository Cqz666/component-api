package com.cqz.component.flink.sql.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;

/**
 * Table Function 表值函数   UDTF
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class SplitFunction  extends TableFunction<Tuple2<String,Integer>> {

    private String separator = " ";

    public SplitFunction(String separator) {
        this.separator = separator;
    }

    public SplitFunction() {
    }

    public void eval(String str){
        for (String s : str.split(separator)) {
            collect(new Tuple2<>(s,s.length()));
        }
    }

}
