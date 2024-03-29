package com.cqz.component.flink.job.cep.dynamic;

import org.apache.flink.cep.dynamic.operator.DynamicCepOperator;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class DemoPatternProcessFunction<IN> extends PatternProcessFunction<IN, String> {
    @Override
    public void processMatch(Map<String, List<IN>> match, Context ctx, Collector<String> out) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("A match for Pattern of (id, version): (")
                .append(((DynamicCepOperator.ContextFunctionImpl) ctx).patternProcessor().getId())
                .append(", ")
                .append(
                        ((DynamicCepOperator.ContextFunctionImpl) ctx)
                                .patternProcessor()
                                .getVersion())
                .append(") is found. The event sequence: ");
        for (Map.Entry<String, List<IN>> entry : match.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue());
        }
        out.collect(sb.toString());
    }


}
