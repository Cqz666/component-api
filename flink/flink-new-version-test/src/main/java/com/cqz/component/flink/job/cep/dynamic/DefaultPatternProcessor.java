package com.cqz.component.flink.job.cep.dynamic;

import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkNotNull;

public class DefaultPatternProcessor<T> implements PatternProcessor<T> {
    private static final long serialVersionUID = 2606894127769376864L;
    private final String id;
    private final Integer version;
    private final String patternStr;

    private final PatternProcessFunction<T, ?> patternProcessFunction;
    public DefaultPatternProcessor(
            final String id,
            final Integer version,
            final String pattern,
            final PatternProcessFunction<T, ?> patternProcessFunction,
            final ClassLoader userCodeClassLoader) {
        this.id = checkNotNull(id);
        this.version = checkNotNull(version);
        this.patternStr = checkNotNull(pattern);
        this.patternProcessFunction = patternProcessFunction;
    }
    @Override
    public String getId() {
        return id;
    }

    @Override
    public Pattern<T, ?> getPattern(ClassLoader classLoader) {
        try {
            return (Pattern<T, ?>) CepJsonUtils.convertJSONStringToPattern(patternStr, classLoader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PatternProcessFunction<T, ?> getPatternProcessFunction() {
        return new DemoPatternProcessFunction<>();
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "DefaultPatternProcessor{"
                + "id='"
                + id
                + '\''
                + ", version="
                + version
                + ", pattern="
                + patternStr
                + ", patternProcessFunction="
                + patternProcessFunction
                + '}';
    }
}
