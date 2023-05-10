package com.cqz.component.flink.job.cep.dynamic;

import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscovererFactory;

import java.util.List;

public abstract class PeriodicPatternProcessorDiscovererFactory<T>
        implements PatternProcessorDiscovererFactory<T> {

    private final List<PatternProcessor<T>> initialPatternProcessors;
    private final Long intervalMillis;

    public PeriodicPatternProcessorDiscovererFactory(
             final List<PatternProcessor<T>> initialPatternProcessors,
             final Long intervalMillis) {
        this.initialPatternProcessors = initialPatternProcessors;
        this.intervalMillis = intervalMillis;
    }

    public List<PatternProcessor<T>> getInitialPatternProcessors() {
        return initialPatternProcessors;
    }

    @Override
    public abstract PatternProcessorDiscoverer<T> createPatternProcessorDiscoverer(
            ClassLoader userCodeClassLoader) throws Exception;

    public Long getIntervalMillis() {
        return intervalMillis;
    }

}
