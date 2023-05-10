package com.cqz.component.flink.job.cep.dynamic;

import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class JDBCPeriodicPatternProcessorDiscovererFactory<T> extends PeriodicPatternProcessorDiscovererFactory<T> {
    private final String jdbcUrl;
    private final String jdbcDriver;
    private final String tableName;

    public JDBCPeriodicPatternProcessorDiscovererFactory(
            final String jdbcUrl,
            final String jdbcDriver,
            final String tableName,
            final List<PatternProcessor<T>> initialPatternProcessors,
            final Long intervalMillis) {
        super(initialPatternProcessors, intervalMillis);
        this.jdbcUrl = requireNonNull(jdbcUrl);
        this.jdbcDriver = requireNonNull(jdbcDriver);
        this.tableName = requireNonNull(tableName);
    }
    @Override
    public PatternProcessorDiscoverer<T> createPatternProcessorDiscoverer(ClassLoader userCodeClassLoader) throws Exception {
        return new JDBCPeriodicPatternProcessorDiscoverer<>(
                jdbcUrl,
                jdbcDriver,
                tableName,
                userCodeClassLoader,
                this.getInitialPatternProcessors(),
                getIntervalMillis());
    }

}
