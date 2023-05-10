package com.cqz.component.flink.job.cep.dynamic;

import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.cep.dynamic.processor.PatternProcessorManager;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public abstract class PeriodicPatternProcessorDiscoverer<T> implements PatternProcessorDiscoverer<T> {
    private final Long intervalMillis;
    private final Timer timer;

    public PeriodicPatternProcessorDiscoverer(final Long intervalMillis) {
        this.intervalMillis = intervalMillis;
        this.timer = new Timer();
    }

    /**
     * Returns whether there are updated pattern processors.
     */
    public abstract boolean arePatternProcessorsUpdated();

    /**
     *  Returns the latest pattern processors.
     */
    public abstract List<PatternProcessor<T>> getLatestPatternProcessors() throws Exception;

    @Override
    public void discoverPatternProcessorUpdates(PatternProcessorManager<T> patternProcessorManager) {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (arePatternProcessorsUpdated()){
                    List<PatternProcessor<T>> patternProcessors = null;
                    try {
                        patternProcessors = getLatestPatternProcessors();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    patternProcessorManager.onPatternProcessorsUpdated(patternProcessors);
                }
            }
        },0, intervalMillis);
    }

    @Override
    public void close() throws IOException {
        timer.cancel();
    }

}
