package com.cqz.component.flink.job.cep.condition;

import com.cqz.component.flink.job.cep.event.Event;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

public class MiddleCondition extends SimpleCondition<Event> {
    @Override
    public boolean filter(Event event) throws Exception {
        return event.getName().contains("middle");
    }
}
