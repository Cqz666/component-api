package com.cqz.component.flink.job.cep.condition;

import com.cqz.component.flink.job.cep.event.Event;
import org.apache.flink.cep.dynamic.condition.AviatorCondition;

public class StartCondition extends AviatorCondition<Event> {
    public StartCondition(String expression) {
        super(expression);
    }
}
