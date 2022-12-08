package com.cqz.component.flink.arthas;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public class WatchStateFunc extends RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>> {
    private ValueState<Long> valueState;
    private ListState<Long> listState;
    private MapState<String,Long> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor descriptor = new ValueStateDescriptor("valueState", Long.class);
        valueState = getRuntimeContext().getState(descriptor);

        ListStateDescriptor listStateDescriptor = new ListStateDescriptor("listState", Long.class);
        listState = getRuntimeContext().getListState(listStateDescriptor);

        MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("mapState", String.class, Long.class);
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);

    }

    @Override
    public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {
        System.out.println("-----------------------------");
        Long historyMaxValue = valueState.value();
        Long currentValue = value.f1;
        listState.add(value.f1);
        if (historyMaxValue == null || currentValue > historyMaxValue) {
            valueState.update(currentValue);
            mapState.put(value.f0, currentValue);
            return Tuple3.of(value.f0, currentValue, currentValue);
        } else {
            return Tuple3.of(value.f0, currentValue, historyMaxValue);
        }
    }

}
