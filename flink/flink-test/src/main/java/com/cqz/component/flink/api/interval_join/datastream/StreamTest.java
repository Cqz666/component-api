package com.cqz.component.flink.api.interval_join.datastream;

import com.cqz.component.flink.api.interval_join.pojo.CurrencyType;
import com.cqz.component.flink.api.interval_join.pojo.ExchangeRateInfo;
import com.cqz.component.flink.api.interval_join.pojo.OrderInfo;
import com.cqz.component.flink.api.interval_join.source.ExchangeRateDataSource;
import com.cqz.component.flink.api.interval_join.source.OrderDataSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Interval Join - 无窗口关联，只关联左元素between范围内的右元素
 * 每秒产生一条汇率数据，每10s产生一条订单数据，实时转换订单汇率
 */
public class StreamTest {
    private static final Logger LOG = LoggerFactory.getLogger(StreamTest.class);
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //CNY -> USD 汇率流
        SingleOutputStreamOperator<ExchangeRateInfo> usdToCny = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.USD, 7, 6),"USD-CNY")
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ExchangeRateInfo>(Time.milliseconds(100)) {
                    @Override
                    public long extractTimestamp(ExchangeRateInfo element) {
                        return element.getTimeStamp().getTime();
                    }
                });
        //订单流
        SingleOutputStreamOperator<OrderInfo> orderDs = env.addSource(new OrderDataSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderInfo>(Time.milliseconds(100)) {
                    @Override
                    public long extractTimestamp(OrderInfo element) {
                        return element.getTimeStamp().getTime();
                    }
                });


        KeyedStream<ExchangeRateInfo, CurrencyType> usdToCnyKeyedStream = usdToCny.keyBy((KeySelector<ExchangeRateInfo, CurrencyType>) (ExchangeRateInfo value) -> {return value.getFrom();});
        KeyedStream<OrderInfo, CurrencyType> orderDsKeyedStream = orderDs.keyBy((KeySelector<OrderInfo, CurrencyType>) (OrderInfo order) -> {return order.getCurrencyType();});

        //订单流inner join 汇率流
        usdToCnyKeyedStream.intervalJoin(orderDsKeyedStream)
                .between(Time.milliseconds(-500), Time.milliseconds(500))
                .upperBoundExclusive()
                .lowerBoundExclusive()
                .process(new ProcessJoinFunction<ExchangeRateInfo, OrderInfo, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void processElement(ExchangeRateInfo arg0, OrderInfo arg1,
                                               ProcessJoinFunction<ExchangeRateInfo, OrderInfo, String>.Context arg2, Collector<String> arg3)
                            throws Exception {
                        arg3.collect(arg1.getTotalAmt().divide(arg0.getCoefficient(),2, BigDecimal.ROUND_HALF_UP).toPlainString());
                    }

                })
                .print();



        env.execute("Flink Streaming Java API Skeleton");
    }
}
