package com.cqz.component.flink.api.interval_join.source;

import com.cqz.component.flink.api.interval_join.pojo.CurrencyType;
import com.cqz.component.flink.api.interval_join.pojo.ExchangeRateInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 随机产生汇率数据
 */
public class ExchangeRateDataSource implements SourceFunction<ExchangeRateInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(ExchangeRateDataSource.class);
    private static final long serialVersionUID = 4836546999687545904L;
    private volatile boolean isRunning = true;
    private CurrencyType from;
    private CurrencyType to;
    private int max = 0;
    private int min = 0;

    public ExchangeRateDataSource(CurrencyType from, CurrencyType to, int max, int min) {
        this.from = from;
        this.to = to;
        this.max = max;
        this.min = min;
    }

    @Override
    public void run(SourceContext<ExchangeRateInfo> ctx) throws Exception {
        while (isRunning) {
            TimeUnit.SECONDS.sleep(1);

            ExchangeRateInfo exchangeRateInfo = new ExchangeRateInfo(from, to,
                    new BigDecimal(min + ((max - min) * new Random().nextFloat())).setScale(2, BigDecimal.ROUND_HALF_UP),new Date());
            LOG.info(exchangeRateInfo.toString());
            ctx.collect(exchangeRateInfo);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
