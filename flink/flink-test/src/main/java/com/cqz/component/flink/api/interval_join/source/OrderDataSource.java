package com.cqz.component.flink.api.interval_join.source;

import com.cqz.component.flink.api.interval_join.pojo.CurrencyType;
import com.cqz.component.flink.api.interval_join.pojo.Goods;
import com.cqz.component.flink.api.interval_join.pojo.GoodsType;
import com.cqz.component.flink.api.interval_join.pojo.OrderInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class OrderDataSource implements SourceFunction<OrderInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderDataSource.class);
    private static final long serialVersionUID = -218080338675267439L;
    private volatile boolean isRunning = true;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private final Random random = new Random();

    @Override
    public void run(SourceContext<OrderInfo> ctx) throws Exception {
        OrderInfo order = null;
        while (isRunning) {
            TimeUnit.SECONDS.sleep(10);
            String orderNo = sdf.format(new Date()) + String.format("%03d", Integer.valueOf(random.nextInt(1000)));
            order = new OrderInfo();
            order.setOrderNo(orderNo);
            order.setTimeStamp(new Date());

            order = getRandomGoods(order);
            LOG.info(order.toString());
            ctx.collect(order);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private OrderInfo getRandomGoods(OrderInfo order) {
        List<Goods> goodsList = new ArrayList<>();
        GoodsType[] goodsTypes = GoodsType.values();
        BigDecimal totalAmt = BigDecimal.ZERO;

        for (int i = 0, size = random.nextInt(10); i < size; i++) {
            BigDecimal unitPrice = new BigDecimal(random.nextDouble()*100).setScale(2, BigDecimal.ROUND_HALF_UP);
            int num = random.nextInt(20);
            goodsList.add(new Goods(goodsTypes[random.nextInt(goodsTypes.length)], unitPrice, CurrencyType.CNY, num));
            totalAmt = totalAmt.add(unitPrice.multiply(BigDecimal.valueOf(num)));
        }
        order.setGoods(goodsList);
        order.setTotalAmt(totalAmt);
        order.setCurrencyType(CurrencyType.CNY);
        return order;
    }
}
