package com.cqz.flink.connectors.redis;

import com.cqz.flink.connectors.redis.common.config.RedisOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.cqz.flink.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

public class RedisDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "redis";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();

        if(context.getCatalogTable().getOptions().containsKey(REDIS_COMMAND)){
            context.getCatalogTable().getOptions().put(REDIS_COMMAND,
                    context.getCatalogTable().getOptions().get(REDIS_COMMAND).toUpperCase());
        }

        return new RedisDynamicTableSink(
                context.getCatalogTable().getOptions(),
                context.getCatalogTable().getSchema(),
                options);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.DATABASE);
        options.add(RedisOptions.HOST);
        options.add(RedisOptions.PORT);
        options.add(RedisOptions.MAXIDLE);
        options.add(RedisOptions.MAXTOTAL);
        options.add(RedisOptions.CLUSTERNODES);
        options.add(RedisOptions.PASSWORD);
        options.add(RedisOptions.TIMEOUT);
        options.add(RedisOptions.MINIDLE);
        options.add(RedisOptions.COMMAND);
        options.add(RedisOptions.REDISMODE);
        options.add(RedisOptions.XPUSH_KEY);
        options.add(RedisOptions.KEY_COLUMN);
        options.add(RedisOptions.VALUE_COLUMN);
        options.add(RedisOptions.FIELD_COLUMN);
        options.add(RedisOptions.PUT_IF_ABSENT);
        options.add(RedisOptions.KEY_TTL);
        options.add(RedisOptions.WRITE_MAX_ROWS);
        return options;
    }
}
