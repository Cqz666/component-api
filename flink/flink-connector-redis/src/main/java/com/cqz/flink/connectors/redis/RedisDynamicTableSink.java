package com.cqz.flink.connectors.redis;

import com.cqz.flink.connectors.redis.common.config.FlinkJedisConfigBase;
import com.cqz.flink.connectors.redis.common.handler.FlinkJedisConfigHandler;
import com.cqz.flink.connectors.redis.common.handler.RedisHandlerServices;
import com.cqz.flink.connectors.redis.common.handler.RedisMapperHandler;
import com.cqz.flink.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

public class RedisDynamicTableSink implements DynamicTableSink {

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisMapper redisMapper;
    private Map<String, String> properties ;
    private TableSchema tableSchema;
    private ReadableConfig config;

    public RedisDynamicTableSink(Map<String, String> properties,  TableSchema tableSchema, ReadableConfig config) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        this.tableSchema = tableSchema;
        Preconditions.checkNotNull(tableSchema, "tableSchema should not be null");
        this.config = config;

        redisMapper = RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, properties)
                .createRedisMapper(config);
        flinkJedisConfigBase = RedisHandlerServices
                .findRedisHandler(FlinkJedisConfigHandler.class, properties).createFlinkJedisConfig(config);
    }


    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
//        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(new RedisSink(flinkJedisConfigBase, redisMapper, tableSchema));
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(properties, tableSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "redis";
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(ChangelogMode.insertOnly().equals(requestedMode),
                "please declare primary key for sink table when query contains update/delete record.");
    }


}
