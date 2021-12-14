package com.cqz.flink.connectors.redis.common.mapper.row;

import com.alibaba.fastjson.JSONObject;
import com.cqz.flink.connectors.redis.common.config.RedisOptions;
import com.cqz.flink.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class XPushMapper extends RowRedisMapper{

    public static final String OP = "op";

    private String key;
    private Long writeMaxRows;

    private transient int writeCount;

    public XPushMapper() {
        super(RedisCommand.XPUSH);
    }

    public XPushMapper(ReadableConfig config) {
        super(RedisCommand.XPUSH, config);

        key = config.get(RedisOptions.XPUSH_KEY);
        Objects.requireNonNull(key,"Redis xpush-key can not be null ");

        writeMaxRows = config.get(RedisOptions.WRITE_MAX_ROWS);

    }

    @Override
    public String getKeyFromData(GenericRowData row, Integer keyIndex) {
        return key;
    }

    @Override
    public String getValueFromData(List<String> fields, GenericRowData row) {
        if (writeMaxRows == null || writeCount < writeMaxRows){
            writeCount++;
            String op  =row.getRowKind().shortString();
            Map<String,String> map = new LinkedHashMap<>();
            for (int i = 0; i < fields.size(); i++){
                if (i==0) map.put(OP , op);
                map.put(fields.get(i),String.valueOf(row.getField(i)));
            }
            return JSONObject.toJSONString(map);
        }else {
            return null;
        }

    }

}
