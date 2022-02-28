-- 数据源表
CREATE TABLE source_table (
    -- 维度数据
    dim STRING,
    -- 用户 id
    user_id BIGINT,
    -- 用户
    price BIGINT,
    -- 事件时间戳
    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),
    -- watermark 设置
    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '10',
  'fields.dim.length' = '1',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '100000',
  'fields.price.min' = '1',
  'fields.price.max' = '100000'
)

-- 数据汇表
CREATE TABLE sink_table (
    dim STRING,
    pv BIGINT,
    sum_price BIGINT,
    max_price BIGINT,
    min_price BIGINT,
    uv BIGINT,
    window_start bigint
) WITH (
  'connector' = 'print'
)

-- 数据处理逻辑
insert into sink_table
SELECT
    dim,
    UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 as window_start,
    count(*) as pv,
    sum(price) as sum_price,
    max(price) as max_price,
    min(price) as min_price,
    count(distinct user_id) as uv
FROM TABLE(TUMBLE(
        TABLE source_table
        , DESCRIPTOR(row_time)
        , INTERVAL '60' SECOND))
GROUP BY window_start,
      window_end,
      dim