CREATE TABLE source_table (
    age STRING,
    sex STRING,
    user_id BIGINT,
    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),
    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.age.length' = '1',
  'fields.sex.length' = '1',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '100000'
);

CREATE TABLE sink_table (
    age STRING,
    sex STRING,
    uv BIGINT,
    window_end bigint
) WITH (
  'connector' = 'print'
);

insert into sink_table
SELECT
    UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end,
    if (age is null, 'ALL', age) as age,
    if (sex is null, 'ALL', sex) as sex,
    count(distinct user_id) as bucket_uv
FROM TABLE(CUMULATE(
       TABLE source_table
       , DESCRIPTOR(row_time)
       , INTERVAL '5' SECOND
       , INTERVAL '1' DAY))
GROUP BY
    window_start,
    window_end,
    GROUPING SETS (
        ()
        , (age)
        , (sex)
        , (age, sex)
    )