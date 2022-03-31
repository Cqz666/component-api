CREATE TABLE source_table (
    name BIGINT NOT NULL,
    search_cnt BIGINT NOT NULL,
    key BIGINT NOT NULL,
    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),
    WATERMARK FOR row_time AS row_time
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '10',
  'fields.name.min' = '1',
  'fields.name.max' = '10',
  'fields.key.min' = '1',
  'fields.key.max' = '2',
  'fields.search_cnt.min' = '1000',
  'fields.search_cnt.max' = '10000'
);

CREATE TABLE sink_table (
    key BIGINT,
    name BIGINT,
    search_cnt BIGINT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3)
) WITH (
  'connector' = 'print'
);

INSERT INTO sink_table
SELECT key, name, search_cnt, window_start, window_end
FROM (
   SELECT key, name, search_cnt, window_start, window_end,
     ROW_NUMBER() OVER (PARTITION BY window_start, window_end, key
       ORDER BY search_cnt desc) AS rownum
   FROM (
      SELECT window_start, window_end, key, name, max(search_cnt) as search_cnt
      FROM TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '1' MINUTES))
      GROUP BY window_start, window_end, key, name
   )
)
WHERE rownum <= 100