CREATE TABLE gamebox_event_clicks(
  `$client_ip` varchar,
  `$model` varchar,
  `$network_type` varchar,
  `event` varchar,
  `receive_timestamp` BIGINT,
  `client_timestamp` BIGINT,
  `uid` varchar,
  `vid` varchar,
  `game_id` bigint,
   proctime as PROCTIME(),
   event_time AS TO_TIMESTAMP(FROM_UNIXTIME(if(client_timestamp <> null, client_timestamp, UNIX_TIMESTAMP()),'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),
   WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'gamebox_event',
  'properties.bootstrap.servers' = '10.21.0.131:9092,10.21.0.132:9092,10.20.0.2:9092,10.20.0.3:9092',
  'properties.group.id' = 'flinksql-clicks-topn',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);


create table dim_game(
  `id` bigint,
  `appname` varchar
)with(
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://10.0.0.246:3307/4399_mobi_services',
  'username'='gprp_gqsj',
  'password'='QzSJsla43a',
  'table-name'='mobi_game_base',
  'driver' = 'com.mysql.jdbc.Driver',
  'scan.fetch-size' = '200',
  'lookup.cache.max-rows' = '2000',
  'lookup.cache.ttl' = '300s'
);

create table result_print(
  `rk` bigint,
  `w_end` TIMESTAMP,
  `appname` varchar,
  `cnt` bigint,
  PRIMARY KEY(rk,w_end)NOT ENFORCED
)with(
  'connector'='print'
);

create view result_view as
	select rk,w_end,appname,cnt from (
		select *,row_number() over(partition by w_end order by cnt desc) as rk
		from(
			select  appname,
				hop_end(event_time,interval '10' minute,interval '1' hour) w_end,
				count(appname) as cnt
			from(
				select	a.event_time,
					b.appname
				from gamebox_event_clicks as a left join dim_game for system_time as of a.proctime as b
				on a.game_id=b.id where a.event='click_game' and a.game_id is not null and a.game_id <> '' and a.game_id <> '-1'
			) group by hop(event_time,interval '10' minute,interval '1' hour),appname
		)
	) where rk<=5;

insert into result_print select * from result_view;
