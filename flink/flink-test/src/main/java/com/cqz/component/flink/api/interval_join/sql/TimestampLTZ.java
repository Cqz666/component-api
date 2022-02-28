package com.cqz.component.flink.api.interval_join.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TimestampLTZ {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        String orders = "CREATE TABLE Orders(\n" +
//                "  id BIGINT,\n" +
//                "  productName VARCHAR,\n" +
//                "  orderTime TIMESTAMP(3),\n" +
//                "  WATERMARK  FOR orderTime as  orderTime - INTERVAL '5' SECOND \n" +
//                ") WITH (\n" +
//                "  'connector'='filesystem',\n" +
//                "  'path'='/home/cqz/IdeaProjects/component-api/flink/flink-test/src/main/resources/order.txt',\n" +
//                "  'format'='csv'\n" +
//                ")\n";
        String orders = "CREATE TABLE Orders(\n" +
                "  id BIGINT,\n" +
                "  productName VARCHAR,\n" +
                "  orderTime BIGINT,\n" +
                "  ts AS TO_TIMESTAMP_LTZ(orderTime, 3),\n" +
                "   WATERMARK  FOR ts as  ts - INTERVAL '5' SECOND \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'order',\n" +
                "  'properties.bootstrap.servers' = '172.21.0.3:9092',\n" +
                "  'properties.group.id' = 'flink-sql-client-test',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv',\n" +
                "'csv.ignore-parse-errors'='true'\n"+
                ")";
//        String ships = "CREATE TABLE Shipments(\n" +
//                "  shipId BIGINT,\n" +
//                "  orderId BIGINT,\n" +
//                "  status VARCHAR,\n" +
//                "  shiptime TIMESTAMP(3),\n" +
//                "  WATERMARK  FOR shiptime as shiptime - INTERVAL '5' SECOND \n" +
//                ") WITH (\n" +
//                "  'connector'='filesystem',\n" +
//                "  'path'='/home/cqz/IdeaProjects/component-api/flink/flink-test/src/main/resources/ship.txt',\n" +
//                "  'format'='csv'\n" +
//                ")";
        String ships = "CREATE TABLE Shipments(\n" +
                "  shipId BIGINT,\n" +
                "  orderId BIGINT,\n" +
                "  status VARCHAR,\n" +
                "  shiptime BIGINT,\n" +
                "  ts AS TO_TIMESTAMP_LTZ(shiptime, 3),\n" +
                "   WATERMARK  FOR ts as  ts - INTERVAL '5' SECOND \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'ship',\n" +
                "  'properties.bootstrap.servers' = '172.21.0.3:9092',\n" +
                "  'properties.group.id' = 'flink-sql-client-test',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv',\n" +
                "'csv.ignore-parse-errors'='true'\n"+
                ")";
        String result = "CREATE TABLE rds_output(\n" +
                "  id BIGINT,\n" +
                "  productName VARCHAR,\n" +
                "  status VARCHAR,\n" +
                "  ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector'='print'\n" +
                "\n" +
                ")";

        String sql  = "INSERT INTO rds_output\n" +
                "SELECT id, productName, status,o.ts\n" +
                "FROM Orders AS o\n" +
                "LEFT JOIN Shipments AS s on o.id = s.orderId AND\n" +
//                "o.orderTime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime";
                "s.ts BETWEEN o.ts - INTERVAL '3' MINUTE AND o.ts + INTERVAL '30' MINUTE";
        tEnv.executeSql(orders);
        tEnv.executeSql(ships);
        tEnv.executeSql(result);
        tEnv.executeSql(sql);

    }
}
