package com.cqz.component.flink.influxdb;

public class InfluxdbTest {

    private static final String host = "localhost";
    private static final int port = 8086;
    private static final String username="admin";
    private static final String password="admin123";
    private static final String database="demo";

    public static void main(String[] args) {
        InfluxdbClient influxdb = new InfluxdbClient(host,port,username,password,database);
        influxdb.open();
        influxdb.query("select * from add_test");
    }


}
