package com.cqz.component.flink.influxdb;

import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class InfluxdbClient {

    private InfluxDB influxDB;
    private String host;
    private int port;
    private String username;
    private String password;
    private String database;

    public InfluxdbClient(String host, int port, String username, String password, String database) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
    }

    public InfluxdbClient(String host, int port,String database) {
        this.host = host;
        this.port = port;
        this.database = database;
    }

    public void open() {
        String url = String.format("http://%s:%d", host, port);
        OkHttpClient.Builder client =
                new OkHttpClient.Builder()
                        .connectTimeout(10000, TimeUnit.MILLISECONDS)
                        .writeTimeout(10000, TimeUnit.MILLISECONDS);

        if (username != null && password != null) {
            influxDB = InfluxDBFactory.connect(url, username, password, client);
        } else {
            influxDB = InfluxDBFactory.connect(url, client);
        }
    }

    public void query(String sql){
        long s = System.currentTimeMillis();
        QueryResult results =  influxDB.query(new Query(sql,database));

        for (QueryResult.Result result : results.getResults()) {
            for (QueryResult.Series series : result.getSeries()) {
                String name = series.getName();
                System.out.println("name="+name);
                for (String column : series.getColumns()) {
                    System.out.println("column="+column);
                }
                for (List<Object> value : series.getValues()) {
                    for (Object o : value) {
                        System.out.println("value="+o);
                    }
                }
            }
            System.out.println(result);
        }
        System.out.println("cost "+ (System.currentTimeMillis()-s)+" ms");

    }

    public void close() {
        if (influxDB != null) {
            influxDB.close();
            influxDB = null;
        }
    }


    }
