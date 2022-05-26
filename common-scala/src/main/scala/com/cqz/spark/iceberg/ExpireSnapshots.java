package com.cqz.spark.iceberg;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class ExpireSnapshots {

    private static SparkSession spark = null;

    public static void main(String[] args) throws Exception {
        if (args.length<2){
            System.out.println("no enough agrs. Input: database table");
            return;
        }
//        String warehouse = args[0];
        String database = args[0];
        String tablename = args[1];

        SparkConf conf = new SparkConf()
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.hive_catalog.type", "hive")
                .set("spark.sql.catalog.hive_catalog.warehouse", "hdfs://4399cluster/hive/warehouse");
        spark = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
        //加载iceberg表
        Table table = Spark3Util.loadIcebergTable(spark, String.format("hive_catalog.%s.%s",database,tablename));

        Snapshot snapshot=table.currentSnapshot();
        //只保留当前快照
        if (snapshot !=null) {
            table.expireSnapshots()
                    .expireOlderThan(snapshot.timestampMillis())
                    .commit();
        }
        spark.close();
    }
}
