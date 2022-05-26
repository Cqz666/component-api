package com.cqz.spark.iceberg;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

public class RunCombineFiles {

    private static SparkSession spark = null;

    public static void main(String[] args) throws Exception {
        if (args.length<4){
            System.out.println("no enough agrs. Input: warehouse database table datekey [event]");
            return;
        }
        String warehouse = args[0];
        String database = args[1];
        String tablename = args[2];
        String datekey = args[3];

        SparkConf conf = new SparkConf()
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.hive_catalog.type", "hive")
                .set("spark.sql.catalog.hive_catalog.warehouse", warehouse);
         spark = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
         //加载iceberg表
        Table table = Spark3Util.loadIcebergTable(spark, String.format("hive_catalog.%s.%s",database,tablename));
        //快照删除
        deleteSnapshot(table);

        //执行合并操作
        if (args.length>4){
            String event = args[4];
            combineFiles(table,datekey,event);
        }else {
            combineFiles(table,datekey);
        }

        spark.close();
    }

    private static void combineFiles(Table table,String datekey){
        combineFiles(table,datekey,null);
    }

    private static void combineFiles(Table table,String datekey,String event){
        //数据文件合并
        if (event!=null){
            SparkActions
                    .get()
                    .rewriteDataFiles(table)
                    .filter(Expressions.equal("datekey", datekey))
                    .filter(Expressions.equal("event",event))
                    .option("target-file-size-bytes",Long.toString(128*1024*1024)) // 128MB
                    .execute();
        }else {
            SparkActions
                    .get()
                    .rewriteDataFiles(table)
                    .filter(Expressions.equal("datekey", datekey))
                    .option("target-file-size-bytes",Long.toString(128*1024*1024)) // 128MB
                    .execute();
        }
        //清单文件合并
//        SparkActions
//                .get()
//                .rewriteManifests(table)
//                .rewriteIf(manifestFile -> manifestFile.length()<10*1024*1024) // 10 MB
//                .execute();
        //清理无用文件
        SparkActions
                .get()
                .deleteOrphanFiles(table)
                .olderThan(table.currentSnapshot().timestampMillis()- TimeUnit.HOURS.toMillis(1))
                .execute();
    }

    private static void deleteSnapshot(Table table){
        Snapshot snapshot=table.currentSnapshot();
//         long oldSnapshot = snapshot.timestampMillis() - TimeUnit.MINUTES.toMillis(2);
        //只保留当前快照
        if (snapshot !=null) {
            table
                    .expireSnapshots()
                    .expireOlderThan(snapshot.timestampMillis())
                    .commit();
        }
    }

}
