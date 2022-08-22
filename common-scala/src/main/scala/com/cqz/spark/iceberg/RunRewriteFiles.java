package com.cqz.spark.iceberg;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RunRewriteFiles {
    private static final Logger log = LoggerFactory.getLogger(RunRewriteFiles.class);

    private static SparkSession spark = null;

    public static void main(String[] args) throws Exception {
        if (args.length<3){
            log.info("no enough agrs. Input: database table datekey [event]");
            return;
        }
        String database = args[0];
        String tablename = args[1];
        String datekey = args[2];

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
        log.info("===============RewriteFiles job starting.....");
//        rewriteFiles(table,datekey);
        log.info("===============ExpireSnapshots job starting.....");
//        expireSnapshots(table);
        log.info("===============DeleteOrphanFiles job starting.....");
        deleteOrphanFiles(table);
        log.info("===============All job finished.");

        spark.close();

    }

    private static void rewriteFiles(Table table,String datekey) {
        //数据文件合并
        SparkActions
                .get()
                .rewriteDataFiles(table)
                .filter(Expressions.equal("datekey", datekey))
                .option("target-file-size-bytes",Long.toString(128*1024*1024)) // 128MB
                .option("partial-progress.enabled","true")
                .option("max-concurrent-file-group-rewrites","100")
                .execute();
//            SparkActions
//            .get()
//            .rewriteManifests(table)
//            .rewriteIf(manifestFile -> manifestFile.length()<10*1024*1024) // 10 MB
//            .execute();
    }

    public static void expireSnapshots(Table table){
        Snapshot snapshot=table.currentSnapshot();
        if (snapshot !=null) {
            table.expireSnapshots()
                    .expireOlderThan(snapshot.timestampMillis())
                    .commit();
        }
    }

    public static void deleteOrphanFiles(Table table){
        SparkActions
                .get()
                .deleteOrphanFiles(table)
                .olderThan(table.currentSnapshot().timestampMillis() - TimeUnit.HOURS.toMillis(1))
                .execute();
    }

}
