package com.cqz.iceberg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.actions.Actions;

import java.util.HashMap;
import java.util.Map;

public class RunRewriteDataFiles {

    public static void main(String[] args) {
        if (args.length<3){
            System.out.println("no enough agrs. Input: database table datekey [event]");
            return;
        }
        String database = args[0];
        String tablename = args[1];
        String datekey = args[2];

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        Map icebergMap=new HashMap<>();
        icebergMap.put("type", "iceberg");
        icebergMap.put("catalog-type", "hive");
        icebergMap.put("property-version", "1");
        icebergMap.put("clients", "5");
        icebergMap.put("uri","thrift://10.21.0.163:9083");
        icebergMap.put("warehouse", "hdfs://4399cluster/hive/warehouse");

        CatalogLoader hiveCatalog=CatalogLoader.hive("hive_catalog", new Configuration(), icebergMap);
        Catalog catalog=hiveCatalog.loadCatalog();

        TableIdentifier tableIdentifier=TableIdentifier.of(Namespace.of(database),tablename);
        Table table=catalog.loadTable(tableIdentifier);
        if (args.length>3){
            String event = args[3];
            rewriteDataFiles(env,table,datekey,event);
        }else {
            rewriteDataFiles(env,table,datekey,null);
        }
        deleteOldSnapshot(table);

//        table.refresh();
    }

        private static void rewriteDataFiles(StreamExecutionEnvironment env, Table table,String datekey,String event) {
        if (event!=null){
             Actions.forTable(env, table)
                    .rewriteDataFiles()
                    .maxParallelism(10)
                    .filter(Expressions.equal("datekey", datekey))
                    .filter(Expressions.equal("event", event))
                    .targetSizeInBytes(128 * 1024 * 1024)
                    .execute();

        }else {
            Actions.forTable(env,table)
                    .rewriteDataFiles()
                    .maxParallelism(20)
                    .filter(Expressions.equal("datekey",datekey))
                    .targetSizeInBytes(128*1024*1024)
                    .execute();
        }
    }

    private static void deleteOldSnapshot(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        //只保留最新的快照
        if (snapshot!=null){
            long time = snapshot.timestampMillis();
            table.expireSnapshots()
                    .expireOlderThan(time)
                    .commit();
        }
    }


//        table.rewriteManifests()
//                .rewriteIf(manifestFile -> manifestFile.length()<32*1024*1024)
//                .clusterBy(dataFile -> dataFile.partition().get(0,String.class))
//                .commit();



}
