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

public class RunCombineTableFiles {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        Map icebergMap=new HashMap<>();
        icebergMap.put("type", "iceberg");
        icebergMap.put("catalog-type", "hive");
        icebergMap.put("property-version", "1");
        icebergMap.put("clients", "5");
        icebergMap.put("uri","thrift://10.21.0.240:9083");
        icebergMap.put("warehouse", "hdfs://qzcs240:9000/tmp/chenqizhu/warehouse/hive");

        CatalogLoader hiveCatalog=CatalogLoader.hive("hive_catalog", new Configuration(), icebergMap);
        Catalog catalog=hiveCatalog.loadCatalog();

        TableIdentifier tableIdentifier=TableIdentifier.of(Namespace.of("default"), "gamebox_event_iceberg3");
        Table table=catalog.loadTable(tableIdentifier);
        //合并小文件
        combineFiles(env,table);
        //删除历史快照
        deleteOldSnapshot(table);
    }

    private static void combineFiles(StreamExecutionEnvironment env, Table table) {
        Actions.forTable(env,table)
                .rewriteDataFiles()
                .maxParallelism(1)
                .filter(Expressions.equal("datekey","20220426"))
                .targetSizeInBytes(128*1024*1024)
                .execute();
        table.rewriteManifests()
                .rewriteIf(manifestFile -> manifestFile.length()<32*1024*1024)
                .clusterBy(dataFile -> dataFile.partition().get(0,String.class))
                .commit();
    }

    private static void deleteOldSnapshot(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        //删除5分钟前的历史快照
        long oldSnapshot = snapshot.timestampMillis();
//                - TimeUnit.MINUTES.toMillis(5);
//        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        if (snapshot!=null){
            table.expireSnapshots().expireOlderThan(oldSnapshot).commit();
        }
    }

}
