package com.cqz.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;

import java.util.HashMap;
import java.util.Map;

public class ExpireSnapshots {
    public static void main(String[] args) {
        String database = args[0];
        String tablename = args[1];

        System.out.println(String.format("database:%s \n tablename:%s",database,tablename));

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

        Snapshot snapshot = table.currentSnapshot();
        //只保留最新的快照
        if (snapshot!=null){
            long time = snapshot.timestampMillis();
            table.expireSnapshots()
                    .expireOlderThan(time)
                    .commit();
        }else {
            System.out.println("snapshot is null!!!");
        }

    }
}
