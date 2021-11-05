package com.cqz.component.flink.sql.catalog;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class CatalogManager {
    public static final String catalogName = "myhive";
    public static final String databaseName = "flink_kafka";
    public static final String hiveConfDir = "/usr/local/hive3/conf/";

    public static void registerHiveCatalog(StreamTableEnvironment tableEnv) {
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, "data_platform_test", hiveConfDir);
        tableEnv.registerCatalog(catalogName, hiveCatalog);
        tableEnv.useCatalog("myhive");
        tableEnv.useDatabase("data_platform_test");
    }

}
