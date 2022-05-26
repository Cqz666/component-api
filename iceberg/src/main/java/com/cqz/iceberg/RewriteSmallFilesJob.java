package com.cqz.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;

public class RewriteSmallFilesJob {
    public static void main(String[] args) {
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://qzcs240:9000/tmp/chenqizhu/warehouse/hadoop/default/gamebox_event_iceberg");
        tableLoader.open();
        Table table = tableLoader.loadTable();
        RewriteDataFilesActionResult result = Actions.forTable(table)
                .rewriteDataFiles()
                .execute();
    }

}
