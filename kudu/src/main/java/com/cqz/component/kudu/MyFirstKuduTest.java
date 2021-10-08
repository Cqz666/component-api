package com.cqz.component.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class MyFirstKuduTest {

    private static final Logger log = LoggerFactory.getLogger(MyFirstKuduTest.class);

    private final static String master = "10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051";

    public static void main(String[] args) {
        if (args.length<1){
            System.out.println("there is not enough args,.Usage TableOptions");
            return;
        }
        int op = Integer.parseInt(args[0]);
        System.out.println(">>>>>>>>op:"+op);

        log.info("init kudu client....");
        KuduClient client = new KuduClient.KuduClientBuilder(master).build();

        switch (TableOptions.getByCode(op)){
            case CREATE_TABLE:
                createTable(client);
                break;
            case DROP_TABLE:
                dropTable(client,"default.m_tab_1");
                break;
            default:
                break;
        }

    }


    private static void createTable(KuduClient client){
        // 设置表的schema
        List<ColumnSchema> columns = new LinkedList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("CompanyId", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("WorkId", Type.INT32).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("Name", Type.STRING).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("Gender", Type.STRING).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("Photo", Type.STRING).build());

        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        CreateTableOptions options = new CreateTableOptions();
        // 设置表的replica备份和分区规则
        List<String> parcols = new LinkedList<>();
        parcols.add("CompanyId");
        //设置表的备份数
        options.setNumReplicas(1);
        //设置range分区
        options.setRangePartitionColumns(parcols);
        //设置hash分区和数量
        options.addHashPartitions(parcols, 3);
        try {
            client.createTable("default.m_tab_1", schema, options);
            client.close();
            log.info("create kudu table success....");
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    private static void dropTable(KuduClient kuduClient, String tableName){
        try {
            kuduClient.deleteTable(tableName);
            System.out.println("成功删除Kudu表：" + tableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

}
