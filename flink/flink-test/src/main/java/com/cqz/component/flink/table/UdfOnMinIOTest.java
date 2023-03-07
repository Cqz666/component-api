package com.cqz.component.flink.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class UdfOnMinIOTest {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment env = TableEnvironment.create(settings);

        env.executeSql("CREATE TEMPORARY FUNCTION SumFunction as 'com.cqz.component.flink.sql.udf.SumFunction'");
        env.executeSql("create table MyTable(id int)with(" +
                "'connector'='filesystem','path'='/home/cqz/IdeaProjects/component-api/flink/flink-sql-udf/src/main/resources/data','format'='csv')");

        TableResult result = env.executeSql("select SumFunction(id,10) as a from MyTable ");
        result.print();
    }
}
