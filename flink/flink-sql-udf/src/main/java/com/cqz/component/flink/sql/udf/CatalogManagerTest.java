package com.cqz.component.flink.sql.udf;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

import java.util.List;
import java.util.Optional;

public class CatalogManagerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableEnvironmentImpl tableEnvImpl = (TableEnvironmentImpl) tableEnv;

        String case1="create table a(id int,name varchar)with('connector'='kafka','topic' = 'xxx','format'='csv','properties.bootstrap.servers'='localhost:9092');" +
                "create table b(id int)with('connector'='kafka');"+
                "create table c(id int)with('connector'='kafka');"+
                "insert into b select * from a";
        String case2 =                 "CREATE TABLE case2( \n" +
                "  `$client_ip` varchar, \n" +
                "  `$model` varchar, \n" +
                "  `$network_type` varchar, \n" +
                "  `event` varchar, \n" +
                "  `receive_timestamp` BIGINT,\n" +
                "  `client_timestamp` BIGINT, \n" +
                "  `uid` varchar, \n" +
                "  `vid` varchar,\n" +
                "  `game_id` bigint, \n" +
                "   proctime as PROCTIME(),\n" +
                "   event_time AS TO_TIMESTAMP(FROM_UNIXTIME(if(client_timestamp <> null, client_timestamp, UNIX_TIMESTAMP()),'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'),\n" +
                "   WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
                ") WITH ( \n" +
                "  'connector' = 'kafka',        \n" +
                "  'topic' = 'xxx', \n" +
                "  'properties.bootstrap.servers' = 'xxx', \n" +
                "  'properties.group.id' = 'flinksql-clicks-topn', \n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ");";

        Parser sqlParser = tableEnvImpl.getParser();
        CatalogManager catalogManager = tableEnvImpl.getCatalogManager();

        SqlParser parser = buildSqlParser(case2);

        List<SqlNode> sqlNodeList = parser.parseStmtList().getList();
        for (SqlNode sqlNode : sqlNodeList) {
            List<Operation> operations = sqlParser.parse(sqlNode.toString());
            Operation operation = operations.get(0);
            if (sqlNode.getKind().equals(SqlKind.CREATE_TABLE)) {
                CreateTableOperation createTableOperation = (CreateTableOperation) operation;
                catalogManager.createTemporaryTable(createTableOperation.getCatalogTable(),
                        createTableOperation.getTableIdentifier(), createTableOperation.isIgnoreIfExists());
            }
        }

        Optional<CatalogManager.TableLookupResult> tableLookupResult =
                catalogManager.getTable(
                        ObjectIdentifier.of(
                                catalogManager.getCurrentCatalog(),
                                catalogManager.getCurrentDatabase(),
                                "case2"));

        CatalogManager.TableLookupResult result = tableLookupResult.orElseGet(null);
        ResolvedSchema resolvedSchema = result.getResolvedSchema();
        List<Column> columns = resolvedSchema.getColumns();
        columns.stream().filter(Column::isPhysical).forEach(column ->{
                    System.out.println(column.getName()+" " + column.getDataType().getLogicalType().toString());
                });

    }
    private static SqlParser buildSqlParser(String sql) {
        return SqlParser.create(sql, SqlParser.config()
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withQuoting(Quoting.BACK_TICK)
                .withUnquotedCasing(Casing.TO_LOWER)
                .withQuotedCasing(Casing.UNCHANGED)
                .withConformance(FlinkSqlConformance.DEFAULT)
        );
    }
}
