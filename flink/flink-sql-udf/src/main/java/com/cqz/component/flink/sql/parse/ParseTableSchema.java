package com.cqz.component.flink.sql.parse;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

import java.util.*;
import java.util.stream.Collectors;

public class ParseTableSchema {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableEnvironmentImpl tableEnvImpl = (TableEnvironmentImpl) tableEnv;

        String case1="create table a(id int,name varchar)with('connector'='kafka','topic' = 'xxx','format'='csv','properties.bootstrap.servers'='localhost:9092');" +
                "create table b(id int)with('connector'='kafka');"+
                "create table c(id int)with('connector'='kafka');"+
                "insert into b select * from a";
        String case2 ="CREATE TABLE gamebox_event(\n" +
                "  `client_timestamp` BIGINT,\n" +
                "  `$city` varchar,\n" +
                "  `$network_type` varchar,\n" +
                "  `event` varchar,\n" +
                "  `uid` varchar,\n" +
                "  `vid` varchar\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'gamebox_event',\n" +
                "  'properties.bootstrap.servers' = '10.21.0.131:9092,10.21.0.132:9092,10.20.0.2:9092,10.20.0.3:9092',\n" +
                "  'properties.group.id' = 'flinksql-clicks-topn',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ");\n" +
                "\n" +
                "create view myview as\n" +
                "select\n" +
                "uid,\n" +
                "vid,\n" +
                "$network_type,\n" +
                "$city,\n" +
                "client_timestamp\n" +
                "from gamebox_event where event='ranking_show';\n" +
                "\n" +
                "select * from myview;";
        String case4 =
                "CREATE TABLE gamebox_event_clicks( \n" +
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
                        "  'topic' = 'xxxx', \n" +
                        "  'properties.bootstrap.servers' = 'xxxxxxxxx', \n" +
                        "  'properties.group.id' = 'flinksql-clicks-topn', \n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ");\n" +
                        "\n" +
                        "\n" +
                        "create table dim_game(\n" +
                        "  `id` bigint,\n" +
                        "  `appname` varchar\n" +
                        ")with(\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://xxxxxxxxxx',\n" +
                        "  'username'='xxxxx',\n" +
                        "  'password'='xxxxx',\n" +
                        "  'table-name'='mobi_game_base',\n" +
                        "  'driver' = 'com.mysql.jdbc.Driver',\n" +
                        "  'scan.fetch-size' = '200',\n" +
                        "  'lookup.cache.max-rows' = '2000',\n" +
                        "  'lookup.cache.ttl' = '300s'\n" +
                        ");\n" +
                        "\n" +
                        "create table result_print(\n" +
                        "  `rk` bigint,\n" +
                        "  `w_end` TIMESTAMP,\n" +
                        "  `appname` varchar,\n" +
                        "  `cnt` bigint,\n" +
                        "  PRIMARY KEY(rk,w_end)NOT ENFORCED\n" +
                        ")with(\n" +
                        "  'connector'='print'\n" +
                        ");\n" +
                        "\n" +
                        "create table result_mysql(\n" +
                        "  `rk` bigint,\n" +
                        "  `w_end` TIMESTAMP,\n" +
                        "  `appname` varchar,\n" +
                        "  `cnt` bigint,\n" +
                        "  PRIMARY KEY(rk,w_end)NOT ENFORCED\n" +
                        ")with(\n" +
                        "   'connector' = 'jdbc',\n" +
                        "   'url' = 'jdbc:mysql://xxxxxxxxx',\n" +
                        "   'username'='xxxx',\n" +
                        "   'password'='xxxx',\n" +
                        "   'table-name' = 'realtime_clicks_topN'\n" +
                        ");\n" +
                        "\n" +
                        "\n" +
                        "create view result_view as\n" +
                        "\tselect rk,w_end,appname,cnt from (\n" +
                        "\t\tselect *,row_number() over(partition by w_end order by cnt desc) as rk\n" +
                        "\t\tfrom(\t\n" +
                        "\t\t\tselect  appname,\n" +
                        "\t\t\t\thop_end(event_time,interval '10' minute,interval '1' hour) w_end,\n" +
                        "\t\t\t\tcount(appname) as cnt\n" +
                        "\t\t\tfrom(\n" +
                        "\t\t\t\tselect\ta.event_time,\n" +
                        "\t\t\t\t\tb.appname\n" +
                        "\t\t\t\tfrom gamebox_event_clicks as a left join dim_game for system_time as of a.proctime as b\n" +
                        "\t\t\t\ton a.game_id=b.id where a.event='click_game' and a.game_id is not null and a.game_id <> '' and a.game_id <> '-1'\n" +
                        "\t\t\t) group by hop(event_time,interval '10' minute,interval '1' hour),appname\n" +
                        "\t\t)\n" +
                        "\t) where rk<=5;\n" +
                        "\n" +
                        "insert into result_print select * from result_view;\n" +
                        "insert into result_mysql select * from result_view;\n";

        Parser sqlParser = tableEnvImpl.getParser();
        CatalogManager catalogManager = tableEnvImpl.getCatalogManager();

        SqlParser parser = buildSqlParser(case2);

        List<SqlNode> sqlNodeList = parser.parseStmtList().getList();

        //source表 schema
        Map<String,String> logicalType = new LinkedHashMap<>();
        //select的字段, 也就是sink表要定义的字段
        List<String> sinkTableColumn = new ArrayList<>();

        long start = System.nanoTime();
        for (SqlNode sqlNode : sqlNodeList) {
            List<Operation> operations = sqlParser.parse(sqlNode.toString());
            Operation operation = operations.get(0);
            if (sqlNode.getKind().equals(SqlKind.CREATE_TABLE)) {
                CreateTableOperation createTableOperation = (CreateTableOperation) operation;
//                System.out.println("//-------------------------------------------------");
//                System.out.println(createTableOperation.asSummaryString());
//                System.out.println("//-------------------------------------------------");

                catalogManager.createTemporaryTable(createTableOperation.getCatalogTable(),
                        createTableOperation.getTableIdentifier(), createTableOperation.isIgnoreIfExists());
                System.out.println("//-------------------------------------------------");
                String tableName = createTableOperation.getTableIdentifier().getObjectName();
                Map<String, String> options = createTableOperation.getCatalogTable().getOptions();

                Map<String, String> newOptions = new HashMap<>();
                newOptions.put("connector","redis");
                CatalogTable redisTable = createTableOperation.getCatalogTable().copy(newOptions);
                Map<String, String> copyedOption =redisTable.getOptions();

//                catalogManager.createTemporaryTable(redisTable,
//                        createTableOperation.getTableIdentifier(), createTableOperation.isIgnoreIfExists());

                createTableOperation.getCatalogTable()
                        .getSchema()
                        .getTableColumns()
                        .stream()
                        .filter(TableColumn::isPhysical)
                        .forEach(tableColumn -> {
                    System.out.println(tableColumn.getName()+" "+tableColumn.getType().getLogicalType().toString());
                            logicalType.put(tableColumn.getName(),tableColumn.getType().getLogicalType().toString());
                });

//                catalogManager.dropTemporaryTable(ObjectIdentifier.of(catalogManager.getCurrentCatalog(),
//                        catalogManager.getCurrentDatabase(),tableName),true);
            }

            if (sqlNode.getKind().equals(SqlKind.CREATE_VIEW)){
                CreateViewOperation createViewOperation = (CreateViewOperation) operation;
                catalogManager.createTemporaryTable(createViewOperation.getCatalogView(),
                        createViewOperation.getViewIdentifier(),createViewOperation.isIgnoreIfExists());
                String expandedQuery = createViewOperation.getCatalogView().getExpandedQuery();
                System.out.println("expandedQuery----->"+expandedQuery);
                String originalQuery = createViewOperation.getCatalogView().getOriginalQuery();
                System.out.println("originalQuery----->"+originalQuery);
                CatalogBaseTable copy = createViewOperation.getCatalogView().copy();
                List<Schema.UnresolvedColumn> columns = copy.getUnresolvedSchema().getColumns();
                for (Schema.UnresolvedColumn column : columns) {
                    if (column instanceof Schema.UnresolvedPhysicalColumn){
                        Schema.UnresolvedPhysicalColumn unresolvedPhysicalColumn = (Schema.UnresolvedPhysicalColumn) column;
                        String name = unresolvedPhysicalColumn.getName();
                        String type = unresolvedPhysicalColumn.getDataType().toString();
                        System.out.println("name:"+name+",type:"+type);
                    }
                }
                System.out.println("--------");
            }
            System.out.println("//-------------------------------------------------");
            if (sqlNode.getKind().equals(SqlKind.SELECT)){
                QueryOperation queryOperation = (QueryOperation)operation;

                ResolvedSchema resolvedSchema = queryOperation.getResolvedSchema();
                List<Column> columns = resolvedSchema.getColumns();
                columns.stream().filter(Column::isPhysical).forEach(column ->{
                    System.out.println(column.getName()+" " + column.getDataType().getLogicalType().toString());
                    sinkTableColumn.add(column.getName());
                });
                Map<String, String> sinkop = new HashMap<>();
                sinkop.put("connector","redis");
                List<Column> pyhsicalColumns = columns.stream().filter(Column::isPhysical).collect(Collectors.toList());
                ResolvedSchema pyhsicalSchema = ResolvedSchema.of(pyhsicalColumns);

                catalogManager.createTemporaryTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(pyhsicalSchema).build(),
                                null,
                                Collections.emptyList(),
                                sinkop),
                        ObjectIdentifier.of(catalogManager.getCurrentCatalog(),catalogManager.getCurrentDatabase(),"sql_preview"), false);

                Optional<CatalogManager.TableLookupResult> tableLookupResult =
                catalogManager.getTable(
                        ObjectIdentifier.of(
                                catalogManager.getCurrentCatalog(),
                                catalogManager.getCurrentDatabase(),
                                "sink_redis"));
                CatalogManager.TableLookupResult result = tableLookupResult.orElseGet(null);

                String sinkSchema = result.getResolvedSchema().toString();
                System.out.println("-----------------------------");
                System.out.println(sinkSchema);
                System.out.println("-----------------------------");
                result.getTable();

            }
        }
        System.out.println("Parse Done in "+(System.nanoTime()-start)/1_000_000+" ms");
        System.out.println("//-------------------------------------------------");

//        Optional<CatalogManager.TableLookupResult> tableLookupResult =
//                catalogManager.getTable(
//                        ObjectIdentifier.of(
//                                catalogManager.getCurrentCatalog(),
//                                catalogManager.getCurrentDatabase(),
//                                "case2"));
//
//        CatalogManager.TableLookupResult result = tableLookupResult.orElseGet(null);
//        ResolvedSchema resolvedSchema = result.getResolvedSchema();
//        List<Column> columns = resolvedSchema.getColumns();
//        columns.stream().filter(Column::isPhysical).forEach(column ->{
//                    System.out.println(column.getName()+" " + column.getDataType().getLogicalType().toString());
//                });
//        System.out.println("resolvedSchema Done in "+(System.nanoTime()-start)/1_000_000+" ms");

        Map<String, String> sinkLogicType = new HashMap<>();


        for (String column : sinkTableColumn) {
            String type = logicalType.get(column);
            sinkLogicType.put(column,type);
        }
        System.out.println("sinkLogicType-----"+sinkLogicType.toString());


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
