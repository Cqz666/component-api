package com.cqz.component.flink.sql.parse;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;

import java.util.List;
import java.util.Optional;

import static org.apache.calcite.avatica.util.Quoting.BACK_TICK;

public class SQLParserDemo3 {

    public static void main(String[] args) throws SqlParseException {
        //1. 先创建ExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //tableEnv.registerCatalog...
        Parser parserImpl = ((TableEnvironmentImpl) tableEnv).getParser();

        CatalogManager catalogManager = ((TableEnvironmentImpl) tableEnv).getCatalogManager();
        StreamPlanner planner = (StreamPlanner) ((TableEnvironmentImpl) tableEnv).getPlanner();
        FlinkPlannerImpl flinkPlanner = planner.createFlinkPlanner();

//        String sql = "create table tab1(id int,name varchar)WITH('connector' = 'filesystem')" ;
        String sql = "create table tab1(id int,name varchar)WITH('connector' = 'filesystem');" +
                "\n" +
                "insert into tb32 select * from tab1";
        List<SqlNode>  sqlNodes = parseFlinkSQL(sql);
        for (SqlNode sqlNode : sqlNodes) {
            Optional<Operation> operationOptional =
                    SqlToOperationConverter.convert(flinkPlanner, catalogManager, sqlNode);
        }

    }


    public static List<SqlNode>  parseFlinkSQL(String sql) throws SqlParseException {

        SqlParser parser = SqlParser.create(sql, SqlParser.configBuilder()
                .setParserFactory(FlinkSqlParserImpl.FACTORY)
                .setQuoting(BACK_TICK)
                .setUnquotedCasing(Casing.TO_LOWER)   //字段名统一转化为小写
                .setQuotedCasing(Casing.UNCHANGED)
                .setConformance(FlinkSqlConformance.DEFAULT)
                .build());

        return parser.parseStmtList().getList();

    }
}
