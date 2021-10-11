package com.cqz.component.flink.sql.client;

import com.cqz.component.flink.sql.throwable.SqlParserException;
import com.cqz.component.flink.sql.utils.SQLStringUtil;
import com.cqz.component.flink.sql.utils.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
//        if (args.length<1){
//            System.out.println("there is not enough args,.Usage sql");
//            return;
//        }
//        LOG.info("------------program params-------------------------");
//        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));
//        LOG.info("-------------------------------------------");

//        String sql = args[0];
        String sql ="CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        ROW<first_name STRING, last_name STRING>,\n" +
                "    order_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'number-of-rows' = '1'\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE print_table (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        ROW<first_name STRING, last_name STRING>,\n" +
                "    order_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");\n" +
                "\n" +
                "insert into print_table\n" +
                "select * from Orders;";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        exeSqlJob(tEnv,sql);

    }


    private static void exeSqlJob(
            StreamTableEnvironment tEnv,
            String sql){
        try {
            StatementSet statementSet = parseSql(sql, tEnv);
//            statementSet.execute();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public static StatementSet parseSql(String sql, StreamTableEnvironment tableEnvironment){
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL must be not empty!");
        }
        sql = SQLStringUtil.dealSqlComment(sql);
        StatementSet statement = tableEnvironment.createStatementSet();
        Splitter splitter = new Splitter(';');
        List<String> stmts = splitter.splitEscaped(sql);

        stmts.stream()
                .filter(stmt -> !Strings.isNullOrEmpty(stmt.trim()))
                .forEach(
                        stmt ->{
                            try {
                                tableEnvironment.executeSql(stmt);
                            } catch (Exception e) {
                                throw new SqlParserException(stmt, e.getMessage(), e);
                            }
                        }
                );
        return statement;
    }

}
