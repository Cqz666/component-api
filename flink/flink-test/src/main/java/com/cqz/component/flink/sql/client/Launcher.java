package com.cqz.component.flink.sql.client;

import com.cqz.component.flink.sql.parse.SqlCommandParser;
import com.cqz.component.flink.sql.parse.SqlExecutionException;
import com.cqz.component.flink.sql.throwable.SqlParserException;
import com.cqz.component.flink.sql.utils.SQLStringUtil;
import com.cqz.component.flink.sql.utils.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Launcher {
    public static Logger LOG = LoggerFactory.getLogger(Launcher.class);


    public static void main(String[] args) throws Exception {
        if (args.length<1){
            System.out.println("there is not enough args,Usage sql udf");
            return;
        }
        LOG.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));
        LOG.info("-------------------------------------------");

//        String sql = args[0];
        String sqlpath=args[0];
        String sql = ArgsParser.getSQLFromFile(sqlpath);
//        String tmp = ArgsParser.getSQLFromHdfsFile(sqlpath);
//        String sql = URLDecoder.decode(tmp, StandardCharsets.UTF_8.name());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        System.out.println("-----------sql-----------");
        System.out.println(sql);
        System.out.println("------------sql----------");

//        registerCatalog(tEnv);

        exeSqlJob(env,tEnv,sql );

//        env.execute("preview_job");

    }


    public static void exeSqlJob(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tEnv,
            String sql){
        try {
             parseSql(sql, tEnv, env);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public static StatementSet parseSql(String sql, StreamTableEnvironment tableEnvironment, StreamExecutionEnvironment env) throws Exception{
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL must be not empty!");
        }
        sql = SQLStringUtil.dealSqlComment(sql);
        StatementSet statement = tableEnvironment.createStatementSet();
        Splitter splitter = new Splitter(';');
        List<String> stmts = splitter.splitEscaped(sql);

        for (String stmt : stmts) {
            Optional<Operation> operation = parseCommand(tableEnvironment, stmt);
            operation.ifPresent(op ->{
                if (op instanceof QueryOperation){

                    Table table = tableEnvironment.sqlQuery(stmt);

//                    DataStream<Tuple2<Boolean, Row>> stream = tableEnvironment.toRetractStream(table, Row.class);
//                    stream.print();
                    TableResult execute = table.execute();
                    execute.print();
                }else {
                    try {
                        tableEnvironment.executeSql(stmt);
                    } catch (Exception e) {
                        throw new SqlParserException(stmt, e.getMessage(), e);
                    }
                }
            });
        }


        return statement;
    }

    public static Operation parseStatement(TableEnvironment tableEnv, String statement){
        Parser parser = ((TableEnvironmentInternal) tableEnv).getParser();
        List<Operation> operations;
        try {
            operations = parser.parse(statement);
        } catch (Exception e) {
            throw new SqlExecutionException("Failed to parse statement: " + statement, e);
        }
        return operations.get(0);
    }

    private static Optional<Operation> parseCommand(TableEnvironment tableEnv, String stmt) {
        // normalize
        stmt = stmt.trim();
        // remove ';' at the end
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }

        // meet bad case, e.g ";\n"
        if (stmt.trim().isEmpty()) {
            return Optional.empty();
        }

        Operation operation = parseStatement(tableEnv,stmt);
        return Optional.of(operation);
    }


    }
