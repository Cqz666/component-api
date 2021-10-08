package com.cqz.component.flink.sql.parse;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class SQLParserDemo {
    private static final Logger log = LoggerFactory.getLogger(SQLParserDemo.class);

    public static final String MESSAGE_SQL_EXECUTION_ERROR = "Could not execute SQL statement.";

    public static void main(String[] args) {
        String statement = "create table tab1(id int,name varchar)WITH('connector' = 'filesystem');";
//        String statement = "insert into tb32 select * from tab1";
        SQLParserDemo demo = new SQLParserDemo();
        demo.executeStatement(statement);
    }

    private void executeStatement(String statement){
        try{
        final Optional<Operation> operation =parseCommand(statement);
        } catch (SqlExecutionException e) {
            printExecutionException(e);
        }
    }

    private Optional<Operation> parseCommand(String stmt) {
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

        Operation operation = parseStatement(stmt);
        return Optional.of(operation);

    }

    public Operation parseStatement(String statement){
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Parser parser = ((TableEnvironmentInternal) tableEnv).getParser();

        List<Operation> operations;
        try {
            operations = parser.parse(statement);
        } catch (Exception e) {
            throw new SqlExecutionException("Failed to parse statement: " + statement, e);
        }
        if (operations.isEmpty()) {
            throw new SqlExecutionException("Failed to parse statement: " + statement);
        }
        return operations.get(0);
    }

    public void printExecutionException(Throwable t){
        final String errorMessage = MESSAGE_SQL_EXECUTION_ERROR;
        log.warn(errorMessage, t);
        String msgError = messageError(errorMessage, t);
        System.out.println(">>>>>>>>>>"+msgError);
    }

    public static String messageError(String message, Throwable t) {
        while (t.getCause() != null
                && t.getCause().getMessage() != null
                && !t.getCause().getMessage().isEmpty()) {
            t = t.getCause();
        }
        return messageError(message, t.getClass().getName() + ": " + t.getMessage());
    }

    public static String messageError(String message, String s) {
        final StringBuilder builder =
                new StringBuilder()
                        .append("[ERROR] ")
                        .append(message);

        if (s != null) {
            builder.append(" Reason:\n").append(s);
        }

        return builder.toString();
    }



}
