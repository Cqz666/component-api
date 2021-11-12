package com.cqz.component.flink.sql.udf;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import java.util.List;
import java.util.Optional;


public class TestParseFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        final TableConfig tableConfig = stEnv.getConfig();
        String sql = "";

        final CalciteParser calciteParser = createCalciteParser(tableConfig);
        final SqlNode parsed = calciteParser.parse(sql);
        if (parsed instanceof SqlSelect){
            final SqlSelect sqlSelect = (SqlSelect) parsed;
            final SqlNodeList selectList = sqlSelect.getSelectList();
            List<SqlNode> sqlNodes = selectList.getList();
            for (SqlNode sqlNode:sqlNodes){
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                final SqlOperator operator = sqlBasicCall.getOperator();
                if (operator instanceof BridgingSqlFunction){
                    BridgingSqlFunction bridgingSqlFunction = (BridgingSqlFunction) operator;
                    SqlFunction sqlFunction = (SqlFunction) operator;
                    final SqlFunctionCategory category = sqlFunction.getFunctionType();
                    final boolean userDefined = category.isUserDefined();
                    if (userDefined){
                        final FunctionIdentifier functionIdentifier = bridgingSqlFunction.getIdentifier().orElse(null);
                        final Optional<String> simpleName = functionIdentifier.getSimpleName();
                    }
                }



            }

        }


    }

    public static CalciteParser createCalciteParser(TableConfig tableConfig) {
        return new CalciteParser(getSqlParserConfig(tableConfig));
    }

    private static CalciteConfig getCalciteConfig(TableConfig tableConfig) {
        return TableConfigUtils.getCalciteConfig(tableConfig);
    }

    private static SqlParser.Config getSqlParserConfig(TableConfig tableConfig) {
        return JavaScalaConversionUtil.<SqlParser.Config>toJava(
                getCalciteConfig(tableConfig).getSqlParserConfig())
                .orElseGet(
                        // we use Java lex because back ticks are easier than double quotes in
                        // programming
                        // and cases are preserved
                        () -> {
                            SqlConformance conformance = getSqlConformance(tableConfig);
                            return SqlParser.config()
                                    .withParserFactory(FlinkSqlParserFactories.create(conformance))
                                    .withConformance(conformance)
                                    .withLex(Lex.JAVA)
                                    .withIdentifierMaxLength(256);
                        });
    }

    private static FlinkSqlConformance getSqlConformance(TableConfig tableConfig) {
        SqlDialect sqlDialect = tableConfig.getSqlDialect();
        switch (sqlDialect) {
            case HIVE:
                return FlinkSqlConformance.HIVE;
            case DEFAULT:
                return FlinkSqlConformance.DEFAULT;
            default:
                throw new TableException("Unsupported SQL dialect: " + sqlDialect);
        }
    }



}
