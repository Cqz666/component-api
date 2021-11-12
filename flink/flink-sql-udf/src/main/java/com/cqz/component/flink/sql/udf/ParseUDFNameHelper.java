package com.cqz.component.flink.sql.udf;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import java.util.ArrayList;
import java.util.List;

public class ParseUDFNameHelper {

    public static void main(String[] args) throws SqlParseException {
        String sqlText =
                "create view result_view as\n" +
                        "        select rk,w_end,appname,cnt from (\n" +
                        "                select *,row_number() over(partition by w_end order by cnt desc) as rk\n" +
                        "                from(\n" +
                        "                        select  appname,\n" +
                        "                                hop_end(event_time,interval '10' minute,interval '1' hour) w_end,\n" +
                        "                                count(appname) as cnt\n" +
                        "                        from(\n" +
                        "                                select  a.event_time,\n" +
                        "                                        b.appname\n" +
                        "                                from gamebox_event_clicks as a left join dim_game for system_time as of a.proctime as b\n" +
                        "                                on a.game_id=b.id where a.event='click_game' and a.game_id is not null and a.game_id <> '' and a.game_id <> '-1'\n" +
                        "                        ) group by hop(event_time,interval '10' minute,interval '1' hour),appname\n" +
                        "                )\n" +
                        "        ) where rk<=5;";

        String single = "select SumFunction(id,10) as a from MyTable";

        String sql = "select * from(\n" +
                "  select a,udf1(b),c from(\n" +
                "    select a,b,udf2(d) as c from table1\n" +
                "    )\n" +
                ")";

        final SqlParser sqlParser = buildSqlParser(sql);

        final List<SqlNode> sqlNodeList = sqlParser.parseStmtList().getList();
        List<String> parsedUdfName = new ArrayList<>();

        parsedUdfName = parseSqlNodes(sqlNodeList,parsedUdfName);
        parsedUdfName.forEach(System.out::println);

    }

private static List<String> parseSqlSelects(SqlSelect sqlSelect,List<String> udfs){
     SqlNodeList selectList = sqlSelect.getSelectList();
     List<SqlNode> selectListList = selectList.getList();
     SqlNode from = sqlSelect.getFrom();
    if (from instanceof SqlSelect){
        SqlSelect childrenSqlSelect = (SqlSelect) from;
        //
        for (SqlNode sqlNode : selectListList) {
            if (sqlNode instanceof SqlBasicCall){
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                parseBasicCall(sqlBasicCall,udfs);
            }
        }
        parseSqlSelects(childrenSqlSelect,udfs);
    }else {
        for (SqlNode sqlNode : selectListList) {
            if (sqlNode instanceof SqlBasicCall){
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                parseBasicCall(sqlBasicCall,udfs);
            }
        }
    }
    return udfs;
}

private static List<String> parseBasicCall(SqlBasicCall sqlBasicCall,List<String> udfs){
    final List<SqlNode> operandList = sqlBasicCall.getOperandList();
    final SqlOperator operator = sqlBasicCall.getOperator();
    if (operator instanceof  SqlUnresolvedFunction){
        SqlUnresolvedFunction sqlUnresolvedFunction = (SqlUnresolvedFunction) operator;
        final SqlFunctionCategory functionType = sqlUnresolvedFunction.getFunctionType();
        if (functionType.isUserDefinedNotSpecificFunction()||
                functionType.isUserDefined()){
            final String name = sqlUnresolvedFunction.getName();
            udfs.add(name);
        }
    }
    for (SqlNode sqlNode : operandList) {
        if (sqlNode instanceof SqlBasicCall)
        parseBasicCall((SqlBasicCall) sqlNode,udfs);
    }
    return udfs;
}

    private static List<String> parseSqlNodes(List<SqlNode> sqlNodes,List<String> udfs){
        for (SqlNode sqlNode : sqlNodes) {

            if (sqlNode instanceof SqlInsert){
                SqlInsert sqlInsert = (SqlInsert) sqlNode;
                final List<SqlNode> sqlNodeList = sqlInsert.getOperandList();
                parseSqlNodes(sqlNodeList,udfs);
            }
            if (sqlNode instanceof SqlCreateView){
                SqlCreateView sqlCreateView = (SqlCreateView) sqlNode;
                final List<SqlNode> sqlNodeList = sqlCreateView.getOperandList();
                parseSqlNodes(sqlNodeList,udfs);
            }

            if (sqlNode instanceof SqlSelect){
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                parseSqlSelects(sqlSelect,udfs);

            }
            if (sqlNode instanceof SqlBasicCall){
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                parseBasicCall(sqlBasicCall,udfs);

                final List<SqlNode> list = sqlBasicCall.getOperandList();
                parseSqlNodes(list,udfs);
            }

        }
        return udfs;
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
