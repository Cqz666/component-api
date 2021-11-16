package com.cqz.component.flink.sql.udf;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

        String sqlText2 = "CREATE VIEW summary_pvuv_min AS\n" +
                "        select\n" +
                "                CAST(TUMBLE_START(event_time,INTERVAL '1' MINUTE) AS VARCHAR) as app_ts,\n" +
                "                count(`$client_ip`) as pv,\n" +
                "                count(distinct `$client_ip`) as uv\n" +
                "        from gamebox_event_view\n" +
                "        where event='$view'\n" +
                "        group by TUMBLE(event_time,INTERVAL '1' MINUTE);";

        String sqlText3 = "select * from(\n" +
                "  select a,udf1(b),c from(\n" +
                "    select a,b,udf1(d) as c from table1\n" +
                "    )\n" +
                ")";

        String sqlText4 = "INSERT INTO hykb_event_flink_test_sink\n" +
                "SELECT event1, vid1, versionv1, uid1, app_version1\n" +
                "FROM\n" +
                "(\n" +
                "  SELECT event, vid, versionv, uid, `$app_version`, DynamicStringJoinFunction(event, vid, versionv, uid, `$app_version`, ' , ', ':') AS join_str\n" +
                "  FROM hykb_event_flink_test\n" +
                "  WHERE event IS NOT NULL AND vid IS NOT NULL AND versionv IS NOT NULL AND uid IS NOT NULL AND `$app_version` IS NOT NULL AND uid <> ''\n" +
                ")\n" +
                "LEFT JOIN LATERAL TABLE(DynamicFieldSplitFunction(join_str, ' , ', ':', 'a,b,c,d,e')) AS T(event1, vid1, versionv1, uid1, app_version1) ON TRUE;";

        final SqlParser sqlParser = buildSqlParser(sqlText4);

        final List<SqlNode> sqlNodeList = sqlParser.parseStmtList().getList();
        List<String> parsedUdfName = new ArrayList<>();

        parsedUdfName = parseSqlNodes(sqlNodeList,parsedUdfName)
                .stream()
                .distinct()
                .collect(Collectors.toList());
        parsedUdfName.forEach(System.out::println);

    }

private static List<String> parseSqlSelects(SqlNode sqlnode,List<String> udfs){
        if (sqlnode instanceof SqlSelect){
            SqlSelect sqlSelect = (SqlSelect) sqlnode;
            SqlNodeList selectList = sqlSelect.getSelectList();
            List<SqlNode> selectListList = selectList.getList();
            SqlNode from = sqlSelect.getFrom();
            if (from instanceof SqlSelect){
                SqlSelect childrenSqlSelect = (SqlSelect) from;
                for (SqlNode sqlNode : selectListList) {
                    if (sqlNode instanceof SqlBasicCall){
                        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                        parseBasicCall(sqlBasicCall,udfs);
                    }
                }
                parseSqlSelects(childrenSqlSelect,udfs);
            }
            else if (from instanceof SqlJoin){
                SqlJoin sqlJoin = (SqlJoin) from;
                for (SqlNode sqlNode : sqlJoin.getOperandList()){
                    if (sqlNode instanceof SqlBasicCall){
                        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                        parseBasicCall(sqlBasicCall,udfs);
                    }
                    if (sqlNode instanceof SqlSelect){
                        parseSqlSelects(sqlNode,udfs);
                    }
                }
            }
            else {
                for (SqlNode sqlNode : selectListList) {
                    if (sqlNode instanceof SqlBasicCall){
                        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                        parseBasicCall(sqlBasicCall,udfs);
                    }
                }
            }
        }
    return udfs;
}

private static List<String> parseBasicCall(SqlBasicCall sqlBasicCall,List<String> udfs){
    final List<SqlNode> operandList = sqlBasicCall.getOperandList();
    final SqlOperator operator = sqlBasicCall.getOperator();

    if (operator instanceof  SqlFunction ){
        SqlFunction sqlFunction = (SqlFunction) operator;
        final SqlFunctionCategory functionType = sqlFunction.getFunctionType();
        if (functionType.isFunction() ||
                functionType.isUserDefined() ||
                functionType.isTableFunction() ||
                functionType.isUserDefinedNotSpecificFunction()){
            final String name = sqlFunction.getName();
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

            if (sqlNode instanceof SqlInsert || sqlNode instanceof  SqlCreateView){
                final List<SqlNode> operandList = ((SqlCall) sqlNode).getOperandList();
                parseSqlNodes(operandList,udfs);
            }

            if (sqlNode instanceof SqlSelect){
                parseSqlSelects(sqlNode,udfs);
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
