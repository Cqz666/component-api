package com.cqz.component.flink.sql.udf;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;

import java.util.*;

import static com.cqz.component.flink.sql.udf.ParseUDFNameHelper.buildSqlParser;

public class ParseMultipleChain {

    private static List<String> TABLE_IN_DDL = new ArrayList<>();
    private static List<String> TABLE_IN_DML = new ArrayList<>();
    private static List<String> VIEW = new ArrayList<>();
    private static List<String> TABLE_IN_VIEW = new ArrayList<>();
    //K=sink,v=[source1,source2...]
    private static Map<String,List<String>> TABLE_MAP = new HashMap<>();

    public static void main(String[] args) throws SqlParseException {
        String case1="create table a(id int);" +
                "create table b(id int);"+
                "create table c(id int);"+
                "insert into b select * from a";
        String case2 = "create table a(id int)with('connector'='kafka');" +
                "create table b(id int);" +
                "create table c(id int);" +
                "create table d(id int);" +
                "insert into c select * from a as a1 join b as b1 on a1.id=b1.id;" +
                "insert into d select * from a;" ;
        String case3="create table source(id int)with('connector'='kafka');" +
                "create table source1(name string)with('connector'='kafka');" +
                "create table sink(id int)with('connector'='kafka');"+
                "create table sink1(name string)with('connector'='kafka');"+
                "create table nouse(id int)with('connector'='kafka');"+
                "insert into sink select * from source;"+
                "insert into sink1 select * from source1";
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
        String case5 = "INSERT INTO hykb_event_flink_test_sink\n" +
                "SELECT event1, vid1, versionv1, uid1, app_version1\n" +
                "FROM\n" +
                "(\n" +
                "  SELECT event, vid, versionv, uid, `$app_version`, DynamicStringJoinFunction(event, vid, versionv, uid, `$app_version`, ' , ', ':') AS join_str\n" +
                "  FROM hykb_event_flink_test\n" +
                "  WHERE event IS NOT NULL AND vid IS NOT NULL AND versionv IS NOT NULL AND uid IS NOT NULL AND `$app_version` IS NOT NULL AND uid <> ''\n" +
                ")\n" +
                "LEFT JOIN LATERAL TABLE(DynamicFieldSplitFunction(join_str, ' , ', ':', 'a,b,c,d,e')) AS T(event1, vid1, versionv1, uid1, app_version1) ON TRUE;";

        final SqlParser sqlParser = buildSqlParser(case4);
        final List<SqlNode> sqlNodeList = sqlParser.parseStmtList().getList();
        parseSqlNodes(sqlNodeList);

        System.out.println(TABLE_IN_DDL.toString());
        System.out.println("--------------------------------");
        System.out.println("TABLE_MAP :"+TABLE_MAP.toString());


    }

    private static void parseSqlNodes(List<SqlNode> sqlNodes){
        for (SqlNode sqlNode : sqlNodes) {
            //获取建表语句的表名，包括源表、目标表、维表
            if (sqlNode instanceof SqlCreateTable){
                SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;
                final String tableName = sqlCreateTable.getTableName().getSimple();
                TABLE_IN_DDL.add(tableName);
            }
            if (sqlNode instanceof SqlInsert){
                SqlInsert sqlInsert = (SqlInsert) sqlNode;
                SqlIdentifier targetTable = (SqlIdentifier) sqlInsert.getTargetTable();
                //val 目标表
                String table = targetTable.getSimple();
                //解析源表
                SqlNode source =  sqlInsert.getSource();
                parseSqlSelects(source);
                //保存目标表和源表对应关系
                TABLE_MAP.put(table,TABLE_IN_DML);
                TABLE_IN_DML = new ArrayList<>();
            }

            if (sqlNode instanceof SqlCreateView){
                TABLE_IN_VIEW = parseSqlView((SqlCreateView) sqlNode);
            }
        }
    }

    private static List<String> parseSqlView(SqlCreateView sqlCreateView){
        final String viewName = sqlCreateView.getViewName().toString();
        VIEW.add(viewName);
        final List<SqlNode> operandList = sqlCreateView.getOperandList();
        for (SqlNode node : operandList) {
            parseSqlSelects(node);
        }
        List<String> tableInView = new ArrayList<>(TABLE_IN_DML);
        TABLE_IN_DML = new ArrayList<>();
        return tableInView;
    }

    private static void parseSqlSelects(SqlNode sqlnode){
        if (sqlnode instanceof SqlSelect){
            SqlSelect sqlSelect = (SqlSelect) sqlnode;
            SqlNode from = sqlSelect.getFrom();
            //无子查询
            if (from instanceof SqlIdentifier){
                SqlIdentifier identifier = (SqlIdentifier) from;
                if (identifier.isSimple()){
                    //排除from view的情况
                     boolean isView = VIEW.stream().anyMatch(view -> view.equalsIgnoreCase(identifier.getSimple()));
                     if (isView){
                         //如果from的是一个view,则把视图里涉及的表存入，而不是视图名
                         TABLE_IN_DML.addAll(TABLE_IN_VIEW);
                     }else {
                         TABLE_IN_DML.add(identifier.getSimple());
                     }
                }else {
                    //可能from多张源表
                    TABLE_IN_DML.addAll(identifier.names);
                }
            }
            //有子查询，递归子查询
            if (from instanceof SqlSelect){
                parseSqlSelects(from);
            }
            //Join场景
            else if (from instanceof SqlJoin){
                SqlJoin sqlJoin = (SqlJoin) from;
                //左右表
                SqlNode left = sqlJoin.getLeft();
                //右表可能是维表，也可能是个双流JOIN
                SqlNode right = sqlJoin.getRight();
                //1.无别名
                if (left instanceof SqlIdentifier){
                    SqlIdentifier identifier = (SqlIdentifier) left;
                    TABLE_IN_DML.addAll(identifier.names);
                }
                if (right instanceof SqlIdentifier){
                    SqlIdentifier identifier = (SqlIdentifier) right;
                    TABLE_IN_DML.addAll(identifier.names);
                }
                //2.如果用了as别名,sqlNode是个sqlBasicCall
                if (left instanceof SqlBasicCall){
                    parseBasicCall(left);
                }
                if (right instanceof SqlBasicCall){
                    parseBasicCall(right);
                }
                //3.左右表可能是select子查询
                if (left instanceof SqlSelect){
                    parseSqlSelects(left);
                }
                if (right instanceof SqlSelect){
                    parseSqlSelects(right);
                }

            }
        }
    }

    private static void parseBasicCall(SqlNode sqlNode){
        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
        final List<SqlNode> operandList = sqlBasicCall.getOperandList();
        //join a as 别名的情况，取表就行
        final SqlNode table = operandList.get(0);
        if (table instanceof SqlIdentifier){
            TABLE_IN_DML.addAll(((SqlIdentifier) table).names);
        }
        //维表Join
        if (table instanceof SqlSnapshot){
            SqlSnapshot sqlSnapshot = (SqlSnapshot) table;
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlSnapshot.getTableRef();
            String dimTable = sqlIdentifier.getSimple();
            TABLE_IN_DML.add(dimTable);
        }

    }



}
