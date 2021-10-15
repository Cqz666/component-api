package com.cqz.component.flink.sql.utils;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class SplitterTest {
    private static final char SQL_DELIMITER = ';';

    @Test
    public void splitEscaped() {
        String sql = "CREATE TEMPORARY FUNCTION my_sum as 'com.cqz.component.flink.sql.udf.SumFunction';" +
                "\n" +
                "CREATE TEMPORARY TABLE MyTable (id int,myField String) with('connector' = 'filesystem','path' = 'hdfs:///tmp/chenqizhu/data.txt','format' = 'csv'  );" +
                "\n" +
                "SELECT id,my_sum(id,2 ) as cnt FROM MyTable;";
        Splitter splitter = new Splitter(SQL_DELIMITER);
        List<String> stmts = splitter.splitEscaped(sql);
        System.out.println(stmts.size());
        stmts.stream()
                .filter(stmt -> !Strings.isNullOrEmpty(stmt.trim()))
                 .forEach(s -> System.out.println("---"+s));
    }
}