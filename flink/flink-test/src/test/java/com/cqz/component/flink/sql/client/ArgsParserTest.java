package com.cqz.component.flink.sql.client;

import org.junit.Test;

public class ArgsParserTest {

    @Test
    public void getSQLFromFile() throws Exception {
        String sql = ArgsParser.getSQLFromFile("/home/cqz/1.com.cqz.component.flink.com.cqz.component.flink.sql");
        System.out.println(sql);
    }
}