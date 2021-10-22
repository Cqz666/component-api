package com.cqz.component.flink.sql.client;

import org.junit.Test;

public class ArgsParserTest {

    @Test
    public void getSQLFromFile() throws Exception {
        String sql = ArgsParser.getSQLFromFile("/home/cqz/IdeaProjects/component-api/flink/flink-test/src/main/resources/data.txt");
        System.out.println(sql);
    }
}