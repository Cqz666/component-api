package com.cqz.component.flink.sql.client;

import org.junit.Test;

import static org.junit.Assert.*;

public class ArgsParserTest {

    @Test
    public void getSQLFromFile() throws Exception {
        String sql = ArgsParser.getSQLFromFile("/home/cqz/1.sql");
        System.out.println(sql);
    }
}