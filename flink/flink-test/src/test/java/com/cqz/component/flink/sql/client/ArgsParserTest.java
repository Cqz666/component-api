package com.cqz.component.flink.sql.client;

import org.junit.Test;

import java.util.List;

public class ArgsParserTest {

    public static final String file = "/home/cqz/IdeaProjects/component-api/flink/flink-test/src/main/resources/data.txt";

    @Test
    public void getSQLFromFile() throws Exception {
        String sql = ArgsParser.getSQLFromFile(file);
        System.out.println(sql);
    }


    @Test
    public void getSqlFromFileByFlinkAPI(){
        String sql = ArgsParser.getSqlFromFileByFlinkAPI(file);
        System.out.println(sql);
    }

    @Test
    public void getSqlListFromFile(){
        List<String> sqlListFromFile = ArgsParser.getSqlListFromFile(file);
        sqlListFromFile.forEach(System.out::println);
    }

}