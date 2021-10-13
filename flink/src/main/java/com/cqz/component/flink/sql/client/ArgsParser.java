package com.cqz.component.flink.sql.client;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;

public class ArgsParser {

    public static String getSQLFromFile(String pathname) throws Exception {
        String value ;
        File file = new File(pathname);
        try (FileInputStream in = new FileInputStream(file)) {
            byte[] fileContent = new byte[(int) file.length()];
            in.read(fileContent);
            value = new String(fileContent, StandardCharsets.UTF_8);
        }
        return value;
    }

}
