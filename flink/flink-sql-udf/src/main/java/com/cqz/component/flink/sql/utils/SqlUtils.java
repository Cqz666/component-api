package com.cqz.component.flink.sql.utils;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class SqlUtils {

    public static String getSqlText(String sqlFile) {
        if (StringUtils.isEmpty(sqlFile)) {
            throw new IllegalArgumentException("SQL file " + sqlFile + " is empty");
        }
        File file = new File(sqlFile);
        if (!(file.exists() && file.isFile())) {
            throw new IllegalArgumentException(
                    "SQL file "
                            + file.getAbsolutePath()
                            + " does not exits ,or SQL file does not a file !");
        }
        String sqlText;
        try {
            sqlText = new String(FileUtils.readAllBytes(Paths.get(sqlFile)));
        } catch (IOException e) {
            throw new RuntimeException(
                    "Unable to read SQL file to SQL text, cause by " + e.getMessage());
        }
        return sqlText;
    }

    private static final char SQL_DELIMITER = ';';

    public static List<String> splitSqlText(String sqlText) {
        sqlText =
                SqlStringUtils.dealSqlComment(sqlText)
                        .replaceAll("\r\n", " ")
                        .replaceAll("\n", " ")
                        .replace("\t", " ")
                        .trim();
        return SqlStringUtils.splitIgnoreQuota(sqlText, SQL_DELIMITER);
    }

    public static SqlParser buildSqlParser(String sql) {
        return SqlParser.create(sql, SqlParser.config()
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withQuoting(Quoting.BACK_TICK)
                .withUnquotedCasing(Casing.TO_LOWER)
                .withQuotedCasing(Casing.UNCHANGED)
                .withConformance(FlinkSqlConformance.DEFAULT)
        );
    }


}
