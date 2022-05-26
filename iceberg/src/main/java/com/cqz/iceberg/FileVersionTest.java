package com.cqz.iceberg;

import org.apache.hadoop.fs.Path;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileVersionTest {
    private static final Pattern LOG_FILE_PATTERN =
            Pattern.compile("\\.(.*)_(.*)\\.(.*)\\.([0-9]*)(_(([0-9]*)-([0-9]*)-([0-9]*)))?");
    public static void main(String[] args) {
        String logFile = makeLogFileName(UUID.randomUUID().toString(), ".log", "100", 2, "1-0-1");
        System.out.println("Log File =" + logFile);
        Path rlPath = new Path(new Path("2022/05/05/"), logFile);

//        Matcher matcher = LOG_FILE_PATTERN.matcher(rlPath.getName());
        Matcher matcher = LOG_FILE_PATTERN.matcher(".1ed74dfe-0b56-49d1-9ebe-dacce3fb325e_20220511150719715.log.9_0-2-3");
        if (!matcher.find()) {
            System.out.println("error");
        }
        System.out.println( Integer.parseInt(matcher.group(4)));
    }

    public static String makeLogFileName(String fileId, String logFileExtension, String baseCommitTime, int version,
                                         String writeToken) {
        String suffix = (writeToken == null)
                ? String.format("%s_%s%s.%d", fileId, baseCommitTime, logFileExtension, version)
                : String.format("%s_%s%s.%d_%s", fileId, baseCommitTime, logFileExtension, version, writeToken);
        return "." + suffix;
    }
}
