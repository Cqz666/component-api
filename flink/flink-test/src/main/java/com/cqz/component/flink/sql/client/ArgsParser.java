package com.cqz.component.flink.sql.client;

import com.cqz.component.flink.sql.utils.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static com.cqz.component.flink.sql.utils.HdfsUtil.HADOOP_CONF;

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

    public static String getSQLFromHdfsFile(String pathname){
        String value =null;
        Configuration hadoopConf = HdfsUtil.getHadoopConf(HADOOP_CONF);
        try {
            FileSystem fileSystem = HdfsUtil.getFileSystem(hadoopConf);
            InputStream in = HdfsUtil.openFile(fileSystem, pathname);
            byte[] fileContent = new byte[1024];
            in.read(fileContent);
            value = new String(fileContent, StandardCharsets.UTF_8);
           System.out.println("read sql from hdfs :"+value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    public static void main(String[] args) {
        int num = 1007;
        int stepSize = 3;
        int base = num/stepSize;
        int i = num % stepSize;
        System.out.println(base);
        System.out.println(i);
    }

}
