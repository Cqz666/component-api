package com.cqz.component.flink.sql.client;

import com.cqz.component.flink.sql.utils.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
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
        StringBuilder sb = new StringBuilder();
        Configuration hadoopConf = HdfsUtil.getHadoopConf(HADOOP_CONF);

        try (FileSystem fileSystem = HdfsUtil.getFileSystem(hadoopConf);
             InputStream in = HdfsUtil.openFile(fileSystem, pathname)){
            byte[] buffer = new byte[1024];
            int length ;
            while ((length = in.read(buffer))!=-1){
                String value = new String(buffer, 0,length,StandardCharsets.UTF_8);
                sb.append(value);
            }
           System.out.println("read sql from hdfs :");
            System.out.println(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
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
