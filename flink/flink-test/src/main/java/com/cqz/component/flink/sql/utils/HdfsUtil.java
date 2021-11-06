//package com.cqz.component.flink.sql.utils;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.LocalFileSystem;
//import org.apache.hadoop.fs.Path;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.net.URI;
//
//public class HdfsUtil {
//    public static final Logger log = LoggerFactory.getLogger(HdfsUtil.class);
//    public static final String HADOOP_CONF = "/etc/hadoop/conf";
//
//    public static void main(String[] args) throws Exception {
//        Configuration hadoopConf = getHadoopConf(HADOOP_CONF);
//        FileSystem fileSystem=null;
//        try {
//             fileSystem = getFileSystem(hadoopConf);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        boolean exist = isExist(fileSystem, args[0]);
//
//        System.out.println("--------"+exist);
//
//    }
//
//
//
//
//    /**
//     * 文件或路径是否存在
//     *
//     * @param fs
//     * @param path
//     * @return
//     * @throws IOException
//     */
//    public static boolean isExist(FileSystem fs, String path) throws Exception {
//        log.info("isExist：{}", path);
//        if (fs instanceof LocalFileSystem) {
//            File file = new File(new URI(path));
//            return file.exists();
//        } else {
//            return fs.exists(new Path(path));
//        }
//    }
//
//    public static InputStream openFile(FileSystem fs, String path) throws Exception {
//        if (fs != null) {
//            if (fs instanceof LocalFileSystem) {
//                File file = new File(new URI(path));
//                if (file.exists()) {
//                    log.info("openFile：{}", path);
//                    return new FileInputStream(file);
//                }
//            } else {
//                if (fs.exists(new Path(path))) {
//                    log.info("openFile：{}", path);
//                    return fs.open(new Path(path));
//                }
//            }
//        }
//        return null;
//    }
//
//    /**
//     * 通过配置获取分布式文件对象
//     */
//    public static FileSystem getFileSystem(Configuration hadoopConfig) throws IOException {
//        return FileSystem.newInstance(hadoopConfig);
//    }
//
//    public static void setHadoopUser(String user_name) {
//        System.setProperty("HADOOP_USER_NAME", user_name);
//    }
//
//    /**
//     * 获取配置文件
//     */
//    public static Configuration getHadoopConf(String path) {
//        Configuration hadoopConfig = new Configuration();
//        hadoopConfig.addResource(new Path(path + "core-site.xml"));
//        hadoopConfig.addResource(new Path(path + "hdfs-site.xml"));
//        hadoopConfig.addResource(new Path(path + "mapred-site.xml"));
////        hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        log.info("hadoopConfig：{}", hadoopConfig);
//        return hadoopConfig;
//    }
//
//
//
//    public static void closeFileSystem(FileSystem fs) throws IOException {
//        log.info("closeFileSystem：{}", fs);
//        if (fs != null)
//            fs.close();
//    }
//
//
//}
