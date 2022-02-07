package com.cqz.component.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HAUtil;

import java.io.IOException;
import java.net.InetSocketAddress;

import static com.cqz.component.hadoop.hdfs.FtpToHdfs.getHdfsFileSystem;
import static com.cqz.component.hadoop.hdfs.HdfsTool.getFileSystem;
import static com.cqz.component.hadoop.hdfs.HdfsUtil.getHadoopConf;

public class ActiveNamenode {

    public static void main(String[] args) throws IOException {
        FileSystem fs = getHdfsFileSystem(args[0]);
        InetSocketAddress addressOfActive = HAUtil.getAddressOfActive(fs);
        String hostString = addressOfActive.getHostString();
        String hostName = addressOfActive.getHostName();
        int port = addressOfActive.getPort();
        System.out.println("host = "+hostString);
        System.out.println("hostName:port = "+hostName+":"+port);

    }

    public static FileSystem getHdfsFileSystem(String conf){
        Configuration hadoopConf = getHadoopConf(conf);
        hadoopConf.set("fs.defaultFS","hdfs://flinkcluster");
        FileSystem fileSystem=null;
        try {
            fileSystem = getFileSystem(hadoopConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }
}
