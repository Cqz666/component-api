package com.cqz.component.hadoop.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HAUtil;

import java.io.IOException;
import java.net.InetSocketAddress;

import static com.cqz.component.hadoop.hdfs.FtpToHdfs.getHdfsFileSystem;

public class ActiveNamenode {

    public static void main(String[] args) throws IOException {
        FileSystem fs = getHdfsFileSystem();
        InetSocketAddress addressOfActive = HAUtil.getAddressOfActive(fs);
        String hostString = addressOfActive.getHostString();
        String hostName = addressOfActive.getHostName();
        int port = addressOfActive.getPort();
        System.out.println("hostString = "+hostString);
        System.out.println("hostName:port = "+hostName+":"+port);

    }
}
