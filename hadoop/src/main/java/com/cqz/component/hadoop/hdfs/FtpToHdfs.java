package com.cqz.component.hadoop.hdfs;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static com.cqz.component.hadoop.hdfs.HdfsTool.getFileSystem;
import static com.cqz.component.hadoop.hdfs.HdfsUtil.getHadoopConf;

public class FtpToHdfs {

    private static final String HADOOP_CONF = "/usr/local/hadoop/etc/hadoop";

    private static final String FTP_Host = "10.20.0.11";
    private static final String FTP_USERNAME = "ftp";
    private static final String FTP_PASSWORD= "ftp11";

    public static void main(String[] args) throws Exception {
        String ftpPath = args[0];
        String fileName = args[1];
        String hdfsPath = args[2];

         FileSystem fileSystem = getHdfsFileSystem();

         OutputStream out = HdfsTool.createFile(fileSystem, hdfsPath);

        retrieveFile(out,ftpPath,fileName);

    }

    public static FileSystem getHdfsFileSystem(){
        Configuration hadoopConf = getHadoopConf(HADOOP_CONF);
        FileSystem fileSystem=null;
        try {
            fileSystem = getFileSystem(hadoopConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    private static boolean retrieveFile(OutputStream out,String ftpFilePath, String fileName){
        FTPClient ftp = new FTPClient();
        boolean flag = false;
        try {
            ftp.setAutodetectUTF8(true);
            ftp.connect(FTP_Host);
            ftp.login(FTP_USERNAME, FTP_PASSWORD);
            // 跳转到文件目录
            ftp.changeWorkingDirectory(ftpFilePath);
            ftp.enterLocalPassiveMode();
            //设置二进制传输，使用BINARY_FILE_TYPE，ASC容易造成文件损坏
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
            // 获取目录下文件集合
            FTPFile[] files = ftp.listFiles(ftpFilePath);
            FTPFile file = Arrays.stream(files).filter(c -> c.getName().equals(fileName)).findFirst().orElse(null);
            // 取得指定文件并下载
            if (file == null) {
                throw new RuntimeException("文件不存在或者文件已被删除");
            } else {
                // 绑定输出流下载文件
                flag = ftp.retrieveFile(file.getName(), out);
                out.flush();
                out.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return flag;
    }

}
