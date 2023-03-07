package com.cqz.component.common.minio;

import com.jcraft.jsch.*;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class MinIOToRemote {
    public static final String JAR_FILE = "flink-sql-udf-1.0-SNAPSHOT.jar";
    public static void main(String[] args) {
        String host = "cs240";
        String name = "data_platform";
        String prikeyPath = "/home/cqz/IdeaProjects/4399/comm-service/src/main/resources/privatekey/hdp-fat/id_rsa";
        String dstFile = "/home/data_platform/"+JAR_FILE;
        JSch jsch = new JSch();
        Session session = null;
        Channel channel = null;
        ChannelSftp sftp = null;
        InputStream fileStream = null;
        try{
            //添加私钥
            jsch.addIdentity(prikeyPath);
            session=jsch.getSession(name, host, 22);
            //SSH 公钥检查机制 no、ask、yes
            session.setConfig("StrictHostKeyChecking","no");
            session.connect();
            if (session.isConnected()){
                System.out.println("session连接成功");
            }
            // 打开执行sftp的通道
             channel = session.openChannel("sftp");
             sftp = (ChannelSftp) channel;
            channel.connect();
            if (channel.isConnected()){
                System.out.println("sftp通道连接成功");
            }
            //从MinIO获取文件流
             fileStream = retrieveFileStream();
            if (fileStream != null){
                sftp.put(fileStream,dstFile);
                System.out.println("文件上传到远程服务器成功");
            }
        } catch (JSchException ex) {
            ex.printStackTrace();
        } catch (SftpException e) {
            throw new RuntimeException(e);
        } finally {
            sftp.disconnect();
            channel.disconnect();
            session.disconnect();
            try {
                fileStream.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static InputStream retrieveFileStream(){
        InputStream input = null;
        try{
        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint("http://localhost:9000")
                        .credentials("1II0U6RVM4S6H05MMRCH", "f0ZNZO+hF+hsovRuCyw8Tr0d6qJW01Vc+LX87ZvS")
                        .build();
         input = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket("public")
                        .object(JAR_FILE)
                        .build());

        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
            System.out.println("HTTP trace: " + e.httpTrace());
        } catch (IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
        return input;
    }
}
