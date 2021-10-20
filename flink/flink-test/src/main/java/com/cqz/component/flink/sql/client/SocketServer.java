package com.cqz.component.flink.sql.client;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer {
    public static void main(String[] args) throws IOException {
        ServerSocket ss = new ServerSocket(6324);
        System.out.println("启动服务器....");
        Socket socket = ss.accept();
        System.out.println("客户端:" + socket.getInetAddress().getLocalHost() + "已连接到服务器");

        BufferedReader br = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println("客户端："+line);
        }

    }
}
