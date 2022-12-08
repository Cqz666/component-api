package com.cqz.component.common.netty.server;

import com.cqz.component.common.netty.handler.EchoServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

public class EchoServer {

    private final int port;

    public EchoServer(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(); //3创建 EventLoopGroup
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(group)                                //4创建 ServerBootstrap
                    .channel(NioServerSocketChannel.class)        //5指定使用 NIO 的传输 Channel
                    .localAddress(new InetSocketAddress(port))    //6设置 socket 地址使用所选的端口
                    //7添加 EchoServerHandler 到 Channel 的 ChannelPipeline
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch)
                                throws Exception {
                            ch.pipeline().addLast(
                                    new EchoServerHandler());
                        }
                    });

            ChannelFuture f = b.bind().sync();            //8绑定的服务器;sync 等待服务器关闭
            System.out.println(EchoServer.class.getName() + " 服务端开始监听started and listen on " + f.channel().localAddress());
            f.channel().closeFuture().sync();            //9关闭 channel 和 块，直到它被关闭
        } finally {
            group.shutdownGracefully().sync();            //10关闭 EventLoopGroup，释放所有资源。
        }
    }

    public static void main(String[] args) throws Exception {
        new EchoServer(8080).start();
    }

}
