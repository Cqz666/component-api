package com.cqz.component.common.netty.client;

import com.cqz.component.common.netty.handler.EchoClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

public class EchoClient {
    private final String host;
    private final int port;

    public EchoClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            //1 创建 Bootstrap
            Bootstrap b = new Bootstrap();
            //2 34行含义： 指定EventLoopGroup 来处理客户端事件。由于我们使用 NIO 传输，所以用到了 NioEventLoopGroup 的实现
            b.group(group)
                    .channel(NioSocketChannel.class)            //3 使用的 channel 类型是一个用于 NIO 传输
                    .remoteAddress(new InetSocketAddress(host, port))    //4 设置服务器的 InetSocketAddress
                    //5 当建立一个连接和一个新的通道时，创建添加到 EchoClientHandler 实例 到 channel pipeline
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch)
                                throws Exception {
                            ch.pipeline().addLast(
                                    new EchoClientHandler());
                        }
                    });

            ChannelFuture f = b.connect().sync();        //6 连接到远程;等待连接完成

            f.channel().closeFuture().sync();            //7 阻塞直到 Channel 关闭
        } finally {
            group.shutdownGracefully().sync();            //8 调用 shutdownGracefully() 来关闭线程池和释放所有资源
        }
    }

    public static void main(String[] args) throws Exception {
        new EchoClient("127.0.0.1", 8080).start();
    }
}
