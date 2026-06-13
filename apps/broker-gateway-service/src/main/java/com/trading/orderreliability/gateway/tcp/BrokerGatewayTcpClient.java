package com.trading.orderreliability.gateway.tcp;

import com.trading.orderreliability.gateway.config.GatewayBrokerProperties;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

@Component
public class BrokerGatewayTcpClient {

    private final GatewayBrokerProperties properties;
    private final BrokerGatewayTcpHandler handler;
    private EventLoopGroup workerGroup;
    private Channel channel;

    public BrokerGatewayTcpClient(GatewayBrokerProperties properties, BrokerGatewayTcpHandler handler) {
        this.properties = properties;
        this.handler = handler;
    }

    public synchronized void send(byte[] frame) {
        ensureConnected();
        channel.writeAndFlush(Unpooled.wrappedBuffer(frame)).syncUninterruptibly();
    }

    private void ensureConnected() {
        if (channel != null && channel.isActive()) {
            return;
        }
        if (workerGroup == null) {
            workerGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        }
        channel = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline()
                                .addLast(new BrokerGatewayFrameDecoder())
                                .addLast(handler);
                    }
                })
                .connect(properties.getHost(), properties.getPort())
                .syncUninterruptibly()
                .channel();
    }

    @PreDestroy
    public synchronized void close() {
        if (channel != null) {
            channel.close().syncUninterruptibly();
            channel = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
            workerGroup = null;
        }
    }
}
