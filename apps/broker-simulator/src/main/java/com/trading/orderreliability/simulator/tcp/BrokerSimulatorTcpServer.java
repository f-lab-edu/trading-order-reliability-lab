package com.trading.orderreliability.simulator.tcp;

import com.trading.orderreliability.simulator.config.BrokerSimulatorProperties;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;

@Component
public class BrokerSimulatorTcpServer implements SmartLifecycle {

    private final BrokerSimulatorProperties properties;
    private final BrokerSimulatorTcpHandler handler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean running;
    private volatile int port;

    public BrokerSimulatorTcpServer(BrokerSimulatorProperties properties, BrokerSimulatorTcpHandler handler) {
        this.properties = properties;
        this.handler = handler;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        try {
            bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
            workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline()
                                    .addLast(new BrokerSimulatorFrameDecoder())
                                    .addLast(handler);
                        }
                    });

            serverChannel = bootstrap.bind(properties.getTcp().getPort()).syncUninterruptibly().channel();
            port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
            running = true;
        } catch (RuntimeException exception) {
            cleanupResources();
            throw exception;
        }
    }

    @Override
    public void stop() {
        cleanupResources();
        running = false;
    }

    private void cleanupResources() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
            workerGroup = null;
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public int port() {
        return port;
    }
}
