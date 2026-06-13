package com.trading.orderreliability.gateway.tcp;

import com.trading.orderreliability.gateway.application.BrokerGatewayInboundService;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@ChannelHandler.Sharable
class BrokerGatewayTcpHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger log = LoggerFactory.getLogger(BrokerGatewayTcpHandler.class);

    private final BrokerGatewayInboundService inboundService;

    BrokerGatewayTcpHandler(BrokerGatewayInboundService inboundService) {
        this.inboundService = inboundService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, Object message) {
        if (message instanceof BrokerGatewayMalformedFrame malformedFrame) {
            inboundService.handleMalformedFrame(malformedFrame.malformedType(), malformedFrame.reason(), malformedFrame.rawBytes());
            context.close();
            return;
        }
        inboundService.handleFrame((byte[]) message);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        log.warn("Broker Gateway TCP handler exception", cause);
        context.close();
    }
}
