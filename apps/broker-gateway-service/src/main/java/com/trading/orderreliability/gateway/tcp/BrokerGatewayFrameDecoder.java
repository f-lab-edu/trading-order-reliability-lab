package com.trading.orderreliability.gateway.tcp;

import com.trading.orderreliability.broker.protocol.BrokerMalformedType;
import com.trading.orderreliability.broker.protocol.BrokerProtocolModule;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

class BrokerGatewayFrameDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext context, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < BrokerProtocolModule.LENGTH_HEADER_LENGTH) {
            return;
        }
        in.markReaderIndex();
        byte[] lengthBytes = new byte[BrokerProtocolModule.LENGTH_HEADER_LENGTH];
        in.readBytes(lengthBytes);
        String lengthText = new String(lengthBytes, StandardCharsets.US_ASCII);
        int frameLength;
        try {
            frameLength = Integer.parseInt(lengthText);
        } catch (NumberFormatException e) {
            out.add(new BrokerGatewayMalformedFrame(BrokerMalformedType.FRAME, "length header is not numeric", lengthBytes));
            in.skipBytes(in.readableBytes());
            return;
        }
        if (frameLength < BrokerProtocolModule.COMMON_HEADER_LENGTH) {
            out.add(new BrokerGatewayMalformedFrame(BrokerMalformedType.FRAME, "frame length is shorter than common header", lengthBytes));
            in.skipBytes(in.readableBytes());
            return;
        }
        if (in.readableBytes() < frameLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] payload = new byte[frameLength];
        in.readBytes(payload);
        byte[] frame = new byte[lengthBytes.length + payload.length];
        System.arraycopy(lengthBytes, 0, frame, 0, lengthBytes.length);
        System.arraycopy(payload, 0, frame, lengthBytes.length, payload.length);
        out.add(frame);
    }
}
