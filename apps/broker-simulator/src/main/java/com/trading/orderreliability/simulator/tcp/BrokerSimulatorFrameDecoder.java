package com.trading.orderreliability.simulator.tcp;

import com.trading.orderreliability.broker.protocol.BrokerProtocolModule;
import com.trading.orderreliability.broker.protocol.BrokerMalformedType;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class BrokerSimulatorFrameDecoder extends ByteToMessageDecoder {

    private static final int MAX_FRAME_LENGTH = 8192;

    @Override
    protected void decode(io.netty.channel.ChannelHandlerContext context, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < BrokerProtocolModule.LENGTH_HEADER_LENGTH) {
            return;
        }

        in.markReaderIndex();
        byte[] lengthBytes = new byte[BrokerProtocolModule.LENGTH_HEADER_LENGTH];
        in.getBytes(in.readerIndex(), lengthBytes);
        String lengthText = new String(lengthBytes, StandardCharsets.US_ASCII);
        if (!lengthText.chars().allMatch(Character::isDigit)) {
            in.skipBytes(in.readableBytes());
            out.add(new BrokerSimulatorMalformedFrame(BrokerMalformedType.FRAME, "length header must be numeric"));
            return;
        }

        int frameLength = Integer.parseInt(lengthText);
        if (frameLength < BrokerProtocolModule.COMMON_HEADER_LENGTH || frameLength > MAX_FRAME_LENGTH) {
            in.skipBytes(in.readableBytes());
            out.add(new BrokerSimulatorMalformedFrame(BrokerMalformedType.FRAME, "frame length is outside simulator bounds"));
            return;
        }
        int totalLength = BrokerProtocolModule.LENGTH_HEADER_LENGTH + frameLength;
        if (in.readableBytes() < totalLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] frame = new byte[totalLength];
        in.readBytes(frame);
        out.add(frame);
    }
}
