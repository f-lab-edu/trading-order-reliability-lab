package com.trading.orderreliability.simulator.domain;

import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMessageId;
import com.trading.orderreliability.broker.protocol.BrokerMessages.Fill;
import com.trading.orderreliability.simulator.tcp.BrokerSimulatorClientSessions;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.UUID;

@Service
public class BrokerSimulatorEventPublisher {

    private final BrokerFrameCodec codec = new BrokerFrameCodec();
    private final BrokerSimulatorState state;
    private final BrokerSimulatorClientSessions sessions;

    public BrokerSimulatorEventPublisher(BrokerSimulatorState state, BrokerSimulatorClientSessions sessions) {
        this.state = state;
        this.sessions = sessions;
    }

    public DuplicateFillResult sendDuplicateFill(UUID orderId) {
        SimulatorOrder order = state.findByOrderId(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "simulator order not found"));
        if (order.status() != SimulatorOrderStatus.ACCEPTED && order.status() != SimulatorOrderStatus.PARTIALLY_FILLED) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "duplicate fill requires accepted order");
        }
        Channel channel = sessions.channelFor(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.CONFLICT, "order TCP session is not active"));

        String wireMessageId = state.wireMessageIdForLogicalEvent("FILL:%s:DUPLICATE".formatted(orderId));
        Instant eventTime = Instant.now();
        Fill fill = new Fill(
                BrokerCommonHeader.of(BrokerMessageId.FILL, wireMessageId, order.orderId(), order.traceId(), eventTime),
                order.brokerOrderId(),
                "EXEC-DUPLICATE-0001",
                "P",
                1,
                1,
                Math.max(order.leavesQty() - 1, 0),
                eventTime
        );
        byte[] frame = codec.encode(fill);
        channel.writeAndFlush(Unpooled.wrappedBuffer(frame.clone()));
        channel.writeAndFlush(Unpooled.wrappedBuffer(frame.clone()));
        return new DuplicateFillResult(orderId, wireMessageId, 2);
    }

    public FillResult sendFill(UUID orderId, long lastFillQty) {
        SimulatorOrder order;
        try {
            order = state.applyFill(orderId, lastFillQty);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, e.getMessage());
        } catch (IllegalStateException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, e.getMessage());
        }
        Channel channel = sessions.channelFor(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.CONFLICT, "order TCP session is not active"));

        String executionId = "EXEC-%s-%06d".formatted(orderId.toString().substring(0, 8), order.cumQty());
        String wireMessageId = state.wireMessageIdForLogicalEvent("FILL:%s:%s".formatted(orderId, executionId));
        Instant eventTime = Instant.now();
        Fill fill = new Fill(
                BrokerCommonHeader.of(BrokerMessageId.FILL, wireMessageId, order.orderId(), order.traceId(), eventTime),
                order.brokerOrderId(),
                executionId,
                order.status() == SimulatorOrderStatus.FILLED ? "F" : "P",
                lastFillQty,
                order.cumQty(),
                order.leavesQty(),
                eventTime
        );
        channel.writeAndFlush(Unpooled.wrappedBuffer(codec.encode(fill)));
        return new FillResult(orderId, wireMessageId, executionId, lastFillQty, order.cumQty(), order.leavesQty());
    }

    public record DuplicateFillResult(UUID orderId, String wireMessageId, int sentFrames) {
    }

    public record FillResult(
            UUID orderId,
            String wireMessageId,
            String executionId,
            long lastFillQty,
            long cumQty,
            long leavesQty
    ) {
    }
}
