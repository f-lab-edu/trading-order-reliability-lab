package com.trading.orderreliability.simulator.tcp;

import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMalformedType;
import com.trading.orderreliability.broker.protocol.BrokerMessage;
import com.trading.orderreliability.broker.protocol.BrokerMessageId;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRejected;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRequest;
import com.trading.orderreliability.broker.protocol.BrokerMessages.StatusQuery;
import com.trading.orderreliability.broker.protocol.BrokerMessages.StatusSnapshot;
import com.trading.orderreliability.broker.protocol.BrokerParseResult;
import com.trading.orderreliability.simulator.domain.BrokerSimulatorState;
import com.trading.orderreliability.simulator.domain.SimulatorOrder;
import com.trading.orderreliability.simulator.domain.SimulatorOrderStatus;
import com.trading.orderreliability.simulator.domain.SimulatorScenario;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

@Component
@ChannelHandler.Sharable
public class BrokerSimulatorTcpHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger log = LoggerFactory.getLogger(BrokerSimulatorTcpHandler.class);
    private static final String DEFAULT_REJECT_CODE = "MARKET_CLOSED";
    private static final String DEFAULT_REJECT_REASON = "scenario configured to reject order";

    private final BrokerFrameCodec codec = new BrokerFrameCodec();
    private final BrokerSimulatorState state;
    private final BrokerSimulatorClientSessions sessions;

    public BrokerSimulatorTcpHandler(BrokerSimulatorState state, BrokerSimulatorClientSessions sessions) {
        this.state = state;
        this.sessions = sessions;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, Object inbound) {
        if (inbound instanceof BrokerSimulatorMalformedFrame(
            BrokerMalformedType malformedType,
            String reason
        )) {
            log.warn("Broker Simulator received malformed frame: type={}, reason={}",
                malformedType,
                reason);
            context.close();
            return;
        }

        byte[] frame = (byte[]) inbound;
        BrokerParseResult parseResult = codec.decode(frame);
        if (!(parseResult instanceof BrokerParseResult.Success success)) {
            if (parseResult instanceof BrokerParseResult.Malformed malformed) {
                log.warn("Broker Simulator received malformed broker message: type={}, reason={}",
                        malformed.malformedType(),
                        malformed.reason());
            }
            context.close();
            return;
        }

        BrokerMessage message = success.message();
        if (message instanceof OrderRequest orderRequest) {
            handleOrderRequest(context, orderRequest);
        } else if (message instanceof StatusQuery statusQuery) {
            handleStatusQuery(context, statusQuery);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) {
        sessions.unregister(context.channel());
    }

    private void handleOrderRequest(ChannelHandlerContext context, OrderRequest request) {
        if (state.scenario() == SimulatorScenario.REJECT_SUCCESS) {
            SimulatorOrder rejected = state.reject(request);
            OrderRejected response = new OrderRejected(
                    responseHeader(BrokerMessageId.RJCT, request.header()),
                    DEFAULT_REJECT_CODE,
                    DEFAULT_REJECT_REASON
            );
            write(context, response);
            sessions.register(rejected.orderId(), context.channel());
            return;
        }

        SimulatorOrder accepted = state.accept(request);
        OrderAccepted response = new OrderAccepted(
                responseHeader(BrokerMessageId.ACKN, request.header()),
                accepted.brokerOrderId(),
                Instant.now()
        );
        sessions.register(accepted.orderId(), context.channel());
        write(context, response);
    }

    private void handleStatusQuery(ChannelHandlerContext context, StatusQuery query) {
        Optional<SimulatorOrder> order = state.findForStatusQuery(query.header().orderId(), query.brokerOrderId());
        StatusSnapshot response = order
                .map(simulatorOrder -> snapshot(query, simulatorOrder))
                .orElseGet(() -> notFoundSnapshot(query));
        write(context, response);
    }

    private StatusSnapshot snapshot(StatusQuery query, SimulatorOrder order) {
        return new StatusSnapshot(
                responseHeader(BrokerMessageId.OSTS, query.header()),
                query.jobId(),
                query.attemptId(),
                order.brokerOrderId(),
                snapshotStatus(order),
                order.cumQty(),
                order.leavesQty(),
                order.rejectCode(),
                Instant.now()
        );
    }

    private StatusSnapshot notFoundSnapshot(StatusQuery query) {
        return new StatusSnapshot(
                responseHeader(BrokerMessageId.OSTS, query.header()),
                query.jobId(),
                query.attemptId(),
                "",
                "NOT_FOUND",
                0,
                0,
                "",
                Instant.now()
        );
    }

    private String snapshotStatus(SimulatorOrder order) {
        if (order.status() == SimulatorOrderStatus.REJECTED) {
            return "REJECTED";
        }
        return "ACCEPTED";
    }

    private BrokerCommonHeader responseHeader(BrokerMessageId messageId, BrokerCommonHeader requestHeader) {
        return BrokerCommonHeader.of(
                messageId,
                requestHeader.wireMessageId(),
                requestHeader.orderId(),
                traceIdOrGenerated(requestHeader.traceId(), requestHeader.orderId()),
                Instant.now()
        );
    }

    private void write(ChannelHandlerContext context, BrokerMessage message) {
        context.writeAndFlush(Unpooled.wrappedBuffer(codec.encode(message)));
    }

    private static String traceIdOrGenerated(String traceId, java.util.UUID orderId) {
        if (traceId != null && !traceId.isBlank()) {
            return traceId;
        }
        return "trace-simulator-" + orderId;
    }
}
