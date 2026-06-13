package com.trading.orderreliability.broker.protocol;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public final class BrokerMessages {

    private BrokerMessages() {
    }

    public record OrderRequest(
            BrokerCommonHeader header,
            String accountId,
            String market,
            String symbol,
            String side,
            String orderType,
            String tif,
            long orderQty,
            BigDecimal limitPrice
    ) implements BrokerMessage {
    }

    public record OrderAccepted(
            BrokerCommonHeader header,
            String brokerOrderId,
            Instant acceptedAtUtc
    ) implements BrokerMessage {
    }

    public record OrderRejected(
            BrokerCommonHeader header,
            String rejectCode,
            String rejectReason
    ) implements BrokerMessage {
    }

    public record Fill(
            BrokerCommonHeader header,
            String brokerOrderId,
            String executionId,
            String fillStatus,
            long lastFillQty,
            long cumQty,
            long leavesQty,
            Instant filledAtUtc
    ) implements BrokerMessage {
    }

    public record CancelRequest(
            BrokerCommonHeader header,
            String brokerOrderId
    ) implements BrokerMessage {
    }

    public record CancelAccepted(
            BrokerCommonHeader header,
            String brokerOrderId,
            Instant canceledAtUtc
    ) implements BrokerMessage {
    }

    public record CancelRejected(
            BrokerCommonHeader header,
            String brokerOrderId,
            String rejectCode,
            String rejectReason
    ) implements BrokerMessage {
    }

    public record OrderExpired(
            BrokerCommonHeader header,
            String brokerOrderId,
            long cumQty,
            long leavesQty,
            Instant expiredAtUtc
    ) implements BrokerMessage {
    }

    public record StatusQuery(
            BrokerCommonHeader header,
            UUID jobId,
            UUID attemptId,
            String brokerOrderId,
            String triggerType
    ) implements BrokerMessage {
    }

    public record StatusSnapshot(
            BrokerCommonHeader header,
            UUID jobId,
            UUID attemptId,
            String brokerOrderId,
            String snapshotStatus,
            long cumQty,
            long leavesQty,
            String rejectCode,
            Instant snapshotAtUtc
    ) implements BrokerMessage {
    }
}
