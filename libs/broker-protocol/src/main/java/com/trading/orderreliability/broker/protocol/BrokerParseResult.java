package com.trading.orderreliability.broker.protocol;

import java.util.List;
import java.util.Objects;

public sealed interface BrokerParseResult permits BrokerParseResult.Success, BrokerParseResult.Malformed {

    default boolean isSuccess() {
        return this instanceof Success;
    }

    record Success(
            BrokerMessage message,
            String payloadHash,
            List<BrokerBusinessAnomaly> anomalies
    ) implements BrokerParseResult {

        public Success {
            Objects.requireNonNull(message, "message must not be null");
            Objects.requireNonNull(payloadHash, "payloadHash must not be null");
            anomalies = List.copyOf(anomalies);
        }
    }

    record Malformed(
            BrokerMalformedType malformedType,
            String reason
    ) implements BrokerParseResult {

        public Malformed {
            Objects.requireNonNull(malformedType, "malformedType must not be null");
            Objects.requireNonNull(reason, "reason must not be null");
        }
    }
}
