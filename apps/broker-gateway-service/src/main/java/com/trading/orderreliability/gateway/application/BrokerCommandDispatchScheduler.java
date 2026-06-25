package com.trading.orderreliability.gateway.application;

import com.trading.orderreliability.gateway.config.GatewayBrokerProperties;
import com.trading.orderreliability.gateway.persistence.GatewayCommandAttemptRecord;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "gateway.broker", name = "command-dispatch-enabled", havingValue = "true")
public class BrokerCommandDispatchScheduler {

    private static final Logger log = LoggerFactory.getLogger(BrokerCommandDispatchScheduler.class);

    private final GatewayJdbcRepository repository;
    private final GatewayBrokerProperties properties;
    private final BrokerCommandDispatcher dispatcher;
    private final Clock clock = Clock.systemUTC();

    public BrokerCommandDispatchScheduler(
            GatewayJdbcRepository repository,
            GatewayBrokerProperties properties,
            BrokerCommandDispatcher dispatcher
    ) {
        this.repository = repository;
        this.properties = properties;
        this.dispatcher = dispatcher;
    }

    @Scheduled(
            fixedDelayString = "${gateway.broker.command-dispatch-poll-delay-ms:250}",
            initialDelayString = "${gateway.broker.command-dispatch-initial-delay-ms:0}"
    )
    public void dispatchCreatedAttempts() {
        parkExpiredSubmitOutcomes();
        dispatchCreatedCancelAttempts();
        Instant now = clock.instant();
        List<GatewayCommandAttemptRecord> attempts = repository.claimCreatedSubmitAttempts(
                properties.getCommandDispatchBatchSize(),
                now,
                now.plusMillis(properties.getCommandAckTimeoutMs())
        );
        for (GatewayCommandAttemptRecord attempt : attempts) {
            try {
                dispatcher.dispatchSubmit(attempt);
            } catch (RuntimeException e) {
                log.warn("Failed to dispatch broker submit attempt: attemptId={}, orderId={}, wireMessageId={}",
                        attempt.id(),
                        attempt.orderId(),
                        attempt.wireMessageId(),
                        e);
            }
        }
    }

    private void dispatchCreatedCancelAttempts() {
        Instant now = clock.instant();
        List<GatewayCommandAttemptRecord> attempts = repository.claimDispatchableCancelAttempts(
                properties.getCommandDispatchBatchSize(),
                now,
                now.plusMillis(properties.getCommandAckTimeoutMs())
        );
        for (GatewayCommandAttemptRecord attempt : attempts) {
            try {
                dispatcher.dispatchCancel(attempt);
            } catch (RuntimeException e) {
                log.warn("Failed to dispatch broker cancel attempt: attemptId={}, orderId={}, wireMessageId={}",
                        attempt.id(),
                        attempt.orderId(),
                        attempt.wireMessageId(),
                        e);
            }
        }
    }

    private void parkExpiredSubmitOutcomes() {
        Instant now = clock.instant();
        List<GatewayCommandAttemptRecord> attempts = repository.findExpiredSubmitOutcomeAttempts(
                now,
                properties.getCommandDispatchBatchSize()
        );
        for (GatewayCommandAttemptRecord attempt : attempts) {
            String message = "Broker submit command outcome is unknown; reconciliation is deferred to a later recovery flow";
            if (repository.markAttemptUnknown(attempt.id(), "SUBMIT_OUTCOME_UNKNOWN", message, now)) {
                repository.parkRawMessage(
                        java.util.UUID.randomUUID(),
                        "broker-command-attempt",
                        "broker-gateway-command-dispatcher",
                        attempt.payloadJson(),
                        "SUBMIT_OUTCOME_UNKNOWN",
                        message,
                        now
                );
            }
        }
    }
}
