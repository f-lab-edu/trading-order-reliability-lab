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
        parkStaleDispatchingAttempts();
        List<GatewayCommandAttemptRecord> attempts = repository.claimCreatedSubmitAttempts(
                properties.getCommandDispatchBatchSize(),
                clock.instant()
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

    private void parkStaleDispatchingAttempts() {
        Instant now = clock.instant();
        Instant staleBefore = now.minusMillis(properties.getCommandDispatchLeaseTimeoutMs());
        List<GatewayCommandAttemptRecord> attempts = repository.findStaleDispatchingSubmitAttempts(
                staleBefore,
                properties.getCommandDispatchBatchSize()
        );
        for (GatewayCommandAttemptRecord attempt : attempts) {
            String message = "Broker command dispatch outcome is unknown in M4; reconciliation is deferred to M7";
            if (repository.markAttemptUnknown(attempt.id(), "DISPATCH_OUTCOME_UNKNOWN_FOR_M4", message, now)) {
                repository.parkRawMessage(
                        java.util.UUID.randomUUID(),
                        "broker-command-attempt",
                        "broker-gateway-command-dispatcher",
                        attempt.payloadJson(),
                        "DISPATCH_OUTCOME_UNKNOWN_FOR_M4",
                        message,
                        now
                );
            }
        }
    }
}
