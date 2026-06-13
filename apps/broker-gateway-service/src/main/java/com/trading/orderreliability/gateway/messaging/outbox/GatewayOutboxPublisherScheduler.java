package com.trading.orderreliability.gateway.messaging.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "gateway.messaging.outbox", name = "enabled", havingValue = "true")
class GatewayOutboxPublisherScheduler {

    private final GatewayOutboxPublisher publisher;

    GatewayOutboxPublisherScheduler(GatewayOutboxPublisher publisher) {
        this.publisher = publisher;
    }

    @Scheduled(fixedDelayString = "${gateway.messaging.outbox.poll-delay-ms:1000}")
    void publish() {
        publisher.publishAvailable();
    }
}
