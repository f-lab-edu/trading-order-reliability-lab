package com.trading.orderreliability.order.adapter.out.messaging;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "order-service.messaging.outbox", name = "enabled", havingValue = "true")
class OutboxPublisherScheduler {

    private final OutboxPublisher publisher;

    OutboxPublisherScheduler(OutboxPublisher publisher) {
        this.publisher = publisher;
    }

    @Scheduled(fixedDelayString = "${order-service.messaging.outbox.poll-delay-ms:1000}")
    void publishAvailable() {
        publisher.publishAvailable();
    }
}
