package com.trading.orderreliability.order.adapter.out.messaging.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@ConditionalOnProperty(prefix = "order-service.messaging.outbox", name = "enabled", havingValue = "true")
class OrderMessagingSchedulingConfiguration {
}
