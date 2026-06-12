package com.trading.orderreliability.order.adapter.out.messaging;

import com.trading.orderreliability.common.messaging.MessagingTopics;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@ConditionalOnProperty(prefix = "order-service.messaging.kafka", name = "topic-bootstrap-enabled", havingValue = "true")
class OrderKafkaTopicConfiguration {

    private final OrderMessagingKafkaProperties properties;

    OrderKafkaTopicConfiguration(OrderMessagingKafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    NewTopic brokerCommandTopic() {
        return topic(MessagingTopics.BROKER_COMMAND);
    }

    @Bean
    NewTopic brokerEventTopic() {
        return topic(MessagingTopics.BROKER_EVENT);
    }

    @Bean
    NewTopic recoveryAttemptReportTopic() {
        return topic(MessagingTopics.RECOVERY_ATTEMPT_REPORT);
    }

    @Bean
    NewTopic orderLifecycleTopic() {
        return topic(MessagingTopics.ORDER_LIFECYCLE);
    }

    @Bean
    NewTopic recoveryEventTopic() {
        return topic(MessagingTopics.RECOVERY_EVENT);
    }

    private NewTopic topic(String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(properties.getTopicPartitions())
                .replicas(properties.getTopicReplicationFactor())
                .build();
    }
}
