package com.trading.orderreliability.order.adapter.out.messaging.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "order-service.messaging.kafka")
public class OrderMessagingKafkaProperties {

    private boolean topicBootstrapEnabled;
    private int topicPartitions = 1;
    private short topicReplicationFactor = 1;

    public boolean isTopicBootstrapEnabled() {
        return topicBootstrapEnabled;
    }

    public void setTopicBootstrapEnabled(boolean topicBootstrapEnabled) {
        this.topicBootstrapEnabled = topicBootstrapEnabled;
    }

    public int getTopicPartitions() {
        return topicPartitions;
    }

    public void setTopicPartitions(int topicPartitions) {
        this.topicPartitions = topicPartitions;
    }

    public short getTopicReplicationFactor() {
        return topicReplicationFactor;
    }

    public void setTopicReplicationFactor(short topicReplicationFactor) {
        this.topicReplicationFactor = topicReplicationFactor;
    }
}
