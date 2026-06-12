package com.trading.orderreliability.order.adapter.out.messaging;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "order-service.messaging.outbox")
public class OutboxPublisherProperties {

    private boolean enabled;
    private int batchSize = 50;
    private Duration lockTtl = Duration.ofSeconds(30);
    private Duration publishTimeout = Duration.ofSeconds(10);
    private Duration initialRetryDelay = Duration.ofSeconds(1);
    private Duration maxRetryDelay = Duration.ofMinutes(5);
    private double retryBackoffMultiplier = 2.0;
    private int maxRetryCount = 12;
    private long pollDelayMs = 1000;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Duration getLockTtl() {
        return lockTtl;
    }

    public void setLockTtl(Duration lockTtl) {
        this.lockTtl = lockTtl;
    }

    public Duration getPublishTimeout() {
        return publishTimeout;
    }

    public void setPublishTimeout(Duration publishTimeout) {
        this.publishTimeout = publishTimeout;
    }

    public Duration getInitialRetryDelay() {
        return initialRetryDelay;
    }

    public void setInitialRetryDelay(Duration initialRetryDelay) {
        this.initialRetryDelay = initialRetryDelay;
    }

    public Duration getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public void setMaxRetryDelay(Duration maxRetryDelay) {
        this.maxRetryDelay = maxRetryDelay;
    }

    public double getRetryBackoffMultiplier() {
        return retryBackoffMultiplier;
    }

    public void setRetryBackoffMultiplier(double retryBackoffMultiplier) {
        this.retryBackoffMultiplier = retryBackoffMultiplier;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    public long getPollDelayMs() {
        return pollDelayMs;
    }

    public void setPollDelayMs(long pollDelayMs) {
        this.pollDelayMs = pollDelayMs;
    }

    Duration nextRetryDelay(int nextRetryCount) {
        double multiplier = Math.pow(retryBackoffMultiplier, Math.max(nextRetryCount - 1, 0));
        long delayMillis = (long) (initialRetryDelay.toMillis() * multiplier);
        return Duration.ofMillis(Math.min(delayMillis, maxRetryDelay.toMillis()));
    }
}
