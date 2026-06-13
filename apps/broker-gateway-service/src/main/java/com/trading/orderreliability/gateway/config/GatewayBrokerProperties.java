package com.trading.orderreliability.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.broker")
public class GatewayBrokerProperties {

    private String code = "SIM";
    private String host = "127.0.0.1";
    private int port = 9093;
    private boolean commandDispatchEnabled;
    private long commandDispatchPollDelayMs = 250;
    private long commandDispatchLeaseTimeoutMs = 30_000;
    private int commandDispatchBatchSize = 25;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isCommandDispatchEnabled() {
        return commandDispatchEnabled;
    }

    public void setCommandDispatchEnabled(boolean commandDispatchEnabled) {
        this.commandDispatchEnabled = commandDispatchEnabled;
    }

    public long getCommandDispatchPollDelayMs() {
        return commandDispatchPollDelayMs;
    }

    public void setCommandDispatchPollDelayMs(long commandDispatchPollDelayMs) {
        this.commandDispatchPollDelayMs = commandDispatchPollDelayMs;
    }

    public long getCommandDispatchLeaseTimeoutMs() {
        return commandDispatchLeaseTimeoutMs;
    }

    public void setCommandDispatchLeaseTimeoutMs(long commandDispatchLeaseTimeoutMs) {
        this.commandDispatchLeaseTimeoutMs = commandDispatchLeaseTimeoutMs;
    }

    public int getCommandDispatchBatchSize() {
        return commandDispatchBatchSize;
    }

    public void setCommandDispatchBatchSize(int commandDispatchBatchSize) {
        this.commandDispatchBatchSize = commandDispatchBatchSize;
    }
}
