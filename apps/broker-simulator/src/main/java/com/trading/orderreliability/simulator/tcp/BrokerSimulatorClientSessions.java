package com.trading.orderreliability.simulator.tcp;

import io.netty.channel.Channel;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class BrokerSimulatorClientSessions {

    private final ConcurrentMap<UUID, Channel> channelsByOrderId = new ConcurrentHashMap<>();

    public void register(UUID orderId, Channel channel) {
        channelsByOrderId.put(orderId, channel);
    }

    public Optional<Channel> channelFor(UUID orderId) {
        Channel channel = channelsByOrderId.get(orderId);
        if (channel == null || !channel.isActive()) {
            channelsByOrderId.remove(orderId, channel);
            return Optional.empty();
        }
        return Optional.of(channel);
    }

    public void unregister(Channel channel) {
        channelsByOrderId.entrySet().removeIf(entry -> entry.getValue() == channel);
    }
}
