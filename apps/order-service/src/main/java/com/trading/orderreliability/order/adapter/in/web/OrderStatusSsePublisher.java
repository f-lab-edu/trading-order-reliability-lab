package com.trading.orderreliability.order.adapter.in.web;

import com.trading.orderreliability.order.application.broker.OrderStatusChangedNotification;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Component
public class OrderStatusSsePublisher {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(ignored -> emitters.remove(emitter));
        return emitter;
    }

    public void publish(OrderStatusChangedNotification notification) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("order-status-changed")
                        .id(notification.orderId() + ":" + notification.occurredAt().toEpochMilli())
                        .data(notification));
            } catch (IOException | IllegalStateException e) {
                emitters.remove(emitter);
            }
        }
    }

    void completeAll() {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.complete();
            } finally {
                emitters.remove(emitter);
            }
        }
    }
}
