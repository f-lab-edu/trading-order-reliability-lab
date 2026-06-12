package com.trading.orderreliability.order.adapter.out.messaging;

import com.trading.orderreliability.common.messaging.MessageEnvelope;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class ProcessedMessageGuard {

    private final JpaProcessedMessageRepository repository;
    private final Clock clock;

    @Autowired
    public ProcessedMessageGuard(JpaProcessedMessageRepository repository) {
        this(repository, Clock.systemUTC());
    }

    ProcessedMessageGuard(JpaProcessedMessageRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    @Transactional
    public ProcessedMessageHandlingResult runOnce(
            String consumerName,
            MessageEnvelope<?> envelope,
            Runnable handler
    ) {
        // 중복이면 insert 결과가 0이므로 handler를 실행하지 않아 business side effect를 막는다.
        int inserted = repository.insertIfAbsent(
                consumerName,
                envelope.messageId(),
                envelope.messageType(),
                envelope.messageKey(),
                clock.instant()
        );
        if (inserted != 1) {
            return ProcessedMessageHandlingResult.DUPLICATE_SKIPPED;
        }
        handler.run();
        return ProcessedMessageHandlingResult.HANDLED;
    }
}
