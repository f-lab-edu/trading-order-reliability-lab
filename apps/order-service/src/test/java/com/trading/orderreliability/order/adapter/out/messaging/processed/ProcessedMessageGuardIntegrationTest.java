package com.trading.orderreliability.order.adapter.out.messaging.processed;

import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.order.support.MySqlTestContainerSupport;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
@DisplayName("processed message guard 통합 흐름")
class ProcessedMessageGuardIntegrationTest extends MySqlTestContainerSupport {

    @Autowired
    private ProcessedMessageGuard processedMessageGuard;

    @Test
    @DisplayName("중복 envelope는 business handler를 한 번만 실행한다")
    void duplicateEnvelopeRunsHandlerOnlyOnce() {
        AtomicInteger handledCount = new AtomicInteger();
        MessageEnvelope<String> envelope = envelope(UUID.randomUUID(), "order-processed-001");

        ProcessedMessageHandlingResult first = processedMessageGuard.runOnce(
                "broker-gateway-command-consumer",
                envelope,
                handledCount::incrementAndGet
        );
        ProcessedMessageHandlingResult second = processedMessageGuard.runOnce(
                "broker-gateway-command-consumer",
                envelope,
                handledCount::incrementAndGet
        );

        assertThat(first).isEqualTo(ProcessedMessageHandlingResult.HANDLED);
        assertThat(second).isEqualTo(ProcessedMessageHandlingResult.DUPLICATE_SKIPPED);
        assertThat(handledCount).hasValue(1);
    }

    @Test
    @DisplayName("handler 실패 시 processed row를 rollback하여 메시지를 재시도할 수 있다")
    void handlerFailureRollsBackProcessedRowSoMessageCanBeRetried() {
        AtomicInteger handledCount = new AtomicInteger();
        MessageEnvelope<String> envelope = envelope(UUID.randomUUID(), "order-processed-002");

        assertThatThrownBy(() -> processedMessageGuard.runOnce(
                "broker-gateway-command-consumer",
                envelope,
                () -> {
                    handledCount.incrementAndGet();
                    throw new IllegalStateException("business failure");
                }
        )).isInstanceOf(IllegalStateException.class);

        ProcessedMessageHandlingResult retry = processedMessageGuard.runOnce(
                "broker-gateway-command-consumer",
                envelope,
                handledCount::incrementAndGet
        );

        assertThat(retry).isEqualTo(ProcessedMessageHandlingResult.HANDLED);
        assertThat(handledCount).hasValue(2);
    }

    @Test
    @DisplayName("동시 중복 envelope도 business handler를 한 번만 실행한다")
    void concurrentDuplicateEnvelopeRunsHandlerOnlyOnce() throws Exception {
        AtomicInteger handledCount = new AtomicInteger();
        MessageEnvelope<String> envelope = envelope(UUID.randomUUID(), "order-processed-concurrent");
        CountDownLatch ready = new CountDownLatch(4);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        try {
            Callable<ProcessedMessageHandlingResult> task = () -> {
                ready.countDown();
                if (!start.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("동시 processed message 테스트 시작 신호를 받지 못했습니다.");
                }
                return processedMessageGuard.runOnce(
                        "broker-gateway-command-consumer",
                        envelope,
                        handledCount::incrementAndGet
                );
            };

            List<Future<ProcessedMessageHandlingResult>> futures = List.of(
                    executorService.submit(task),
                    executorService.submit(task),
                    executorService.submit(task),
                    executorService.submit(task)
            );
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            List<ProcessedMessageHandlingResult> results = futures.stream()
                    .map(ProcessedMessageGuardIntegrationTest::getResult)
                    .toList();

            assertThat(results).containsExactlyInAnyOrder(
                    ProcessedMessageHandlingResult.HANDLED,
                    ProcessedMessageHandlingResult.DUPLICATE_SKIPPED,
                    ProcessedMessageHandlingResult.DUPLICATE_SKIPPED,
                    ProcessedMessageHandlingResult.DUPLICATE_SKIPPED
            );
            assertThat(handledCount).hasValue(1);
        } finally {
            executorService.shutdownNow();
            assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static MessageEnvelope<String> envelope(UUID messageId, String messageKey) {
        return new MessageEnvelope<>(
                messageId,
                MessageTypes.SUBMIT_ORDER_COMMAND,
                messageKey,
                Instant.parse("2026-06-10T00:00:00Z"),
                "trace-processed-message",
                "{}"
        );
    }

    private static ProcessedMessageHandlingResult getResult(Future<ProcessedMessageHandlingResult> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new AssertionError("동시 processed message guard 실행은 예외 없이 결과로 수렴해야 합니다.", e);
        }
    }
}
