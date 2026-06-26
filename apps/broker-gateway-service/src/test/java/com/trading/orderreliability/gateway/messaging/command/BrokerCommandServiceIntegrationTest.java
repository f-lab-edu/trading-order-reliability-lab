package com.trading.orderreliability.gateway.messaging.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.CancelOrderCommandPayload;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.SubmitOrderCommandPayload;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.gateway.application.BrokerCommandDispatcher;
import com.trading.orderreliability.gateway.config.GatewayBrokerProperties;
import com.trading.orderreliability.gateway.persistence.GatewayCommandAttemptRecord;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;
import com.trading.orderreliability.gateway.support.GatewayMySqlTestContainerSupport;
import com.trading.orderreliability.gateway.tcp.BrokerGatewayTcpClient;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.jdbc.Sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "gateway.broker.command-dispatch-enabled=false",
        "gateway.messaging.kafka.consumer-enabled=false",
        "gateway.messaging.outbox.enabled=false"
})
@ActiveProfiles("test")
@Sql(statements = {
        "DELETE FROM outbox_message",
        "DELETE FROM broker_message_journal",
        "DELETE FROM broker_command_attempt",
        "DELETE FROM broker_order_binding",
        "DELETE FROM processed_message",
        "DELETE FROM parked_message"
})
@DisplayName("Broker Gateway command consumer 통합 흐름")
class BrokerCommandServiceIntegrationTest extends GatewayMySqlTestContainerSupport {

    @Autowired
    private BrokerCommandService commandService;

    @Autowired
    private GatewayJdbcRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private UuidV7Generator uuidGenerator;

    @Autowired
    private GatewayBrokerProperties properties;

    @Test
    @DisplayName("SubmitOrderCommand는 processed message와 SUBMIT attempt를 한 번만 기록한다")
    void submitOrderCommandCreatesProcessedMessageAndSubmitAttemptOnce() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = submitEnvelope(orderId, UUID.randomUUID());

        BrokerCommandHandlingResult first = commandService.handle(envelope);
        BrokerCommandHandlingResult second = commandService.handle(envelope);

        assertThat(first).isEqualTo(BrokerCommandHandlingResult.HANDLED);
        assertThat(second).isEqualTo(BrokerCommandHandlingResult.DUPLICATE_SKIPPED);
        List<GatewayCommandAttemptRecord> attempts = repository.findCreatedSubmitAttempts(10);
        assertThat(attempts)
                .extracting(GatewayCommandAttemptRecord::orderId)
                .contains(orderId);
        assertThat(attempts.stream().filter(attempt -> attempt.orderId().equals(orderId))).hasSize(1);
    }

    @Test
    @DisplayName("기존 CREATED attempt의 ack_deadline_at은 dispatch lock으로 backfill되고 ack deadline에서는 제거된다")
    void legacyCreatedAckDeadlineIsBackfilledToDispatchLock() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId, UUID.randomUUID()));
        GatewayCommandAttemptRecord attempt = repository.findCreatedSubmitAttempts(10)
                .stream()
                .filter(candidate -> candidate.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        Instant legacyDeadline = Instant.parse("2026-06-13T01:02:30Z");
        jdbcTemplate.update(
                "UPDATE broker_command_attempt SET ack_deadline_at = ? WHERE id = ?",
                legacyDeadline,
                UuidBytes.toBytes(attempt.id())
        );

        int updated = jdbcTemplate.update(
                """
                        UPDATE broker_command_attempt
                        SET dispatch_token = UUID(),
                            dispatch_owner = 'migration-m5-5',
                            dispatch_locked_until = ack_deadline_at,
                            ack_deadline_at = NULL
                        WHERE transport_state = 'CREATED'
                          AND ack_deadline_at IS NOT NULL
                        """
        );

        assertThat(updated).isEqualTo(1);
        assertThat(repository.findAttemptAckDeadline(attempt.id())).isEmpty();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT dispatch_token FROM broker_command_attempt WHERE id = ?",
                String.class,
                UuidBytes.toBytes(attempt.id())
        )).isNotBlank();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT dispatch_owner FROM broker_command_attempt WHERE id = ?",
                String.class,
                UuidBytes.toBytes(attempt.id())
        )).isEqualTo("migration-m5-5");
        Timestamp dispatchLockedUntil = jdbcTemplate.queryForObject(
                "SELECT dispatch_locked_until FROM broker_command_attempt WHERE id = ?",
                Timestamp.class,
                UuidBytes.toBytes(attempt.id())
        );
        assertThat(dispatchLockedUntil.toInstant()).isEqualTo(legacyDeadline);
    }

    @Test
    @DisplayName("CancelOrderCommand는 processed message와 CANCEL attempt를 한 번만 기록하고 접수 binding 전에는 dispatch 후보가 아니다")
    void cancelOrderCommandCreatesProcessedMessageAndCancelAttemptOnce() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-cancel-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );

        BrokerCommandHandlingResult first = commandService.handle(envelope);
        BrokerCommandHandlingResult second = commandService.handle(envelope);

        assertThat(first).isEqualTo(BrokerCommandHandlingResult.HANDLED);
        assertThat(second).isEqualTo(BrokerCommandHandlingResult.DUPLICATE_SKIPPED);
        assertThat(repository.findCreatedSubmitAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.findDispatchableCancelAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "CREATED")).isEqualTo(1);
        assertThat(repository.countParkedByErrorCode("UNSUPPORTED_COMMAND")).isZero();
    }

    @Test
    @DisplayName("접수 binding이 생긴 CancelOrderCommand만 dispatch 후보로 조회된다")
    void cancelOrderCommandBecomesDispatchableAfterAcceptedBinding() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-cancel-binding-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );

        commandService.handle(envelope);
        assertThat(repository.findDispatchableCancelAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));

        repository.updateBindingAccepted(orderId, "SIM", "BRK-CANCEL-BINDING-001", Instant.parse("2026-06-13T01:01:00Z"));

        assertThat(repository.findDispatchableCancelAttempts(10))
                .filteredOn(attempt -> attempt.orderId().equals(orderId))
                .singleElement()
                .extracting(GatewayCommandAttemptRecord::brokerOrderId)
                .isEqualTo("BRK-CANCEL-BINDING-001");
    }

    @Test
    @DisplayName("claim된 CANCEL attempt는 dispatch lock 전에는 숨겨지고 lock 만료 후 다시 dispatch 후보가 된다")
    void claimedCancelAttemptIsDispatchableAgainAfterDispatchLockExpires() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-cancel-lease-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );

        commandService.handle(envelope);
        repository.updateBindingAccepted(orderId, "SIM", "BRK-CANCEL-LEASE-001", Instant.parse("2026-06-13T01:01:00Z"));

        assertThat(repository.claimDispatchableCancelAttempts(
                10,
                Instant.parse("2026-06-13T01:02:00Z"),
                Instant.parse("2026-06-13T01:02:30Z"),
                "worker-a"
        )).filteredOn(attempt -> attempt.orderId().equals(orderId)).hasSize(1);

        assertThat(repository.findDispatchableCancelAttempts(Instant.parse("2026-06-13T01:02:10Z"), 10))
                .noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.findDispatchableCancelAttempts(Instant.parse("2026-06-13T01:02:31Z"), 10))
                .filteredOn(attempt -> attempt.orderId().equals(orderId))
                .singleElement()
                .extracting(GatewayCommandAttemptRecord::brokerOrderId)
                .isEqualTo("BRK-CANCEL-LEASE-001");
    }

    @Test
    @DisplayName("OUT CXLQ journal이 있는 CANCEL attempt는 deadline 이후에도 재송신 후보가 아니다")
    void cancelAttemptWithOutboundJournalIsNotDispatchableAgainAfterDispatchLockExpires() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-cancel-out-journal-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );

        commandService.handle(envelope);
        repository.updateBindingAccepted(orderId, "SIM", "BRK-CANCEL-JOURNAL-001", Instant.parse("2026-06-13T01:01:00Z"));
        GatewayCommandAttemptRecord attempt = repository.claimDispatchableCancelAttempts(
                10,
                Instant.parse("2026-06-13T01:02:00Z"),
                Instant.parse("2026-06-13T01:02:30Z"),
                "worker-a"
        ).stream()
                .filter(candidate -> candidate.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        assertThat(repository.insertOutboundJournalIfDispatchTokenMatches(
                attempt.id(),
                attempt.dispatchToken(),
                UUID.randomUUID(),
                attempt.brokerCode(),
                "CXLQ",
                attempt.wireMessageId(),
                attempt.traceId(),
                attempt.brokerOrderId(),
                orderId,
                "cxlq".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                objectMapper.createObjectNode().put("brokerOrderId", attempt.brokerOrderId()),
                Instant.parse("2026-06-13T01:02:01Z")
        )).isTrue();

        assertThat(repository.findDispatchableCancelAttempts(Instant.parse("2026-06-13T01:02:31Z"), 10))
                .noneMatch(candidate -> candidate.orderId().equals(orderId));
    }

    @Test
    @DisplayName("OUT journal은 brokerCode와 msgId와 wireMessageId 기준으로 한 번만 저장된다")
    void outboundJournalIsUniqueByBrokerCodeMessageTypeAndWireMessageId() {
        UUID orderId = UUID.randomUUID();
        String brokerCode = "SIM";
        String msgId = "CXLQ";
        String wireMessageId = "WIRE-OUT-UNIQUE-001";

        insertOutboundJournalFixture(
                UUID.randomUUID(),
                brokerCode,
                msgId,
                wireMessageId,
                "trace-out-unique-1",
                "BRK-OUT-UNIQUE-001",
                orderId,
                "cxlq-first".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                Instant.parse("2026-06-13T01:02:01Z")
        );

        assertThatThrownBy(() -> insertOutboundJournalFixture(
                UUID.randomUUID(),
                brokerCode,
                msgId,
                wireMessageId,
                "trace-out-unique-2",
                "BRK-OUT-UNIQUE-001",
                orderId,
                "cxlq-second".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                Instant.parse("2026-06-13T01:02:02Z")
        )).isInstanceOf(DuplicateKeyException.class);

        Integer journalCount = jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM broker_message_journal
                        WHERE broker_code = ?
                          AND direction = 'OUT'
                          AND msg_id = ?
                          AND wire_message_id = ?
                        """,
                Integer.class,
                brokerCode,
                msgId,
                wireMessageId
        );
        assertThat(journalCount).isOne();
    }

    @Test
    @DisplayName("IN journal은 같은 brokerCode와 msgId와 wireMessageId라도 중복 수신 이력을 저장할 수 있다")
    void inboundJournalAllowsDuplicateWireMessageHistory() {
        UUID orderId = UUID.randomUUID();
        String brokerCode = "SIM";
        String msgId = "CXLA";
        String wireMessageId = "WIRE-IN-DUPLICATE-001";

        repository.insertInboundJournal(
                UUID.randomUUID(),
                brokerCode,
                msgId,
                wireMessageId,
                "trace-in-duplicate-1",
                "BRK-IN-DUPLICATE-001",
                orderId,
                "PARSED",
                null,
                null,
                "cxla-first".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                objectMapper.createObjectNode().put("wireMessageId", wireMessageId),
                null,
                Instant.parse("2026-06-13T01:03:01Z")
        );
        repository.insertInboundJournal(
                UUID.randomUUID(),
                brokerCode,
                msgId,
                wireMessageId,
                "trace-in-duplicate-2",
                "BRK-IN-DUPLICATE-001",
                orderId,
                "PARSED",
                null,
                null,
                "cxla-second".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                objectMapper.createObjectNode().put("wireMessageId", wireMessageId),
                null,
                Instant.parse("2026-06-13T01:03:02Z")
        );

        Integer journalCount = jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM broker_message_journal
                        WHERE broker_code = ?
                          AND direction = 'IN'
                          AND msg_id = ?
                          AND wire_message_id = ?
                        """,
                Integer.class,
                brokerCode,
                msgId,
                wireMessageId
        );
        assertThat(journalCount).isEqualTo(2);
    }

    @Test
    @DisplayName("동시에 같은 CANCEL attempt를 claim해도 하나의 worker만 dispatch token을 얻는다")
    void concurrentCancelClaimAllowsOnlyOneDispatchOwner() throws Exception {
        UUID orderId = UUID.randomUUID();
        commandService.handle(cancelEnvelope(orderId, UUID.randomUUID(), "trace-gateway-cancel-concurrent-test"));
        repository.updateBindingAccepted(
                orderId,
                "SIM",
                "BRK-CANCEL-CONCURRENT-001",
                Instant.parse("2026-06-13T01:01:00Z")
        );
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<List<GatewayCommandAttemptRecord>> first = executor.submit(() -> {
                start.await();
                return repository.claimDispatchableCancelAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:00Z"),
                        Instant.parse("2026-06-13T01:02:30Z"),
                        "worker-a"
                );
            });
            Future<List<GatewayCommandAttemptRecord>> second = executor.submit(() -> {
                start.await();
                return repository.claimDispatchableCancelAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:00Z"),
                        Instant.parse("2026-06-13T01:02:30Z"),
                        "worker-b"
                );
            });

            start.countDown();
            List<GatewayCommandAttemptRecord> claimed = new ArrayList<>();
            claimed.addAll(first.get(10, TimeUnit.SECONDS));
            claimed.addAll(second.get(10, TimeUnit.SECONDS));

            assertThat(claimed)
                    .filteredOn(attempt -> attempt.orderId().equals(orderId))
                    .singleElement()
                    .satisfies(attempt -> {
                        assertThat(attempt.dispatchToken()).isNotBlank();
                        assertThat(attempt.dispatchOwner()).isIn("worker-a", "worker-b");
                        assertThat(attempt.brokerOrderId()).isEqualTo("BRK-CANCEL-CONCURRENT-001");
                    });
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("stale owner의 cancel dispatcher는 OUT CXLQ journal과 TCP send를 수행하지 못한다")
    void staleCancelDispatchOwnerCannotReachOutboundJournalOrTcpSend() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(cancelEnvelope(orderId, UUID.randomUUID(), "trace-gateway-cancel-stale-test"));
        repository.updateBindingAccepted(
                orderId,
                "SIM",
                "BRK-CANCEL-STALE-001",
                Instant.parse("2026-06-13T01:01:00Z")
        );
        Instant claimTime = Instant.now();
        GatewayCommandAttemptRecord firstClaim = repository.claimDispatchableCancelAttempts(
                        10,
                        claimTime.minusSeconds(60),
                        claimTime.minusSeconds(30),
                        "worker-a"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        repository.claimDispatchableCancelAttempts(
                        10,
                        claimTime,
                        claimTime.plusSeconds(30),
                        "worker-b"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        RecordingTcpClient tcpClient = new RecordingTcpClient(properties, false);
        BrokerCommandDispatcher dispatcher = new BrokerCommandDispatcher(
                repository,
                tcpClient,
                uuidGenerator,
                objectMapper,
                properties
        );

        assertThatThrownBy(() -> dispatcher.dispatchCancel(firstClaim))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dispatch token");

        assertThat(tcpClient.sendCount()).isZero();
        assertThat(repository.countJournalByOrderId(orderId)).isZero();
    }

    @Test
    @DisplayName("OUT CXLQ journal 직후 token이 정리되면 cancel dispatcher는 TCP send를 중단한다")
    void cancelDispatcherStopsTcpSendWhenDispatchTokenIsClearedAfterOutboundJournal() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(cancelEnvelope(orderId, UUID.randomUUID(), "trace-gateway-cancel-token-clear-test"));
        repository.updateBindingAccepted(
                orderId,
                "SIM",
                "BRK-CANCEL-TOKEN-CLEAR-001",
                Instant.parse("2026-06-13T01:01:00Z")
        );
        Instant claimTime = Instant.now();
        GatewayCommandAttemptRecord attempt = repository.claimDispatchableCancelAttempts(
                        10,
                        claimTime,
                        claimTime.plusSeconds(30),
                        "worker-a"
                )
                .stream()
                .filter(candidate -> candidate.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        TokenClearingRepository fencingRepository = new TokenClearingRepository(
                jdbcTemplate,
                objectMapper,
                attempt.id(),
                "CANCEL_OUTCOME_UNKNOWN"
        );
        RecordingTcpClient tcpClient = new RecordingTcpClient(properties, false);
        BrokerCommandDispatcher dispatcher = new BrokerCommandDispatcher(
                fencingRepository,
                tcpClient,
                uuidGenerator,
                objectMapper,
                properties
        );

        assertThatThrownBy(() -> dispatcher.dispatchCancel(attempt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before TCP send");

        assertThat(tcpClient.sendCount()).isZero();
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "UNKNOWN")).isEqualTo(1);
    }

    @Test
    @DisplayName("OUT CXLQ journal 직후 dispatch lock이 만료되면 cancel dispatcher는 TCP send를 중단한다")
    void cancelDispatcherStopsTcpSendWhenDispatchLockExpiresAfterOutboundJournal() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(cancelEnvelope(orderId, UUID.randomUUID(), "trace-gateway-cancel-lock-expiry-test"));
        repository.updateBindingAccepted(
                orderId,
                "SIM",
                "BRK-CANCEL-LOCK-EXPIRY-001",
                Instant.parse("2026-06-13T01:01:00Z")
        );
        Instant claimTime = Instant.now();
        GatewayCommandAttemptRecord attempt = repository.claimDispatchableCancelAttempts(
                        10,
                        claimTime,
                        claimTime.plusSeconds(30),
                        "worker-a"
                )
                .stream()
                .filter(candidate -> candidate.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        LockExpiringRepository fencingRepository = new LockExpiringRepository(
                jdbcTemplate,
                objectMapper,
                attempt.id()
        );
        RecordingTcpClient tcpClient = new RecordingTcpClient(properties, false);
        BrokerCommandDispatcher dispatcher = new BrokerCommandDispatcher(
                fencingRepository,
                tcpClient,
                uuidGenerator,
                objectMapper,
                properties
        );

        assertThatThrownBy(() -> dispatcher.dispatchCancel(attempt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before TCP send");

        assertThat(tcpClient.sendCount()).isZero();
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "CREATED")).isEqualTo(1);
    }

    @Test
    @DisplayName("OUT CXLQ journal 이후 TCP send 실패는 CANCEL attempt를 FAILED로 단정하지 않는다")
    void tcpSendFailureAfterOutboundCancelJournalDoesNotFailAttempt() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(cancelEnvelope(orderId, UUID.randomUUID(), "trace-gateway-cancel-send-fail-test"));
        repository.updateBindingAccepted(
                orderId,
                "SIM",
                "BRK-CANCEL-SEND-FAIL-001",
                Instant.parse("2026-06-13T01:01:00Z")
        );
        Instant claimTime = Instant.now();
        GatewayCommandAttemptRecord attempt = repository.claimDispatchableCancelAttempts(
                        10,
                        claimTime,
                        claimTime.plusSeconds(30),
                        "worker-a"
                )
                .stream()
                .filter(candidate -> candidate.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        RecordingTcpClient tcpClient = new RecordingTcpClient(properties, true);
        BrokerCommandDispatcher dispatcher = new BrokerCommandDispatcher(
                repository,
                tcpClient,
                uuidGenerator,
                objectMapper,
                properties
        );

        assertThatThrownBy(() -> dispatcher.dispatchCancel(attempt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("simulated send failure");

        assertThat(tcpClient.sendCount()).isEqualTo(1);
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "FAILED")).isZero();
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "CREATED")).isEqualTo(1);
    }

    @Test
    @DisplayName("lock 만료 후 새 owner가 CANCEL attempt 재claim과 OUT CXLQ journal 저장을 완료하면 이전 owner의 OUT CXLQ journal은 실패한다")
    void staleCancelOwnerCannotWriteOutboundJournalAfterReclaimEvenWithRecordedAtBeforeExpiry() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(cancelEnvelope(orderId, UUID.randomUUID(), "trace-gateway-cancel-reclaim-token-test"));
        repository.updateBindingAccepted(
                orderId,
                "SIM",
                "BRK-CANCEL-RECLAIM-TOKEN-001",
                Instant.parse("2026-06-13T01:01:00Z")
        );

        GatewayCommandAttemptRecord firstClaim = repository.claimDispatchableCancelAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:00Z"),
                        Instant.parse("2026-06-13T01:02:30Z"),
                        "worker-a"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        GatewayCommandAttemptRecord secondClaim = repository.claimDispatchableCancelAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:31Z"),
                        Instant.parse("2026-06-13T01:03:01Z"),
                        "worker-b"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();

        assertThat(secondClaim.dispatchToken()).isNotEqualTo(firstClaim.dispatchToken());
        assertThat(repository.insertOutboundJournalIfDispatchTokenMatches(
                secondClaim.id(),
                secondClaim.dispatchToken(),
                UUID.randomUUID(),
                secondClaim.brokerCode(),
                "CXLQ",
                secondClaim.wireMessageId(),
                secondClaim.traceId(),
                secondClaim.brokerOrderId(),
                secondClaim.orderId(),
                "cxlq-current".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                objectMapper.createObjectNode().put("brokerOrderId", secondClaim.brokerOrderId()),
                Instant.parse("2026-06-13T01:02:32Z")
        )).isTrue();
        assertThat(repository.insertOutboundJournalIfDispatchTokenMatches(
                firstClaim.id(),
                firstClaim.dispatchToken(),
                UUID.randomUUID(),
                firstClaim.brokerCode(),
                "CXLQ",
                firstClaim.wireMessageId(),
                firstClaim.traceId(),
                firstClaim.brokerOrderId(),
                firstClaim.orderId(),
                "cxlq-stale".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                objectMapper.createObjectNode().put("brokerOrderId", firstClaim.brokerOrderId()),
                Instant.parse("2026-06-13T01:02:29Z")
        )).isFalse();
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "SENT")).isZero();
    }

    @Test
    @DisplayName("dispatch lock 만료 후 재claim되면 이전 owner의 token으로 SENT 처리할 수 없다")
    void staleDispatchOwnerCannotMarkAttemptSentAfterAnotherOwnerReclaims() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId, UUID.randomUUID()));

        GatewayCommandAttemptRecord firstClaim = repository.claimCreatedSubmitAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:00Z"),
                        Instant.parse("2026-06-13T01:02:30Z"),
                        "worker-a"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();

        assertThat(firstClaim.dispatchToken()).isNotBlank();
        assertThat(firstClaim.dispatchOwner()).isEqualTo("worker-a");
        assertThat(repository.findAttemptAckDeadline(firstClaim.id())).isEmpty();

        GatewayCommandAttemptRecord secondClaim = repository.claimCreatedSubmitAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:31Z"),
                        Instant.parse("2026-06-13T01:03:01Z"),
                        "worker-b"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();

        assertThat(secondClaim.dispatchToken()).isNotEqualTo(firstClaim.dispatchToken());
        assertThat(secondClaim.dispatchOwner()).isEqualTo("worker-b");

        Instant sentAt = Instant.parse("2026-06-13T01:02:32Z");
        assertThat(repository.markAttemptSent(
                firstClaim.id(),
                firstClaim.dispatchToken(),
                sentAt,
                sentAt.plusSeconds(30)
        )).isFalse();
        assertThat(repository.markAttemptSent(
                secondClaim.id(),
                secondClaim.dispatchToken(),
                sentAt,
                sentAt.plusSeconds(30)
        )).isTrue();
    }

    @Test
    @DisplayName("동시에 같은 SUBMIT attempt를 claim해도 하나의 worker만 dispatch token을 얻는다")
    void concurrentSubmitClaimAllowsOnlyOneDispatchOwner() throws Exception {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId, UUID.randomUUID()));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<List<GatewayCommandAttemptRecord>> first = executor.submit(() -> {
                start.await();
                return repository.claimCreatedSubmitAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:00Z"),
                        Instant.parse("2026-06-13T01:02:30Z"),
                        "worker-a"
                );
            });
            Future<List<GatewayCommandAttemptRecord>> second = executor.submit(() -> {
                start.await();
                return repository.claimCreatedSubmitAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:00Z"),
                        Instant.parse("2026-06-13T01:02:30Z"),
                        "worker-b"
                );
            });

            start.countDown();
            List<GatewayCommandAttemptRecord> claimed = new ArrayList<>();
            claimed.addAll(first.get(10, TimeUnit.SECONDS));
            claimed.addAll(second.get(10, TimeUnit.SECONDS));

            assertThat(claimed)
                    .filteredOn(attempt -> attempt.orderId().equals(orderId))
                    .singleElement()
                    .satisfies(attempt -> {
                        assertThat(attempt.dispatchToken()).isNotBlank();
                        assertThat(attempt.dispatchOwner()).isIn("worker-a", "worker-b");
                    });
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("재claim된 attempt는 이전 owner의 token으로 OUT journal이나 FAILED 상태를 남길 수 없다")
    void staleDispatchOwnerCannotWriteOutboundJournalOrFailAttemptAfterAnotherOwnerReclaims() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId, UUID.randomUUID()));

        GatewayCommandAttemptRecord firstClaim = repository.claimCreatedSubmitAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:00Z"),
                        Instant.parse("2026-06-13T01:02:30Z"),
                        "worker-a"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        GatewayCommandAttemptRecord secondClaim = repository.claimCreatedSubmitAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:31Z"),
                        Instant.parse("2026-06-13T01:03:01Z"),
                        "worker-b"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();

        assertThat(repository.insertOutboundJournalIfDispatchTokenMatches(
                firstClaim.id(),
                firstClaim.dispatchToken(),
                UUID.randomUUID(),
                firstClaim.brokerCode(),
                "ORDR",
                firstClaim.wireMessageId(),
                firstClaim.traceId(),
                null,
                firstClaim.orderId(),
                "ordr".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                objectMapper.createObjectNode().put("orderId", orderId.toString()),
                Instant.parse("2026-06-13T01:02:32Z")
        )).isFalse();
        assertThat(repository.markAttemptFailed(
                firstClaim.id(),
                firstClaim.dispatchToken(),
                "TCP_SEND_FAILED",
                "stale failure",
                Instant.parse("2026-06-13T01:02:33Z")
        )).isFalse();
        assertThat(repository.countJournalByOrderId(orderId)).isZero();
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "SUBMIT", "FAILED")).isZero();

        assertThat(repository.insertOutboundJournalIfDispatchTokenMatches(
                secondClaim.id(),
                secondClaim.dispatchToken(),
                UUID.randomUUID(),
                secondClaim.brokerCode(),
                "ORDR",
                secondClaim.wireMessageId(),
                secondClaim.traceId(),
                null,
                secondClaim.orderId(),
                "ordr".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                objectMapper.createObjectNode().put("orderId", orderId.toString()),
                Instant.parse("2026-06-13T01:02:34Z")
        )).isTrue();
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
        String rawMessage = jdbcTemplate.queryForObject(
                "SELECT raw_message FROM broker_message_journal WHERE order_id = ?",
                String.class,
                UuidBytes.toBytes(orderId)
        );
        assertThat(rawMessage).isEqualTo(Base64.getEncoder()
                .encodeToString("ordr".getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
    }

    @Test
    @DisplayName("만료된 dispatch lock의 token은 OUT journal을 남길 수 없다")
    void expiredDispatchLockTokenCannotWriteOutboundJournal() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId, UUID.randomUUID()));

        GatewayCommandAttemptRecord claim = repository.claimCreatedSubmitAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:00Z"),
                        Instant.parse("2026-06-13T01:02:30Z"),
                        "worker-a"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();

        assertThat(repository.insertOutboundJournalIfDispatchTokenMatches(
                claim.id(),
                claim.dispatchToken(),
                UUID.randomUUID(),
                claim.brokerCode(),
                "ORDR",
                claim.wireMessageId(),
                claim.traceId(),
                null,
                claim.orderId(),
                "ordr".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                objectMapper.createObjectNode().put("orderId", orderId.toString()),
                Instant.parse("2026-06-13T01:02:31Z")
        )).isFalse();

        assertThat(repository.countJournalByOrderId(orderId)).isZero();
    }

    @Test
    @DisplayName("stale owner의 dispatcher는 OUT journal과 TCP send를 수행하지 못한다")
    void staleDispatchOwnerCannotReachOutboundJournalOrTcpSend() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId, UUID.randomUUID()));
        Instant claimTime = Instant.now();
        GatewayCommandAttemptRecord firstClaim = repository.claimCreatedSubmitAttempts(
                        10,
                        claimTime.minusSeconds(60),
                        claimTime.minusSeconds(30),
                        "worker-a"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        repository.claimCreatedSubmitAttempts(
                        10,
                        claimTime,
                        claimTime.plusSeconds(30),
                        "worker-b"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        RecordingTcpClient tcpClient = new RecordingTcpClient(properties, false);
        BrokerCommandDispatcher dispatcher = new BrokerCommandDispatcher(
                repository,
                tcpClient,
                uuidGenerator,
                objectMapper,
                properties
        );

        assertThatThrownBy(() -> dispatcher.dispatchSubmit(firstClaim))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dispatch token");

        assertThat(tcpClient.sendCount()).isZero();
        assertThat(repository.countJournalByOrderId(orderId)).isZero();
    }

    @Test
    @DisplayName("OUT journal 직후 token이 정리되면 dispatcher는 TCP send를 중단한다")
    void dispatcherStopsTcpSendWhenDispatchTokenIsClearedAfterOutboundJournal() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId, UUID.randomUUID()));
        Instant claimTime = Instant.now();
        GatewayCommandAttemptRecord attempt = repository.claimCreatedSubmitAttempts(
                        10,
                        claimTime,
                        claimTime.plusSeconds(30),
                        "worker-a"
                )
                .stream()
                .filter(candidate -> candidate.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        TokenClearingRepository fencingRepository = new TokenClearingRepository(
                jdbcTemplate,
                objectMapper,
                attempt.id()
        );
        RecordingTcpClient tcpClient = new RecordingTcpClient(properties, false);
        BrokerCommandDispatcher dispatcher = new BrokerCommandDispatcher(
                fencingRepository,
                tcpClient,
                uuidGenerator,
                objectMapper,
                properties
        );

        assertThatThrownBy(() -> dispatcher.dispatchSubmit(attempt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before TCP send");

        assertThat(tcpClient.sendCount()).isZero();
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "SUBMIT", "UNKNOWN")).isEqualTo(1);
    }

    @Test
    @DisplayName("OUT journal 직후 dispatch lock이 만료되면 dispatcher는 TCP send를 중단한다")
    void dispatcherStopsTcpSendWhenDispatchLockExpiresAfterOutboundJournal() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId, UUID.randomUUID()));
        Instant claimTime = Instant.now();
        GatewayCommandAttemptRecord attempt = repository.claimCreatedSubmitAttempts(
                        10,
                        claimTime,
                        claimTime.plusSeconds(30),
                        "worker-a"
                )
                .stream()
                .filter(candidate -> candidate.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        LockExpiringRepository fencingRepository = new LockExpiringRepository(
                jdbcTemplate,
                objectMapper,
                attempt.id()
        );
        RecordingTcpClient tcpClient = new RecordingTcpClient(properties, false);
        BrokerCommandDispatcher dispatcher = new BrokerCommandDispatcher(
                fencingRepository,
                tcpClient,
                uuidGenerator,
                objectMapper,
                properties
        );

        assertThatThrownBy(() -> dispatcher.dispatchSubmit(attempt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before TCP send");

        assertThat(tcpClient.sendCount()).isZero();
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "SUBMIT", "CREATED")).isEqualTo(1);
    }

    @Test
    @DisplayName("OUT journal 이후 TCP send 실패는 attempt를 FAILED로 단정하지 않는다")
    void tcpSendFailureAfterOutboundJournalDoesNotFailAttempt() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId, UUID.randomUUID()));
        Instant claimTime = Instant.now();
        GatewayCommandAttemptRecord attempt = repository.claimCreatedSubmitAttempts(
                        10,
                        claimTime,
                        claimTime.plusSeconds(30),
                        "worker-a"
                )
                .stream()
                .filter(candidate -> candidate.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        RecordingTcpClient tcpClient = new RecordingTcpClient(properties, true);
        BrokerCommandDispatcher dispatcher = new BrokerCommandDispatcher(
                repository,
                tcpClient,
                uuidGenerator,
                objectMapper,
                properties
        );

        assertThatThrownBy(() -> dispatcher.dispatchSubmit(attempt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("simulated send failure");

        assertThat(tcpClient.sendCount()).isEqualTo(1);
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "SUBMIT", "FAILED")).isZero();
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "SUBMIT", "CREATED")).isEqualTo(1);
    }

    @Test
    @DisplayName("지원하지 않는 command는 TCP로 보내지 않고 parked_message로 격리한다")
    void unsupportedCommandIsParkedWithoutTcpDispatch() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.QUERY_ORDER_STATUS_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-query-test",
                objectMapper.createObjectNode().put("orderId", orderId.toString())
        );

        BrokerCommandHandlingResult result = commandService.handle(envelope);

        assertThat(result).isEqualTo(BrokerCommandHandlingResult.PARKED_UNSUPPORTED);
        assertThat(repository.findCreatedSubmitAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.findDispatchableCancelAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.countParkedByErrorCode("UNSUPPORTED_COMMAND")).isEqualTo(1);
    }

    private MessageEnvelope<JsonNode> submitEnvelope(UUID orderId, UUID messageId) {
        return new MessageEnvelope<>(
                messageId,
                MessageTypes.SUBMIT_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-submit-test",
                objectMapper.valueToTree(new SubmitOrderCommandPayload(
                        orderId,
                        "ACC-GW",
                        "US",
                        "AAPL",
                        "BUY",
                        "LIMIT",
                        "DAY",
                        100,
                        "189.50"
                ))
        );
    }

    private MessageEnvelope<JsonNode> cancelEnvelope(UUID orderId, UUID messageId, String traceId) {
        return new MessageEnvelope<>(
                messageId,
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                traceId,
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );
    }

    private void insertOutboundJournalFixture(
            UUID journalId,
            String brokerCode,
            String msgId,
            String wireMessageId,
            String traceId,
            String brokerOrderId,
            UUID orderId,
            byte[] rawMessage,
            Instant recordedAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO broker_message_journal (
                            id, broker_code, direction, msg_id, wire_message_id, trace_id, broker_order_id,
                            order_id, parse_status, error_code, error_message, raw_message, parsed_payload_json,
                            payload_hash, recorded_at
                        )
                        VALUES (?, ?, 'OUT', ?, ?, ?, ?, ?, 'PARSED', NULL, NULL, ?, JSON_OBJECT(), NULL, ?)
                        """,
                UuidBytes.toBytes(journalId),
                brokerCode,
                msgId,
                wireMessageId,
                traceId,
                brokerOrderId,
                UuidBytes.toBytes(orderId),
                Base64.getEncoder().encodeToString(rawMessage),
                recordedAt
        );
    }

    private static final class TokenClearingRepository extends GatewayJdbcRepository {

        private final UUID attemptIdToClear;
        private final String errorCode;

        private TokenClearingRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, UUID attemptIdToClear) {
            this(jdbcTemplate, objectMapper, attemptIdToClear, "SUBMIT_OUTCOME_UNKNOWN");
        }

        private TokenClearingRepository(
                JdbcTemplate jdbcTemplate,
                ObjectMapper objectMapper,
                UUID attemptIdToClear,
                String errorCode
        ) {
            super(jdbcTemplate, objectMapper);
            this.attemptIdToClear = attemptIdToClear;
            this.errorCode = errorCode;
        }

        @Override
        public boolean insertOutboundJournalIfDispatchTokenMatches(
                UUID attemptId,
                String dispatchToken,
                UUID journalId,
                String brokerCode,
                String msgId,
                String wireMessageId,
                String traceId,
                String brokerOrderId,
                UUID orderId,
                byte[] rawMessage,
                Object parsedPayload,
                Instant recordedAt
        ) {
            boolean inserted = super.insertOutboundJournalIfDispatchTokenMatches(
                    attemptId,
                    dispatchToken,
                    journalId,
                    brokerCode,
                    msgId,
                    wireMessageId,
                    traceId,
                    brokerOrderId,
                    orderId,
                    rawMessage,
                    parsedPayload,
                    recordedAt
            );
            if (inserted && attemptIdToClear.equals(attemptId)) {
                super.markAttemptUnknown(
                        attemptId,
                        errorCode,
                        "simulated race after OUT journal",
                        recordedAt.plusMillis(1)
                );
            }
            return inserted;
        }
    }

    private static final class LockExpiringRepository extends GatewayJdbcRepository {

        private final JdbcTemplate jdbcTemplate;
        private final UUID attemptIdToExpire;

        private LockExpiringRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, UUID attemptIdToExpire) {
            super(jdbcTemplate, objectMapper);
            this.jdbcTemplate = jdbcTemplate;
            this.attemptIdToExpire = attemptIdToExpire;
        }

        @Override
        public boolean insertOutboundJournalIfDispatchTokenMatches(
                UUID attemptId,
                String dispatchToken,
                UUID journalId,
                String brokerCode,
                String msgId,
                String wireMessageId,
                String traceId,
                String brokerOrderId,
                UUID orderId,
                byte[] rawMessage,
                Object parsedPayload,
                Instant recordedAt
        ) {
            boolean inserted = super.insertOutboundJournalIfDispatchTokenMatches(
                    attemptId,
                    dispatchToken,
                    journalId,
                    brokerCode,
                    msgId,
                    wireMessageId,
                    traceId,
                    brokerOrderId,
                    orderId,
                    rawMessage,
                    parsedPayload,
                    recordedAt
            );
            if (inserted && attemptIdToExpire.equals(attemptId)) {
                jdbcTemplate.update(
                        "UPDATE broker_command_attempt SET dispatch_locked_until = ? WHERE id = ?",
                        recordedAt.minusMillis(1),
                        UuidBytes.toBytes(attemptId)
                );
            }
            return inserted;
        }
    }

    private static final class RecordingTcpClient extends BrokerGatewayTcpClient {

        private final boolean fail;
        private int sendCount;

        private RecordingTcpClient(GatewayBrokerProperties properties, boolean fail) {
            super(properties, null);
            this.fail = fail;
        }

        @Override
        public synchronized void send(byte[] frame) {
            sendCount++;
            if (fail) {
                throw new IllegalStateException("simulated send failure");
            }
        }

        int sendCount() {
            return sendCount;
        }
    }
}
