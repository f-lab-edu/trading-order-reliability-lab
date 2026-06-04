package com.trading.orderreliability.order.adapter.out.persistence;

import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.InstructionType;
import com.trading.orderreliability.order.domain.model.OrderId;
import com.trading.orderreliability.order.domain.model.OrderInstruction;
import com.trading.orderreliability.order.domain.model.OrderInstructionId;
import com.trading.orderreliability.order.domain.model.OrderInstructionStatus;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OrderInstructionRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderInstructionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(OrderInstruction instruction, String payloadJson) {
        jdbcTemplate.update("""
                        INSERT INTO order_instruction (
                            id, order_id, account_id, instruction_type, client_instruction_id,
                            status, retry_count, request_payload_json, request_payload_hash,
                            result_code, result_message, trace_id, created_at, updated_at, resolved_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                UuidBytes.toBytes(instruction.instructionId().value()),
                UuidBytes.toBytes(instruction.orderId().value()),
                instruction.accountId().value(),
                instruction.instructionType().name(),
                instruction.clientInstructionId(),
                instruction.status().name(),
                instruction.retryCount(),
                payloadJson,
                instruction.requestPayloadHash(),
                instruction.resultCode(),
                instruction.resultMessage(),
                instruction.traceId(),
                Timestamp.from(instruction.createdAt()),
                Timestamp.from(instruction.updatedAt()),
                toTimestamp(instruction.resolvedAt())
        );
    }

    public Optional<OrderInstruction> findByIdempotencyKey(String accountId, InstructionType instructionType, String clientInstructionId) {
        List<OrderInstruction> instructions = jdbcTemplate.query("""
                        SELECT *
                        FROM order_instruction
                        WHERE account_id = ? AND instruction_type = ? AND client_instruction_id = ?
                        """,
                this::mapInstruction,
                accountId,
                instructionType.name(),
                clientInstructionId
        );
        return instructions.stream().findFirst();
    }

    public Optional<OrderInstruction> findByOrderAndTypeAndClientInstructionId(
            OrderId orderId,
            InstructionType instructionType,
            String clientInstructionId
    ) {
        List<OrderInstruction> instructions = jdbcTemplate.query("""
                        SELECT *
                        FROM order_instruction
                        WHERE order_id = ? AND instruction_type = ? AND client_instruction_id = ?
                        """,
                this::mapInstruction,
                UuidBytes.toBytes(orderId.value()),
                instructionType.name(),
                clientInstructionId
        );
        return instructions.stream().findFirst();
    }

    public Optional<OrderInstruction> findActiveCancel(OrderId orderId) {
        List<OrderInstruction> instructions = jdbcTemplate.query("""
                        SELECT *
                        FROM order_instruction
                        WHERE order_id = ? AND instruction_type = ? AND status = ?
                        """,
                this::mapInstruction,
                UuidBytes.toBytes(orderId.value()),
                InstructionType.CANCEL.name(),
                OrderInstructionStatus.REQUESTED.name()
        );
        return instructions.stream().findFirst();
    }

    private OrderInstruction mapInstruction(ResultSet rs, int rowNum) throws SQLException {
        return new OrderInstruction(
                new OrderInstructionId(UuidBytes.fromBytes(rs.getBytes("id"))),
                new OrderId(UuidBytes.fromBytes(rs.getBytes("order_id"))),
                new AccountId(rs.getString("account_id")),
                InstructionType.valueOf(rs.getString("instruction_type")),
                rs.getString("client_instruction_id"),
                OrderInstructionStatus.valueOf(rs.getString("status")),
                rs.getInt("retry_count"),
                rs.getString("request_payload_hash"),
                rs.getString("result_code"),
                rs.getString("result_message"),
                rs.getString("trace_id"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(),
                toInstant(rs.getTimestamp("resolved_at"))
        );
    }

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
