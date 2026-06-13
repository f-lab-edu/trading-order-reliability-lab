package com.trading.orderreliability.broker.protocol;

import com.trading.orderreliability.broker.protocol.BrokerMessages.CancelAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.CancelRejected;
import com.trading.orderreliability.broker.protocol.BrokerMessages.CancelRequest;
import com.trading.orderreliability.broker.protocol.BrokerMessages.Fill;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderExpired;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRejected;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRequest;
import com.trading.orderreliability.broker.protocol.BrokerMessages.StatusQuery;
import com.trading.orderreliability.broker.protocol.BrokerMessages.StatusSnapshot;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class BrokerFrameCodec {

    private static final DateTimeFormatter TS17_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
    private static final ZoneOffset UTC = ZoneOffset.UTC;

    public byte[] encode(BrokerMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        BrokerMessageId expectedMessageId = expectedMessageId(message);
        validateHeader(message.header(), expectedMessageId);

        String body = encodeBody(message);
        String commonHeader = encodeCommonHeader(message.header());
        String payload = commonHeader + body;
        if (payload.length() != BrokerProtocolModule.COMMON_HEADER_LENGTH + expectedMessageId.bodyLength()) {
            throw new IllegalArgumentException("encoded payload length does not match message layout");
        }

        String lengthHeader = formatNumber(payload.length(), BrokerProtocolModule.LENGTH_HEADER_LENGTH);
        return (lengthHeader + payload).getBytes(StandardCharsets.US_ASCII);
    }

    public BrokerParseResult decode(byte[] frame) {
        try {
            return decodeOrThrow(frame);
        } catch (MalformedMessage malformed) {
            return new BrokerParseResult.Malformed(malformed.type, malformed.getMessage());
        }
    }

    private BrokerParseResult.Success decodeOrThrow(byte[] frame) {
        if (frame == null || frame.length < BrokerProtocolModule.LENGTH_HEADER_LENGTH) {
            throw malformed(BrokerMalformedType.FRAME, "length header is shorter than 8 bytes");
        }

        String lengthText = ascii(frame, 0, BrokerProtocolModule.LENGTH_HEADER_LENGTH);
        int frameLength = parseInt(lengthText, BrokerMalformedType.FRAME, "length header must be numeric");
        if (frameLength < BrokerProtocolModule.COMMON_HEADER_LENGTH) {
            throw malformed(BrokerMalformedType.FRAME, "frame length is shorter than common header");
        }
        if (frame.length != BrokerProtocolModule.LENGTH_HEADER_LENGTH + frameLength) {
            throw malformed(BrokerMalformedType.FRAME, "actual frame byte count does not match length header");
        }

        String payload = ascii(frame, BrokerProtocolModule.LENGTH_HEADER_LENGTH, frameLength);
        String messageIdText = slice(payload, 0, 4);
        BrokerMessageId messageId = BrokerMessageId.findByCode(messageIdText)
                .orElseThrow(() -> malformed(BrokerMalformedType.HEADER, "unknown msgId: " + messageIdText));
        int protocolVersion = parseInt(slice(payload, 4, 2), BrokerMalformedType.HEADER, "protocolVersion must be numeric");
        int bodyLength = parseInt(slice(payload, 6, 5), BrokerMalformedType.HEADER, "bodyLength must be numeric");

        if (frameLength != BrokerProtocolModule.COMMON_HEADER_LENGTH + bodyLength) {
            throw malformed(BrokerMalformedType.FRAME, "frame length does not equal common header plus bodyLength");
        }
        if (protocolVersion != BrokerProtocolModule.PROTOCOL_VERSION) {
            throw malformed(BrokerMalformedType.HEADER, "unsupported protocolVersion: " + protocolVersion);
        }
        if (bodyLength != messageId.bodyLength()) {
            throw malformed(BrokerMalformedType.HEADER, "bodyLength does not match msgId layout");
        }

        String wireMessageId = parseAlpha(slice(payload, 11, 64), true, BrokerMalformedType.HEADER, "wireMessageId");
        UUID orderId = parseUuid(slice(payload, 75, 36), BrokerMalformedType.HEADER, "orderId");
        String traceId = parseAlpha(slice(payload, 111, 64), false, BrokerMalformedType.HEADER, "traceId");
        Instant sentAtUtc = parseTimestamp(slice(payload, 175, 17), BrokerMalformedType.HEADER, "sentAtUtc");
        BrokerCommonHeader header = new BrokerCommonHeader(
                messageId,
                protocolVersion,
                bodyLength,
                wireMessageId,
                orderId,
                traceId,
                sentAtUtc
        );

        String body = slice(payload, BrokerProtocolModule.COMMON_HEADER_LENGTH, bodyLength);
        BrokerMessage message = parseBody(header, body);
        return new BrokerParseResult.Success(
                message,
                sha256(payload.getBytes(StandardCharsets.US_ASCII)),
                detectBusinessAnomalies(message)
        );
    }

    private static String encodeCommonHeader(BrokerCommonHeader header) {
        StringBuilder builder = new StringBuilder(BrokerProtocolModule.COMMON_HEADER_LENGTH);
        appendAlpha(builder, header.messageId().code(), 4, true, "msgId");
        appendNumber(builder, header.protocolVersion(), 2, "protocolVersion");
        appendNumber(builder, header.bodyLength(), 5, "bodyLength");
        appendAlpha(builder, header.wireMessageId(), 64, true, "wireMessageId");
        appendUuid(builder, header.orderId(), "orderId");
        appendAlpha(builder, header.traceId(), 64, false, "traceId");
        appendTimestamp(builder, header.sentAtUtc(), "sentAtUtc");
        if (builder.length() != BrokerProtocolModule.COMMON_HEADER_LENGTH) {
            throw new IllegalArgumentException("common header length must be 192 bytes");
        }
        return builder.toString();
    }

    private static String encodeBody(BrokerMessage message) {
        StringBuilder builder = new StringBuilder(message.messageId().bodyLength());
        if (message instanceof OrderRequest orderRequest) {
            appendAlpha(builder, orderRequest.accountId(), 32, true, "accountId");
            appendAlpha(builder, orderRequest.market(), 2, true, "market");
            appendAlpha(builder, orderRequest.symbol(), 16, true, "symbol");
            appendAlpha(builder, orderRequest.side(), 1, true, "side");
            appendAlpha(builder, orderRequest.orderType(), 1, true, "orderType");
            appendAlpha(builder, orderRequest.tif(), 3, true, "tif");
            appendNumber(builder, orderRequest.orderQty(), 18, "orderQty");
            appendPrice(builder, orderRequest.limitPrice(), 18, "limitPrice");
        } else if (message instanceof OrderAccepted accepted) {
            appendAlpha(builder, accepted.brokerOrderId(), 64, true, "brokerOrderId");
            appendTimestamp(builder, accepted.acceptedAtUtc(), "acceptedAtUtc");
        } else if (message instanceof OrderRejected rejected) {
            appendAlpha(builder, rejected.rejectCode(), 16, true, "rejectCode");
            appendAlpha(builder, rejected.rejectReason(), 80, true, "rejectReason");
        } else if (message instanceof Fill fill) {
            appendAlpha(builder, fill.brokerOrderId(), 64, true, "brokerOrderId");
            appendAlpha(builder, fill.executionId(), 64, true, "executionId");
            appendAlpha(builder, fill.fillStatus(), 1, true, "fillStatus");
            appendNumber(builder, fill.lastFillQty(), 18, "lastFillQty");
            appendNumber(builder, fill.cumQty(), 18, "cumQty");
            appendNumber(builder, fill.leavesQty(), 18, "leavesQty");
            appendTimestamp(builder, fill.filledAtUtc(), "filledAtUtc");
        } else if (message instanceof CancelRequest cancelRequest) {
            appendAlpha(builder, cancelRequest.brokerOrderId(), 64, false, "brokerOrderId");
        } else if (message instanceof CancelAccepted cancelAccepted) {
            appendAlpha(builder, cancelAccepted.brokerOrderId(), 64, true, "brokerOrderId");
            appendTimestamp(builder, cancelAccepted.canceledAtUtc(), "canceledAtUtc");
        } else if (message instanceof CancelRejected cancelRejected) {
            appendAlpha(builder, cancelRejected.brokerOrderId(), 64, true, "brokerOrderId");
            appendAlpha(builder, cancelRejected.rejectCode(), 16, true, "rejectCode");
            appendAlpha(builder, cancelRejected.rejectReason(), 80, true, "rejectReason");
        } else if (message instanceof OrderExpired expired) {
            appendAlpha(builder, expired.brokerOrderId(), 64, true, "brokerOrderId");
            appendNumber(builder, expired.cumQty(), 18, "cumQty");
            appendNumber(builder, expired.leavesQty(), 18, "leavesQty");
            appendTimestamp(builder, expired.expiredAtUtc(), "expiredAtUtc");
        } else if (message instanceof StatusQuery statusQuery) {
            appendUuid(builder, statusQuery.jobId(), "jobId");
            appendUuid(builder, statusQuery.attemptId(), "attemptId");
            appendAlpha(builder, statusQuery.brokerOrderId(), 64, false, "brokerOrderId");
            appendAlpha(builder, statusQuery.triggerType(), 32, true, "triggerType");
        } else if (message instanceof StatusSnapshot snapshot) {
            appendUuid(builder, snapshot.jobId(), "jobId");
            appendUuid(builder, snapshot.attemptId(), "attemptId");
            appendAlpha(builder, snapshot.brokerOrderId(), 64, false, "brokerOrderId");
            appendAlpha(builder, snapshot.snapshotStatus(), 12, true, "snapshotStatus");
            appendNumber(builder, snapshot.cumQty(), 18, "cumQty");
            appendNumber(builder, snapshot.leavesQty(), 18, "leavesQty");
            appendAlpha(builder, snapshot.rejectCode(), 16, false, "rejectCode");
            appendTimestamp(builder, snapshot.snapshotAtUtc(), "snapshotAtUtc");
        } else {
            throw new IllegalArgumentException("unsupported broker message: " + message.getClass().getName());
        }
        if (builder.length() != message.messageId().bodyLength()) {
            throw new IllegalArgumentException("encoded body length does not match msgId layout");
        }
        return builder.toString();
    }

    private static BrokerMessage parseBody(BrokerCommonHeader header, String body) {
        return switch (header.messageId()) {
            case ORDR -> parseOrderRequest(header, body);
            case ACKN -> parseOrderAccepted(header, body);
            case RJCT -> parseOrderRejected(header, body);
            case FILL -> parseFill(header, body);
            case CXLQ -> parseCancelRequest(header, body);
            case CXLA -> parseCancelAccepted(header, body);
            case CXLR -> parseCancelRejected(header, body);
            case EXPR -> parseExpired(header, body);
            case OSTQ -> parseStatusQuery(header, body);
            case OSTS -> parseStatusSnapshot(header, body);
        };
    }

    private static OrderRequest parseOrderRequest(BrokerCommonHeader header, String body) {
        String market = parseAlpha(slice(body, 32, 2), true, BrokerMalformedType.BODY, "market");
        String side = parseAlpha(slice(body, 50, 1), true, BrokerMalformedType.BODY, "side");
        String orderType = parseAlpha(slice(body, 51, 1), true, BrokerMalformedType.BODY, "orderType");
        String tif = parseAlpha(slice(body, 52, 3), true, BrokerMalformedType.BODY, "tif");
        requireOneOf(market, "market", "US");
        requireOneOf(side, "side", "B", "S");
        requireOneOf(orderType, "orderType", "L");
        requireOneOf(tif, "tif", "DAY");
        return new OrderRequest(
                header,
                parseAlpha(slice(body, 0, 32), true, BrokerMalformedType.BODY, "accountId"),
                market,
                parseAlpha(slice(body, 34, 16), true, BrokerMalformedType.BODY, "symbol"),
                side,
                orderType,
                tif,
                parseLong(slice(body, 55, 18), BrokerMalformedType.BODY, "orderQty"),
                parsePrice(slice(body, 73, 18), "limitPrice")
        );
    }

    private static OrderAccepted parseOrderAccepted(BrokerCommonHeader header, String body) {
        return new OrderAccepted(
                header,
                parseAlpha(slice(body, 0, 64), true, BrokerMalformedType.BODY, "brokerOrderId"),
                parseTimestamp(slice(body, 64, 17), BrokerMalformedType.BODY, "acceptedAtUtc")
        );
    }

    private static OrderRejected parseOrderRejected(BrokerCommonHeader header, String body) {
        return new OrderRejected(
                header,
                parseAlpha(slice(body, 0, 16), true, BrokerMalformedType.BODY, "rejectCode"),
                parseAlpha(slice(body, 16, 80), true, BrokerMalformedType.BODY, "rejectReason")
        );
    }

    private static Fill parseFill(BrokerCommonHeader header, String body) {
        String fillStatus = parseAlpha(slice(body, 128, 1), true, BrokerMalformedType.BODY, "fillStatus");
        requireOneOf(fillStatus, "fillStatus", "P", "F");
        return new Fill(
                header,
                parseAlpha(slice(body, 0, 64), true, BrokerMalformedType.BODY, "brokerOrderId"),
                parseAlpha(slice(body, 64, 64), true, BrokerMalformedType.BODY, "executionId"),
                fillStatus,
                parseLong(slice(body, 129, 18), BrokerMalformedType.BODY, "lastFillQty"),
                parseLong(slice(body, 147, 18), BrokerMalformedType.BODY, "cumQty"),
                parseLong(slice(body, 165, 18), BrokerMalformedType.BODY, "leavesQty"),
                parseTimestamp(slice(body, 183, 17), BrokerMalformedType.BODY, "filledAtUtc")
        );
    }

    private static CancelRequest parseCancelRequest(BrokerCommonHeader header, String body) {
        return new CancelRequest(
                header,
                parseAlpha(slice(body, 0, 64), false, BrokerMalformedType.BODY, "brokerOrderId")
        );
    }

    private static CancelAccepted parseCancelAccepted(BrokerCommonHeader header, String body) {
        return new CancelAccepted(
                header,
                parseAlpha(slice(body, 0, 64), true, BrokerMalformedType.BODY, "brokerOrderId"),
                parseTimestamp(slice(body, 64, 17), BrokerMalformedType.BODY, "canceledAtUtc")
        );
    }

    private static CancelRejected parseCancelRejected(BrokerCommonHeader header, String body) {
        return new CancelRejected(
                header,
                parseAlpha(slice(body, 0, 64), true, BrokerMalformedType.BODY, "brokerOrderId"),
                parseAlpha(slice(body, 64, 16), true, BrokerMalformedType.BODY, "rejectCode"),
                parseAlpha(slice(body, 80, 80), true, BrokerMalformedType.BODY, "rejectReason")
        );
    }

    private static OrderExpired parseExpired(BrokerCommonHeader header, String body) {
        return new OrderExpired(
                header,
                parseAlpha(slice(body, 0, 64), true, BrokerMalformedType.BODY, "brokerOrderId"),
                parseLong(slice(body, 64, 18), BrokerMalformedType.BODY, "cumQty"),
                parseLong(slice(body, 82, 18), BrokerMalformedType.BODY, "leavesQty"),
                parseTimestamp(slice(body, 100, 17), BrokerMalformedType.BODY, "expiredAtUtc")
        );
    }

    private static StatusQuery parseStatusQuery(BrokerCommonHeader header, String body) {
        return new StatusQuery(
                header,
                parseUuid(slice(body, 0, 36), BrokerMalformedType.BODY, "jobId"),
                parseUuid(slice(body, 36, 36), BrokerMalformedType.BODY, "attemptId"),
                parseAlpha(slice(body, 72, 64), false, BrokerMalformedType.BODY, "brokerOrderId"),
                parseAlpha(slice(body, 136, 32), true, BrokerMalformedType.BODY, "triggerType")
        );
    }

    private static StatusSnapshot parseStatusSnapshot(BrokerCommonHeader header, String body) {
        String snapshotStatus = parseAlpha(slice(body, 136, 12), true, BrokerMalformedType.BODY, "snapshotStatus");
        requireOneOf(snapshotStatus, "snapshotStatus", "ACCEPTED", "PARTIAL", "FILLED", "CANCELED", "REJECTED", "EXPIRED", "NOT_FOUND");
        return new StatusSnapshot(
                header,
                parseUuid(slice(body, 0, 36), BrokerMalformedType.BODY, "jobId"),
                parseUuid(slice(body, 36, 36), BrokerMalformedType.BODY, "attemptId"),
                parseAlpha(slice(body, 72, 64), false, BrokerMalformedType.BODY, "brokerOrderId"),
                snapshotStatus,
                parseLong(slice(body, 148, 18), BrokerMalformedType.BODY, "cumQty"),
                parseLong(slice(body, 166, 18), BrokerMalformedType.BODY, "leavesQty"),
                parseAlpha(slice(body, 184, 16), false, BrokerMalformedType.BODY, "rejectCode"),
                parseTimestamp(slice(body, 200, 17), BrokerMalformedType.BODY, "snapshotAtUtc")
        );
    }

    private static List<BrokerBusinessAnomaly> detectBusinessAnomalies(BrokerMessage message) {
        List<BrokerBusinessAnomaly> anomalies = new ArrayList<>();
        if (message instanceof Fill fill) {
            if (fill.cumQty() < fill.lastFillQty()) {
                anomalies.add(BrokerBusinessAnomaly.FILL_CUM_QTY_LESS_THAN_LAST_FILL_QTY);
            }
            if ("P".equals(fill.fillStatus()) && fill.leavesQty() == 0) {
                anomalies.add(BrokerBusinessAnomaly.PARTIAL_FILL_WITH_ZERO_LEAVES_QTY);
            }
        } else if (message instanceof OrderExpired expired && expired.leavesQty() != 0) {
            anomalies.add(BrokerBusinessAnomaly.EXPIRED_WITH_NON_ZERO_LEAVES_QTY);
        } else if (message instanceof StatusSnapshot snapshot
                && "NOT_FOUND".equals(snapshot.snapshotStatus())
                && (snapshot.cumQty() != 0 || snapshot.leavesQty() != 0)) {
            anomalies.add(BrokerBusinessAnomaly.NOT_FOUND_STATUS_WITH_NON_ZERO_QTY);
        }
        return anomalies;
    }

    private static BrokerMessageId expectedMessageId(BrokerMessage message) {
        if (message instanceof OrderRequest) {
            return BrokerMessageId.ORDR;
        }
        if (message instanceof OrderAccepted) {
            return BrokerMessageId.ACKN;
        }
        if (message instanceof OrderRejected) {
            return BrokerMessageId.RJCT;
        }
        if (message instanceof Fill) {
            return BrokerMessageId.FILL;
        }
        if (message instanceof CancelRequest) {
            return BrokerMessageId.CXLQ;
        }
        if (message instanceof CancelAccepted) {
            return BrokerMessageId.CXLA;
        }
        if (message instanceof CancelRejected) {
            return BrokerMessageId.CXLR;
        }
        if (message instanceof OrderExpired) {
            return BrokerMessageId.EXPR;
        }
        if (message instanceof StatusQuery) {
            return BrokerMessageId.OSTQ;
        }
        if (message instanceof StatusSnapshot) {
            return BrokerMessageId.OSTS;
        }
        throw new IllegalArgumentException("unsupported broker message: " + message.getClass().getName());
    }

    private static void validateHeader(BrokerCommonHeader header, BrokerMessageId expectedMessageId) {
        if (header.messageId() != expectedMessageId) {
            throw new IllegalArgumentException("header msgId does not match message type");
        }
        if (header.protocolVersion() != BrokerProtocolModule.PROTOCOL_VERSION) {
            throw new IllegalArgumentException("unsupported protocolVersion: " + header.protocolVersion());
        }
        if (header.bodyLength() != expectedMessageId.bodyLength()) {
            throw new IllegalArgumentException("header bodyLength does not match message type");
        }
    }

    private static String slice(String value, int offset, int length) {
        return value.substring(offset, offset + length);
    }

    private static String ascii(byte[] bytes, int offset, int length) {
        return new String(bytes, offset, length, StandardCharsets.US_ASCII);
    }

    private static String parseAlpha(String value, boolean required, BrokerMalformedType malformedType, String fieldName) {
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current < 0x20 || current > 0x7E) {
                throw malformed(malformedType, fieldName + " contains non-printable ASCII");
            }
        }
        String parsed = trimRight(value);
        if (required && parsed.isBlank()) {
            throw malformed(malformedType, fieldName + " is blank");
        }
        return parsed;
    }

    private static int parseInt(String value, BrokerMalformedType malformedType, String message) {
        if (!isDigits(value)) {
            throw malformed(malformedType, message);
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw malformed(malformedType, message);
        }
    }

    private static long parseLong(String value, BrokerMalformedType malformedType, String fieldName) {
        if (!isDigits(value)) {
            throw malformed(malformedType, fieldName + " must be numeric");
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw malformed(malformedType, fieldName + " is too large");
        }
    }

    private static BigDecimal parsePrice(String value, String fieldName) {
        long unscaled = parseLong(value, BrokerMalformedType.BODY, fieldName);
        return BigDecimal.valueOf(unscaled, 4);
    }

    private static UUID parseUuid(String value, BrokerMalformedType malformedType, String fieldName) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException exception) {
            throw malformed(malformedType, fieldName + " must be UUID36");
        }
    }

    private static Instant parseTimestamp(String value, BrokerMalformedType malformedType, String fieldName) {
        try {
            return LocalDateTime.parse(value, TS17_FORMATTER).toInstant(UTC);
        } catch (DateTimeParseException exception) {
            throw malformed(malformedType, fieldName + " must be TS17 UTC timestamp");
        }
    }

    private static void requireOneOf(String value, String fieldName, String... allowedValues) {
        for (String allowedValue : allowedValues) {
            if (allowedValue.equals(value)) {
                return;
            }
        }
        throw malformed(BrokerMalformedType.BODY, fieldName + " has unsupported value: " + value);
    }

    private static void appendAlpha(StringBuilder builder, String value, int length, boolean required, String fieldName) {
        String normalized = value == null ? "" : value;
        if (required && normalized.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        if (normalized.length() > length) {
            throw new IllegalArgumentException(fieldName + " is longer than " + length + " bytes");
        }
        for (int i = 0; i < normalized.length(); i++) {
            char current = normalized.charAt(i);
            if (current < 0x20 || current > 0x7E) {
                throw new IllegalArgumentException(fieldName + " must be printable ASCII");
            }
        }
        builder.append(normalized);
        builder.repeat(" ", length - normalized.length());
    }

    private static void appendNumber(StringBuilder builder, long value, int length, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must not be negative");
        }
        builder.append(formatNumber(value, length));
    }

    private static String formatNumber(long value, int length) {
        String text = Long.toString(value);
        if (text.length() > length) {
            throw new IllegalArgumentException("numeric value is longer than " + length + " bytes");
        }
        return "0".repeat(length - text.length()) + text;
    }

    private static void appendPrice(StringBuilder builder, BigDecimal value, int length, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        BigDecimal scaled = value.setScale(4, RoundingMode.UNNECESSARY).movePointRight(4);
        appendNumber(builder, scaled.longValueExact(), length, fieldName);
    }

    private static void appendUuid(StringBuilder builder, UUID value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        builder.append(value);
    }

    private static void appendTimestamp(StringBuilder builder, Instant value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        builder.append(TS17_FORMATTER.format(value.atOffset(UTC)));
    }

    private static boolean isDigits(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static String trimRight(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == ' ') {
            end--;
        }
        return value.substring(0, end);
    }

    private static String sha256(byte[] payload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(payload));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static MalformedMessage malformed(BrokerMalformedType type, String message) {
        return new MalformedMessage(type, message);
    }

    private static final class MalformedMessage extends RuntimeException {

        private final BrokerMalformedType type;

        private MalformedMessage(BrokerMalformedType type, String message) {
            super(message);
            this.type = type;
        }
    }
}
