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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("브로커 TCP 프레임 코덱")
class BrokerFrameCodecTest {

    private static final UUID ORDER_ID = UUID.fromString("018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa");
    private static final UUID JOB_ID = UUID.fromString("018f8b7a-4c4e-7b20-9f0e-9dfeb33e92bb");
    private static final UUID ATTEMPT_ID = UUID.fromString("018f8b7a-4c4e-7b20-9f0e-9dfeb33e92cc");
    private static final Instant NOW = Instant.parse("2026-05-13T01:15:30.123Z");

    private final BrokerFrameCodec codec = new BrokerFrameCodec();

    @Test
    @DisplayName("모든 전문 ID는 직렬화 후 파싱해도 동일한 메시지로 복원된다")
    void allMessageIdsRoundTrip() {
        List<BrokerMessage> messages = List.of(
                new OrderRequest(header(BrokerMessageId.ORDR), "ACC-001", "US", "AAPL", "B", "L", "DAY", 100, new BigDecimal("189.5000")),
                new OrderAccepted(header(BrokerMessageId.ACKN), "BRK-ORDER-0001", NOW),
                new OrderRejected(header(BrokerMessageId.RJCT), "INVALID_QTY", "quantity is invalid"),
                new Fill(header(BrokerMessageId.FILL), "BRK-ORDER-0001", "EXEC-0001", "P", 40, 40, 60, NOW),
                new CancelRequest(header(BrokerMessageId.CXLQ), ""),
                new CancelAccepted(header(BrokerMessageId.CXLA), "BRK-ORDER-0001", NOW),
                new CancelRejected(header(BrokerMessageId.CXLR), "BRK-ORDER-0001", "TOO_LATE_CANCEL", "too late to cancel"),
                new OrderExpired(header(BrokerMessageId.EXPR), "BRK-ORDER-0001", 40, 0, NOW),
                new StatusQuery(header(BrokerMessageId.OSTQ), JOB_ID, ATTEMPT_ID, "BRK-ORDER-0001", "MANUAL"),
                new StatusSnapshot(header(BrokerMessageId.OSTS), JOB_ID, ATTEMPT_ID, "BRK-ORDER-0001", "PARTIAL", 40, 60, "", NOW)
        );

        for (BrokerMessage message : messages) {
            BrokerParseResult result = codec.decode(codec.encode(message));

            assertThat(result).isInstanceOf(BrokerParseResult.Success.class);
            BrokerParseResult.Success success = (BrokerParseResult.Success) result;
            assertThat(success.message()).usingRecursiveComparison().isEqualTo(message);
            assertThat(success.payloadHash()).hasSize(64);
        }
    }

    @Test
    @DisplayName("길이 헤더가 숫자가 아니면 frame malformed로 분류한다")
    void malformedFrameLengthIsFrameMalformed() {
        byte[] frame = codec.encode(sampleOrderRequest());
        frame[0] = 'X';

        BrokerParseResult result = codec.decode(frame);

        assertMalformed(result, BrokerMalformedType.FRAME);
    }

    @Test
    @DisplayName("길이 헤더와 실제 전문 길이가 다르면 frame malformed로 분류한다")
    void mismatchedFrameLengthIsFrameMalformed() {
        String frame = new String(codec.encode(sampleOrderRequest()), StandardCharsets.US_ASCII);
        String modified = "00000284" + frame.substring(BrokerProtocolModule.LENGTH_HEADER_LENGTH);

        BrokerParseResult result = codec.decode(modified.getBytes(StandardCharsets.US_ASCII));

        assertMalformed(result, BrokerMalformedType.FRAME);
    }

    @Test
    @DisplayName("알 수 없는 전문 ID는 header malformed로 분류한다")
    void unknownMessageIdIsHeaderMalformed() {
        byte[] frame = codec.encode(sampleOrderRequest());
        frame[8] = 'X';
        frame[9] = 'X';
        frame[10] = 'X';
        frame[11] = 'X';

        BrokerParseResult result = codec.decode(frame);

        assertMalformed(result, BrokerMalformedType.HEADER);
    }

    @Test
    @DisplayName("프레임 길이는 맞지만 body length가 전문 ID와 다르면 header malformed로 분류한다")
    void bodyLengthMismatchWithConsistentFrameLengthIsHeaderMalformed() {
        String frame = new String(codec.encode(sampleOrderRequest()), StandardCharsets.US_ASCII);
        String modified = "00000284"
                + frame.substring(8, 14)
                + "00092"
                + frame.substring(19)
                + " ";

        BrokerParseResult result = codec.decode(modified.getBytes(StandardCharsets.US_ASCII));

        assertMalformed(result, BrokerMalformedType.HEADER);
    }

    @Test
    @DisplayName("body enum 값이 허용 목록 밖이면 body malformed로 분류한다")
    void invalidBodyEnumIsBodyMalformed() {
        byte[] frame = codec.encode(sampleOrderRequest());
        int sideOffset = BrokerProtocolModule.LENGTH_HEADER_LENGTH + BrokerProtocolModule.COMMON_HEADER_LENGTH + 50;
        frame[sideOffset] = 'X';

        BrokerParseResult result = codec.decode(frame);

        assertMalformed(result, BrokerMalformedType.BODY);
    }

    @Test
    @DisplayName("주문 요청 market 값이 US가 아니면 body malformed로 분류한다")
    void invalidMarketIsBodyMalformed() {
        byte[] frame = codec.encode(sampleOrderRequest());
        int marketOffset = BrokerProtocolModule.LENGTH_HEADER_LENGTH + BrokerProtocolModule.COMMON_HEADER_LENGTH + 32;
        frame[marketOffset] = 'K';
        frame[marketOffset + 1] = 'R';

        BrokerParseResult result = codec.decode(frame);

        assertMalformed(result, BrokerMalformedType.BODY);
    }

    @Test
    @DisplayName("byte-level 오류가 아닌 업무 이상은 파싱 성공과 anomaly로 분리한다")
    void businessAnomalyIsParsedAsSuccess() {
        BrokerMessage fill = new Fill(
                header(BrokerMessageId.FILL),
                "BRK-ORDER-0001",
                "EXEC-0001",
                "P",
                100,
                100,
                0,
                NOW
        );

        BrokerParseResult result = codec.decode(codec.encode(fill));

        assertThat(result).isInstanceOf(BrokerParseResult.Success.class);
        BrokerParseResult.Success success = (BrokerParseResult.Success) result;
        assertThat(success.anomalies()).containsExactly(BrokerBusinessAnomaly.PARTIAL_FILL_WITH_ZERO_LEAVES_QTY);
    }

    private static void assertMalformed(BrokerParseResult result, BrokerMalformedType malformedType) {
        assertThat(result).isInstanceOf(BrokerParseResult.Malformed.class);
        BrokerParseResult.Malformed malformed = (BrokerParseResult.Malformed) result;
        assertThat(malformed.malformedType()).isEqualTo(malformedType);
    }

    private static BrokerCommonHeader header(BrokerMessageId messageId) {
        return BrokerCommonHeader.of(messageId, "W-GW-20260513-0001", ORDER_ID, "trace-order-20260513-0001", NOW);
    }

    private static OrderRequest sampleOrderRequest() {
        return new OrderRequest(
                header(BrokerMessageId.ORDR),
                "ACC-001",
                "US",
                "AAPL",
                "B",
                "L",
                "DAY",
                100,
                new BigDecimal("189.5000")
        );
    }
}
