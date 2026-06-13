package com.trading.orderreliability.broker.protocol;

import java.util.Arrays;
import java.util.Optional;

public enum BrokerMessageId {
    ORDR("ORDR", 91),
    ACKN("ACKN", 81),
    RJCT("RJCT", 96),
    FILL("FILL", 200),
    CXLQ("CXLQ", 64),
    CXLA("CXLA", 81),
    CXLR("CXLR", 160),
    EXPR("EXPR", 117),
    OSTQ("OSTQ", 168),
    OSTS("OSTS", 217);

    private final String code;
    private final int bodyLength;

    BrokerMessageId(String code, int bodyLength) {
        this.code = code;
        this.bodyLength = bodyLength;
    }

    public String code() {
        return code;
    }

    public int bodyLength() {
        return bodyLength;
    }

    public static Optional<BrokerMessageId> findByCode(String code) {
        return Arrays.stream(values())
                .filter(messageId -> messageId.code.equals(code))
                .findFirst();
    }
}
