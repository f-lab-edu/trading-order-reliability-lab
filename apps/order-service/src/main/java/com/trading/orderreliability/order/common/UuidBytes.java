package com.trading.orderreliability.order.common;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class UuidBytes {

    private UuidBytes() {
    }

    public static byte[] toBytes(UUID value) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
        return buffer.array();
    }

    public static UUID fromBytes(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}
