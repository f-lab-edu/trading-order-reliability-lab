package com.trading.orderreliability.common.id;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.Objects;
import java.util.UUID;

public class UuidV7Generator {

    private final Clock clock;
    private final SecureRandom random;

    public UuidV7Generator() {
        this(Clock.systemUTC(), new SecureRandom());
    }

    public UuidV7Generator(Clock clock, SecureRandom random) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.random = Objects.requireNonNull(random, "random must not be null");
    }

    public UUID generate() {
        long timestampMillis = clock.millis();
        byte[] randomBytes = new byte[10];
        random.nextBytes(randomBytes);

        long mostSignificantBits = 0L;
        mostSignificantBits |= (timestampMillis & 0x0000_FFFF_FFFF_FFFFL) << 16;
        mostSignificantBits |= 0x0000_0000_0000_7000L;
        mostSignificantBits |= Byte.toUnsignedLong(randomBytes[0]) << 4;
        mostSignificantBits |= Byte.toUnsignedLong(randomBytes[1]) >>> 4;

        long leastSignificantBits = 0L;
        leastSignificantBits |= (Byte.toUnsignedLong(randomBytes[1]) & 0x0FL) << 60;
        leastSignificantBits |= Byte.toUnsignedLong(randomBytes[2]) << 52;
        leastSignificantBits |= Byte.toUnsignedLong(randomBytes[3]) << 44;
        leastSignificantBits |= Byte.toUnsignedLong(randomBytes[4]) << 36;
        leastSignificantBits |= Byte.toUnsignedLong(randomBytes[5]) << 28;
        leastSignificantBits |= Byte.toUnsignedLong(randomBytes[6]) << 20;
        leastSignificantBits |= Byte.toUnsignedLong(randomBytes[7]) << 12;
        leastSignificantBits |= Byte.toUnsignedLong(randomBytes[8]) << 4;
        leastSignificantBits |= Byte.toUnsignedLong(randomBytes[9]) >>> 4;
        leastSignificantBits &= 0x3FFF_FFFF_FFFF_FFFFL;
        leastSignificantBits |= 0x8000_0000_0000_0000L;

        return new UUID(mostSignificantBits, leastSignificantBits);
    }
}
