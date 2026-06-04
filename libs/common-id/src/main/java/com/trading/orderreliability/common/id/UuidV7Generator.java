package com.trading.orderreliability.common.id;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.Objects;
import java.util.UUID;

public class UuidV7Generator {

    private static final long TIMESTAMP_MASK = 0x0000_FFFF_FFFF_FFFFL;
    private static final long VERSION_7_BITS = 0x0000_0000_0000_7000L;
    private static final long RAND_A_MASK = 0x0000_0000_0000_0FFFL;
    private static final long RAND_B_MASK = 0x3FFF_FFFF_FFFF_FFFFL;
    private static final long VARIANT_BITS = 0x8000_0000_0000_0000L;

    private final Clock clock;
    private final SecureRandom random;
    private long lastTimestampMillis = -1L;
    private long randomHigh12;
    private long randomLow62;

    public UuidV7Generator() {
        this(Clock.systemUTC(), new SecureRandom());
    }

    public UuidV7Generator(Clock clock, SecureRandom random) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.random = Objects.requireNonNull(random, "random must not be null");
    }

    public synchronized UUID generate() {
        long timestampMillis = clock.millis();
        if (timestampMillis > lastTimestampMillis) {
            lastTimestampMillis = timestampMillis;
            reseedRandom();
        } else {
            incrementRandom();
        }

        long mostSignificantBits = 0L;
        mostSignificantBits |= (lastTimestampMillis & TIMESTAMP_MASK) << 16;
        mostSignificantBits |= VERSION_7_BITS;
        mostSignificantBits |= randomHigh12 & RAND_A_MASK;

        long leastSignificantBits = VARIANT_BITS | (randomLow62 & RAND_B_MASK);

        return new UUID(mostSignificantBits, leastSignificantBits);
    }

    private void reseedRandom() {
        randomHigh12 = random.nextInt(1 << 12);
        randomLow62 = random.nextLong() & RAND_B_MASK;
    }

    private void incrementRandom() {
        if (randomLow62 < RAND_B_MASK) {
            randomLow62++;
            return;
        }

        randomLow62 = 0L;
        if (randomHigh12 < RAND_A_MASK) {
            randomHigh12++;
            return;
        }

        lastTimestampMillis++;
        reseedRandom();
    }
}
