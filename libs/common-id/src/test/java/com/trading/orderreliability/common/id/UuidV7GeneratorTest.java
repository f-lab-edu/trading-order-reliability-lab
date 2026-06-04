package com.trading.orderreliability.common.id;

import static org.assertj.core.api.Assertions.assertThat;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UuidV7GeneratorTest {

    @Test
    @DisplayName("UUID v7은 생성 시점의 epoch millisecond를 상위 48bit timestamp로 보존한다")
    void uuid_v7은_생성_시점의_epoch_millisecond를_timestamp로_보존한다() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-04T00:00:00.123Z"));
        UuidV7Generator generator = new UuidV7Generator(clock, new SecureRandom());

        UUID uuid = generator.generate();

        assertThat(extractTimestampMillis(uuid)).isEqualTo(clock.millis());
    }

    @Test
    @DisplayName("UUID v7은 RFC 9562 버전 7과 RFC 4122 variant 값을 가진다")
    void uuid_v7은_버전_7과_rfc4122_variant를_가진다() {
        UuidV7Generator generator = new UuidV7Generator(
                new MutableClock(Instant.parse("2026-06-04T00:00:00Z")),
                new SecureRandom()
        );

        UUID uuid = generator.generate();

        assertThat(uuid.version()).isEqualTo(7);
        assertThat(uuid.variant()).isEqualTo(2);
    }

    @Test
    @DisplayName("시간이 증가하면서 생성된 UUID v7은 UUID 자연 순서와 BINARY(16) byte 순서에서도 증가한다")
    void 시간이_증가하면서_생성된_uuid_v7은_uuid와_binary16_정렬에서_모두_증가한다() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-04T00:00:00Z"));
        UuidV7Generator generator = new UuidV7Generator(clock, new SecureRandom());

        UUID previous = generator.generate();
        for (int i = 1; i <= 1_000; i++) {
            clock.plusMillis(1);
            UUID current = generator.generate();

            assertThat(current).isGreaterThan(previous);
            assertThat(Arrays.compareUnsigned(UuidBytes.toBytes(current), UuidBytes.toBytes(previous)))
                    .isGreaterThan(0);
            assertThat(extractTimestampMillis(current)).isGreaterThan(extractTimestampMillis(previous));

            previous = current;
        }
    }

    @Test
    @DisplayName("UUID v7은 BINARY(16)으로 저장했다가 읽어도 동일한 UUID로 복원된다")
    void uuid_v7은_binary16으로_저장했다가_읽어도_동일하게_복원된다() {
        UuidV7Generator generator = new UuidV7Generator(
                new MutableClock(Instant.parse("2026-06-04T00:00:00Z")),
                new SecureRandom()
        );
        UUID uuid = generator.generate();

        UUID restored = UuidBytes.fromBytes(UuidBytes.toBytes(uuid));

        assertThat(restored).isEqualTo(uuid);
    }

    private static long extractTimestampMillis(UUID uuid) {
        return (uuid.getMostSignificantBits() >>> 16) & 0x0000_FFFF_FFFF_FFFFL;
    }

    private static class MutableClock extends Clock {

        private Instant instant;

        MutableClock(Instant instant) {
            this.instant = instant;
        }

        void plusMillis(long millis) {
            instant = instant.plusMillis(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return Clock.fixed(instant, zone);
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
