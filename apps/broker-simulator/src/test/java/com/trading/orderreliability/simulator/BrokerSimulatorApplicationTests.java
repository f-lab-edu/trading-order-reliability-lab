package com.trading.orderreliability.simulator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@DisplayName("브로커 시뮬레이터 애플리케이션")
class BrokerSimulatorApplicationTests {

    @Test
    @DisplayName("스프링 애플리케이션 컨텍스트가 로딩된다")
    void contextLoads() {
    }
}
