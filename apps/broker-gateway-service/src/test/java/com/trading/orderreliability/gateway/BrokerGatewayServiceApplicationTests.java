package com.trading.orderreliability.gateway;

import com.trading.orderreliability.gateway.support.GatewayMySqlTestContainerSupport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(properties = {
        "gateway.messaging.kafka.consumer-enabled=false",
        "gateway.messaging.outbox.enabled=false",
        "gateway.broker.command-dispatch-enabled=false"
})
@ActiveProfiles("test")
@DisplayName("브로커 게이트웨이 애플리케이션")
class BrokerGatewayServiceApplicationTests extends GatewayMySqlTestContainerSupport {

    @Test
    @DisplayName("스프링 애플리케이션 컨텍스트가 로딩된다")
    void contextLoads() {
    }
}
