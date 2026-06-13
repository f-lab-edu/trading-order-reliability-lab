package com.trading.orderreliability.order;

import com.trading.orderreliability.order.support.MySqlTestContainerSupport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@DisplayName("주문 서비스 애플리케이션")
class OrderServiceApplicationTests extends MySqlTestContainerSupport {

    @Test
    @DisplayName("스프링 애플리케이션 컨텍스트가 로딩된다")
    void contextLoads() {
    }
}
