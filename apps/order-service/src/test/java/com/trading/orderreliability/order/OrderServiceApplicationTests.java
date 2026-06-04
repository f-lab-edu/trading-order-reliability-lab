package com.trading.orderreliability.order;

import com.trading.orderreliability.order.support.MySqlTestContainerSupport;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class OrderServiceApplicationTests extends MySqlTestContainerSupport {

    @Test
    void contextLoads() {
    }
}
