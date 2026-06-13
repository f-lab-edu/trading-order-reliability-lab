package com.trading.orderreliability.gateway.config;

import com.trading.orderreliability.common.id.UuidV7Generator;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class GatewayIdConfiguration {

    @Bean
    UuidV7Generator uuidV7Generator() {
        return new UuidV7Generator();
    }
}
