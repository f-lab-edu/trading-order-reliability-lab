package com.trading.orderreliability.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@ConfigurationPropertiesScan
@SpringBootApplication
public class BrokerSimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(BrokerSimulatorApplication.class, args);
    }
}
