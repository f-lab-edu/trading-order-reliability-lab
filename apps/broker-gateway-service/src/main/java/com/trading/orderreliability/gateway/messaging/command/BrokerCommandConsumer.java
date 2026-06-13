package com.trading.orderreliability.gateway.messaging.command;

import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;

import java.time.Clock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "gateway.messaging.kafka", name = "consumer-enabled", havingValue = "true")
class BrokerCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(BrokerCommandConsumer.class);
    private static final String CONSUMER_NAME = "broker-gateway-command-consumer";

    private final BrokerCommandEnvelopeParser parser;
    private final BrokerCommandService commandService;
    private final GatewayJdbcRepository repository;
    private final UuidV7Generator uuidGenerator;
    private final Clock clock = Clock.systemUTC();

    BrokerCommandConsumer(
            BrokerCommandEnvelopeParser parser,
            BrokerCommandService commandService,
            GatewayJdbcRepository repository,
            UuidV7Generator uuidGenerator
    ) {
        this.parser = parser;
        this.commandService = commandService;
        this.repository = repository;
        this.uuidGenerator = uuidGenerator;
    }

    @KafkaListener(topics = MessagingTopics.BROKER_COMMAND)
    void consume(ConsumerRecord<String, String> record) {
        try {
            BrokerCommandHandlingResult result = commandService.handle(parser.parse(record.value()));
            log.debug("Broker command consumed: key={}, result={}", record.key(), result);
        } catch (Exception e) {
            repository.parkRawMessage(
                    uuidGenerator.generate(),
                    MessagingTopics.BROKER_COMMAND,
                    CONSUMER_NAME,
                    record.value(),
                    "SCHEMA_PARSE_FAILED",
                    e.getMessage(),
                    clock.instant()
            );
        }
    }
}
