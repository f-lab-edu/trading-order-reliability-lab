CREATE TABLE outbox_message (
    id BINARY(16) NOT NULL,
    aggregate_type VARCHAR(32) NOT NULL,
    aggregate_id BINARY(16) NOT NULL,
    topic_name VARCHAR(128) NOT NULL,
    message_key VARCHAR(128) NOT NULL,
    message_type VARCHAR(64) NOT NULL,
    payload_json JSON NOT NULL,
    headers_json JSON NOT NULL,
    status VARCHAR(16) NOT NULL,
    retry_count INT NOT NULL,
    next_retry_at DATETIME(3) NULL,
    locked_by VARCHAR(64) NULL,
    locked_until DATETIME(3) NULL,
    created_at DATETIME(3) NOT NULL,
    published_at DATETIME(3) NULL,
    last_error VARCHAR(512) NULL,
    PRIMARY KEY (id),
    INDEX idx_outbox_status_retry_created (status, next_retry_at, created_at),
    INDEX idx_outbox_aggregate (aggregate_type, aggregate_id),
    INDEX idx_outbox_locked_until (locked_until)
);

CREATE TABLE processed_message (
    consumer_name VARCHAR(64) NOT NULL,
    message_id BINARY(16) NOT NULL,
    message_type VARCHAR(64) NOT NULL,
    message_key VARCHAR(128) NOT NULL,
    processed_at DATETIME(3) NOT NULL,
    PRIMARY KEY (consumer_name, message_id),
    INDEX idx_processed_message_key_processed_at (message_key, processed_at)
);

CREATE TABLE parked_message (
    id BINARY(16) NOT NULL,
    source_topic VARCHAR(128) NOT NULL,
    consumer_name VARCHAR(64) NOT NULL,
    message_id BINARY(16) NULL,
    message_type VARCHAR(64) NULL,
    message_key VARCHAR(128) NULL,
    trace_id VARCHAR(64) NULL,
    error_code VARCHAR(64) NOT NULL,
    retry_count INT NOT NULL,
    payload_text LONGTEXT NOT NULL,
    error_message VARCHAR(512) NOT NULL,
    failed_at DATETIME(3) NOT NULL,
    parked_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_parked_message_consumer_error_code (consumer_name, error_code, parked_at),
    INDEX idx_parked_message_source_consumer_parked_at (source_topic, consumer_name, parked_at),
    INDEX idx_parked_message_message_id (message_id)
);
