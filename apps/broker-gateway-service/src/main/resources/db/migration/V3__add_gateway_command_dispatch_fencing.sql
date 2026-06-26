ALTER TABLE broker_command_attempt
    ADD COLUMN dispatch_token VARCHAR(64) NULL AFTER ack_deadline_at,
    ADD COLUMN dispatch_owner VARCHAR(128) NULL AFTER dispatch_token,
    ADD COLUMN dispatch_locked_until DATETIME(3) NULL AFTER dispatch_owner,
    ADD INDEX idx_broker_command_attempt_dispatch_lock (transport_state, dispatch_locked_until);

ALTER TABLE broker_message_journal
    ADD COLUMN out_msg_id VARCHAR(16)
        GENERATED ALWAYS AS (CASE WHEN direction = 'OUT' THEN msg_id ELSE NULL END) STORED AFTER payload_hash,
    ADD COLUMN out_wire_message_id VARCHAR(64)
        GENERATED ALWAYS AS (CASE WHEN direction = 'OUT' THEN wire_message_id ELSE NULL END) STORED AFTER out_msg_id,
    ADD CONSTRAINT uk_broker_message_journal_out_wire
        UNIQUE (broker_code, out_msg_id, out_wire_message_id);

UPDATE broker_command_attempt
SET dispatch_token = UUID(),
    dispatch_owner = 'migration-m5-5',
    dispatch_locked_until = ack_deadline_at,
    ack_deadline_at = NULL
WHERE transport_state = 'CREATED'
  AND ack_deadline_at IS NOT NULL;
