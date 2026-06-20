-- Controlled message-shard fixture for the searchetl-producer e2e harness.
-- Mirrors the columns the producer reads (id, message_id, message_seq, from_uid,
-- channel_id, channel_type, setting, signal, timestamp, created_at, payload).
--
-- created_at is set well in the past so every row clears the stability lag window
-- immediately (the harness runs the producer with a small lag).

CREATE TABLE IF NOT EXISTS `message` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT,
  `message_id`   VARCHAR(20)  NOT NULL,
  `message_seq`  BIGINT       NOT NULL DEFAULT 0,
  `from_uid`     VARCHAR(64)  NOT NULL DEFAULT '',
  `channel_id`   VARCHAR(128) NOT NULL DEFAULT '',
  `channel_type` TINYINT      NOT NULL DEFAULT 0,
  `setting`      TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `signal`       INT          NOT NULL DEFAULT 0,
  `timestamp`    BIGINT       NOT NULL DEFAULT 0,
  `created_at`   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `payload`      BLOB,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- A normal text message (→ main, body indexed, IK 中文 recall).
INSERT INTO `message` (message_id, message_seq, from_uid, channel_id, channel_type, setting, `signal`, `timestamp`, created_at, payload)
VALUES ('1000000000000000001', 1, 'u_alice', 'g_demo', 2, 0, 0, 1700000000, '2020-01-01 00:00:01',
        '{"type":1,"content":"今天的需求评审会议改到下午三点"}');

-- A valid targeted system message with visibles (→ main, visibles enriched).
INSERT INTO `message` (message_id, message_seq, from_uid, channel_id, channel_type, setting, `signal`, `timestamp`, created_at, payload)
VALUES ('1000000000000000002', 2, 'u_admin', 'g_demo', 2, 0, 0, 1700000001, '2020-01-01 00:00:02',
        '{"type":99,"content":"u_bob 被移出群聊","visibles":["u_admin","u_alice"]}');

-- A Signal-encrypted DM (→ main, raw_excluded, no body).
INSERT INTO `message` (message_id, message_seq, from_uid, channel_id, channel_type, setting, `signal`, `timestamp`, created_at, payload)
VALUES ('1000000000000000003', 3, 'u_carol', 'u_carol@u_dave', 1, 32, 0, 1700000002, '2020-01-01 00:00:03',
        'ENCRYPTED-CIPHERTEXT-NOT-JSON');

-- A non-text media message (→ main, raw_excluded).
INSERT INTO `message` (message_id, message_seq, from_uid, channel_id, channel_type, setting, `signal`, `timestamp`, created_at, payload)
VALUES ('1000000000000000004', 4, 'u_alice', 'g_demo', 2, 0, 0, 1700000003, '2020-01-01 00:00:04',
        '{"type":2,"url":"http://example/x.png"}');

-- A genuine anomaly: non-encrypted but invalid JSON (→ DLQ).
INSERT INTO `message` (message_id, message_seq, from_uid, channel_id, channel_type, setting, `signal`, `timestamp`, created_at, payload)
VALUES ('1000000000000000005', 5, 'u_alice', 'g_demo', 2, 0, 0, 1700000004, '2020-01-01 00:00:05',
        '{not valid json');

-- A fail-closed visibility anomaly: visibles key present but empty (→ DLQ, NEVER main).
INSERT INTO `message` (message_id, message_seq, from_uid, channel_id, channel_type, setting, `signal`, `timestamp`, created_at, payload)
VALUES ('1000000000000000006', 6, 'u_admin', 'g_demo', 2, 0, 0, 1700000005, '2020-01-01 00:00:06',
        '{"type":99,"content":"removed","visibles":[]}');
