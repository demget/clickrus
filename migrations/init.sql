CREATE DATABASE IF NOT EXISTS logs_test;

CREATE TABLE IF NOT EXISTS logs_test.test (
  level String,
  msg String,
  date Date,
  time DateTime,
  stage String,
  event_uuid String,
  user_id UInt32
) ENGINE = MergeTree(date, (level, stage, event_uuid, user_id), 8192);
