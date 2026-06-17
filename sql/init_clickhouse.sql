-- Create Database
CREATE DATABASE IF NOT EXISTS ecom;

-- Destination Customers Table
CREATE TABLE IF NOT EXISTS ecom.customers (
    id UInt32,
    name String,
    email String,
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    ts_ms UInt64,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY id
ORDER BY id;

-- Destination Orders Table
CREATE TABLE IF NOT EXISTS ecom.orders (
    id UInt32,
    customer_id UInt32,
    product String,
    amount Decimal(10, 2),
    status String,
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    ts_ms UInt64,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(ts_ms)
PRIMARY KEY id
ORDER BY id;

-- Queue Table for Customers
CREATE TABLE IF NOT EXISTS ecom.customers_queue (
    raw String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'dbserver.ecom.customers',
         kafka_group_name = 'ch_customers_group',
         kafka_format = 'JSONAsString';

-- Queue Table for Orders
CREATE TABLE IF NOT EXISTS ecom.orders_queue (
    raw String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'dbserver.ecom.orders',
         kafka_group_name = 'ch_orders_group',
         kafka_format = 'JSONAsString';

-- Materialized View for Customers
CREATE MATERIALIZED VIEW IF NOT EXISTS ecom.mv_customers TO ecom.customers AS
SELECT
    if(JSONExtractString(raw, 'op') = 'd', JSONExtractUInt(raw, 'before', 'id'), JSONExtractUInt(raw, 'after', 'id')) AS id,
    if(JSONExtractString(raw, 'op') = 'd', JSONExtractString(raw, 'before', 'name'), JSONExtractString(raw, 'after', 'name')) AS name,
    if(JSONExtractString(raw, 'op') = 'd', JSONExtractString(raw, 'before', 'email'), JSONExtractString(raw, 'after', 'email')) AS email,
    parseDateTime64BestEffortOrZero(if(JSONExtractString(raw, 'op') = 'd', JSONExtractString(raw, 'before', 'created_at'), JSONExtractString(raw, 'after', 'created_at'))) AS created_at,
    parseDateTime64BestEffortOrZero(if(JSONExtractString(raw, 'op') = 'd', JSONExtractString(raw, 'before', 'updated_at'), JSONExtractString(raw, 'after', 'updated_at'))) AS updated_at,
    JSONExtractUInt(raw, 'ts_ms') AS ts_ms,
    if(JSONExtractString(raw, 'op') = 'd', 1, 0) AS is_deleted
FROM ecom.customers_queue;

-- Materialized View for Orders
CREATE MATERIALIZED VIEW IF NOT EXISTS ecom.mv_orders TO ecom.orders AS
SELECT
    if(JSONExtractString(raw, 'op') = 'd', JSONExtractUInt(raw, 'before', 'id'), JSONExtractUInt(raw, 'after', 'id')) AS id,
    if(JSONExtractString(raw, 'op') = 'd', JSONExtractUInt(raw, 'before', 'customer_id'), JSONExtractUInt(raw, 'after', 'customer_id')) AS customer_id,
    if(JSONExtractString(raw, 'op') = 'd', JSONExtractString(raw, 'before', 'product'), JSONExtractString(raw, 'after', 'product')) AS product,
    if(JSONExtractString(raw, 'op') = 'd', JSONExtractFloat(raw, 'before', 'amount'), JSONExtractFloat(raw, 'after', 'amount')) AS amount,
    if(JSONExtractString(raw, 'op') = 'd', JSONExtractString(raw, 'before', 'status'), JSONExtractString(raw, 'after', 'status')) AS status,
    parseDateTime64BestEffortOrZero(if(JSONExtractString(raw, 'op') = 'd', JSONExtractString(raw, 'before', 'created_at'), JSONExtractString(raw, 'after', 'created_at'))) AS created_at,
    parseDateTime64BestEffortOrZero(if(JSONExtractString(raw, 'op') = 'd', JSONExtractString(raw, 'before', 'updated_at'), JSONExtractString(raw, 'after', 'updated_at'))) AS updated_at,
    JSONExtractUInt(raw, 'ts_ms') AS ts_ms,
    if(JSONExtractString(raw, 'op') = 'd', 1, 0) AS is_deleted
FROM ecom.orders_queue;
