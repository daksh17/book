-- create_tables_faker.cql
-- Multiple tables demonstrating different primary key patterns for Faker data generation.
-- Use keyspace taulu_testing (create with create_table.cql or create here).

CREATE KEYSPACE IF NOT EXISTS taulu_testing
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
AND durable_writes = true;

USE taulu_testing;

-- ---------------------------------------------------------------------------
-- 1. Single partition key + single clustering key
--    PRIMARY KEY (partition_key, clustering_key)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS t1_single_pk_single_ck (
    user_id     uuid,
    event_at    timestamp,
    event_type  text,
    region      text,
    payload     blob,
    PRIMARY KEY (user_id, event_at)
) WITH CLUSTERING ORDER BY (event_at DESC);

-- ---------------------------------------------------------------------------
-- 2. Single partition key + composite clustering key (multiple clustering columns)
--    PRIMARY KEY (partition_key, ck1, ck2, ck3)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS t2_single_pk_composite_ck (
    tenant_id   text,
    year        int,
    month       int,
    order_id    uuid,
    amount      decimal,
    status      text,
    payload     blob,
    PRIMARY KEY (tenant_id, year, month, order_id)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, order_id ASC);

-- ---------------------------------------------------------------------------
-- 3. Composite partition key + single clustering key
--    PRIMARY KEY ((pk1, pk2), clustering_key)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS t3_composite_pk_single_ck (
    region_id   text,
    bucket      int,
    created_at  timestamp,
    user_id     uuid,
    event_type  text,
    payload     blob,
    PRIMARY KEY ((region_id, bucket), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

-- ---------------------------------------------------------------------------
-- 4. Composite partition key + composite clustering key
--    PRIMARY KEY ((pk1, pk2), ck1, ck2)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS t4_composite_pk_composite_ck (
    category    text,
    shard       int,
    event_date  date,
    item_id     uuid,
    name        text,
    quantity    int,
    payload     blob,
    PRIMARY KEY ((category, shard), event_date, item_id)
) WITH CLUSTERING ORDER BY (event_date DESC, item_id ASC);

-- ---------------------------------------------------------------------------
-- 5. Composite primary key: multiple partition columns + multiple clustering columns
--    PRIMARY KEY ((pk1, pk2, pk3), ck1, ck2)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS t5_composite_primary_composite_clustering (
    org_id      text,
    location    text,
    bucket      int,
    logged_at   timestamp,
    record_id   uuid,
    level       text,
    message     text,
    payload     blob,
    PRIMARY KEY ((org_id, location, bucket), logged_at, record_id)
) WITH CLUSTERING ORDER BY (logged_at DESC, record_id ASC);
