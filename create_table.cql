-- create_keyspace_and_table.cql

-- Keyspace: taulu_testing (SimpleStrategy for single DC)
CREATE KEYSPACE IF NOT EXISTS taulu_testing
WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : '3'
}
AND durable_writes = true;

-- Table: my_bookstore_example
CREATE TABLE IF NOT EXISTS taulu_testing.my_bookstore_example (
  tenant_id text,
  store_id uuid,
  category text,
  book_id uuid,
  updated_at timestamp,
  book_record blob,
  PRIMARY KEY ((tenant_id, store_id), category, book_id)
) WITH CLUSTERING ORDER BY (category ASC, book_id ASC);
