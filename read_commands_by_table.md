# Read commands by table

Use from `others/book` with Cassandra on port 19442 (demo):  
`CASSANDRA_PORT=19442 python read_faker_data.py <options>`

| Table key | CQL table | Partition key | Clustering key | Example command |
|-----------|-----------|----------------|----------------|-----------------|
| **t1** | t1_single_pk_single_ck | user_id | event_at | `--table t1` |
| | | | | `--table t1 --filter user_id=550e8400-e29b-41d4-a716-446655440000` |
| **t2** | t2_single_pk_composite_ck | tenant_id | year, month, order_id | `--table t2` |
| | | | | `--table t2 --filter tenant_id=acme` |
| | | | | `--table t2 --filter tenant_id=acme --filter year=2024` |
| **t3** | t3_composite_pk_single_ck | region_id, bucket | created_at | `--table t3` |
| | | | | `--table t3 --filter region_id=us-east --filter bucket=0` |
| **t4** | t4_composite_pk_composite_ck | category, shard | event_date, item_id | `--table t4` |
| | | | | `--table t4 --filter category=books --filter shard=0` |
| | | | | `--table t4 --filter category=books --filter event_date=2024-01-15` |
| **t5** | t5_composite_primary_composite_clustering | org_id, location, bucket | logged_at, record_id | `--table t5` |--limit 50
| | | | | `--table t5 --filter org_id=org1 --filter location=nyc` |
| | | | | `--table t5 --filter org_id=org1 --filter bucket=1` |
| **bookstore** | my_bookstore_example | tenant_id, store_id | category, book_id | `--table bookstore` |
| | | | | `--table bookstore --filter tenant_id=my-tenant` |
| | | | | `--table bookstore --filter tenant_id=x --filter store_id=550e8400-e29b-41d4-a716-446655440000` |

**Options:**
- `--limit N` — max rows per table (default 5)
- `--list-keys` — print partition/clustering keys for all tables (no DB)
- `--filter COL=VAL` — repeat for multiple columns; uses ALLOW FILTERING

**Examples:**
```bash
# All tables, 10 rows each
CASSANDRA_PORT=19442 python read_faker_data.py --limit 10

# Only t1 and t4
CASSANDRA_PORT=19442 python read_faker_data.py --table t1 --table t4 --limit 20

# t2 filtered by tenant
CASSANDRA_PORT=19442 python read_faker_data.py --table t2 --filter tenant_id=acme --limit 50

# Show primary keys for all tables
python read_faker_data.py --list-keys
```
