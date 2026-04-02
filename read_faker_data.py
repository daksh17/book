#!/usr/bin/env python3
"""
read_faker_data.py
Reads records from all Faker + bookstore tables; supports primary-key filters with ALLOW FILTERING.
Decodes BookRecord protobuf payloads. Uses the same connection config as generate_faker_data.py.

Usage:
  CASSANDRA_PORT=19442 python read_faker_data.py
  CASSANDRA_PORT=19442 python read_faker_data.py --limit 20 --table t1 --table t4
  python read_faker_data.py --list-keys
  python read_faker_data.py --table t1 --filter user_id=550e8400-e29b-41d4-a716-446655440000

Commands by table (use --table KEY and optional --filter COL=VAL):

  KEY         | CQL TABLE                              | Example read commands
  ------------|----------------------------------------|----------------------------------------------------------
  t1          | t1_single_pk_single_ck                 | --table t1
              | PK: user_id   CK: event_at              | --table t1 --filter user_id=<uuid>
  ------------|----------------------------------------|----------------------------------------------------------
  t2          | t2_single_pk_composite_ck              | --table t2
              | PK: tenant_id  CK: year, month, order_id | --table t2 --filter tenant_id=acme
              |                                        | --table t2 --filter tenant_id=acme --filter year=2024
  ------------|----------------------------------------|----------------------------------------------------------
  t3          | t3_composite_pk_single_ck              | --table t3
              | PK: region_id, bucket  CK: created_at  | --table t3 --filter region_id=us-east --filter bucket=0
  ------------|----------------------------------------|----------------------------------------------------------
  t4          | t4_composite_pk_composite_ck           | --table t4
              | PK: category, shard  CK: event_date, item_id | --table t4 --filter category=books --filter shard=0
              |                                        | --table t4 --filter category=books --filter event_date=2024-01-15
  ------------|----------------------------------------|----------------------------------------------------------
  t5          | t5_composite_primary_composite_clustering | --table t5
              | PK: org_id, location, bucket           | --table t5 --filter org_id=org1 --filter location=nyc
              | CK: logged_at, record_id               | --table t5 --filter org_id=org1 --filter bucket=1
  ------------|----------------------------------------|----------------------------------------------------------
  bookstore   | my_bookstore_example                   | --table bookstore
              | PK: tenant_id, store_id  CK: category, book_id | --table bookstore --filter tenant_id=my-tenant
              |                                        | --table bookstore --filter tenant_id=x --filter store_id=<uuid>

Set CASSANDRA_PORT=19442 (or 19443, 19444) for the demo. Use --limit N to read more rows.
"""

import os
import sys
import uuid
from datetime import datetime, date

from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
import ssl

import book_record_pb2

# ---- Config (same as generate_faker_data.py) ----
_raw_contacts = os.getenv("CASSANDRA_CONTACT_POINTS", "127.0.0.1").strip().split(",")
CONTACT_POINTS = [c.strip() for c in _raw_contacts if c.strip()]
PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taulu_testing")
USERNAME = os.getenv("CASSANDRA_USERNAME", None)
PASSWORD = os.getenv("CASSANDRA_PASSWORD", None)
USE_SSL = os.getenv("CASSANDRA_USE_SSL", "false").lower() == "true"
CA_CERT = os.getenv("CASSANDRA_CA_CERT", None)

if PORT in (19442, 19443, 19444):
    CONTACT_POINTS = ["127.0.0.1"]


def make_session():
    auth_provider = None
    if USERNAME and PASSWORD:
        auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
    ssl_options = None
    if USE_SSL and CA_CERT:
        ssl_context = ssl.create_default_context(cafile=CA_CERT)
        ssl_options = {"ssl_context": ssl_context}
    lbp = WhiteListRoundRobinPolicy(CONTACT_POINTS)
    cluster = Cluster(
        contact_points=CONTACT_POINTS,
        port=PORT,
        auth_provider=auth_provider,
        ssl_options=ssl_options,
        load_balancing_policy=lbp,
    )
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    return cluster, session


def decode_payload(blob):
    if blob is None:
        return None
    br = book_record_pb2.BookRecord()
    try:
        br.ParseFromString(blob)
        return {"title": br.title, "author": br.author, "pages": br.pages, "tags": list(br.tags), "isbn": br.isbn, "publisher": br.publisher}
    except Exception:
        return "<decode error>"


# Table key -> (cql_name, display_cols, payload_col, partition_keys, clustering_keys)
# partition_keys and clustering_keys are used for --list-keys and for building WHERE with ALLOW FILTERING
TABLES = {
    "t1": (
        "t1_single_pk_single_ck",
        ["user_id", "event_at", "event_type", "region"],
        "payload",
        ["user_id"],
        ["event_at"],
    ),
    "t2": (
        "t2_single_pk_composite_ck",
        ["tenant_id", "year", "month", "order_id", "amount", "status"],
        "payload",
        ["tenant_id"],
        ["year", "month", "order_id"],
    ),
    "t3": (
        "t3_composite_pk_single_ck",
        ["region_id", "bucket", "created_at", "user_id", "event_type"],
        "payload",
        ["region_id", "bucket"],
        ["created_at"],
    ),
    "t4": (
        "t4_composite_pk_composite_ck",
        ["category", "shard", "event_date", "item_id", "name", "quantity"],
        "payload",
        ["category", "shard"],
        ["event_date", "item_id"],
    ),
    "t5": (
        "t5_composite_primary_composite_clustering",
        ["org_id", "location", "bucket", "logged_at", "record_id", "level", "message"],
        "payload",
        ["org_id", "location", "bucket"],
        ["logged_at", "record_id"],
    ),
    "bookstore": (
        "my_bookstore_example",
        ["tenant_id", "store_id", "category", "book_id", "updated_at"],
        "book_record",
        ["tenant_id", "store_id"],
        ["category", "book_id"],
    ),
}

# CQL type for PK/CK columns (for parsing --filter values): uuid, int, text, timestamp, date
COL_TYPES = {
    "user_id": "uuid", "event_at": "timestamp", "event_type": "text", "region": "text",
    "tenant_id": "text", "year": "int", "month": "int", "order_id": "uuid", "amount": "decimal", "status": "text",
    "region_id": "text", "bucket": "int", "created_at": "timestamp", "event_date": "date", "item_id": "uuid",
    "category": "text", "shard": "int", "org_id": "text", "location": "text", "logged_at": "timestamp",
    "record_id": "uuid", "store_id": "uuid", "book_id": "uuid", "updated_at": "timestamp",
}


def parse_filter_value(col, val_str):
    """Parse a filter string value to the type expected by COL_TYPES."""
    if col not in COL_TYPES:
        return val_str
    t = COL_TYPES[col]
    if t == "uuid":
        return uuid.UUID(val_str.strip())
    if t == "int":
        return int(val_str.strip())
    if t == "timestamp":
        return datetime.fromisoformat(val_str.strip().replace("Z", "+00:00"))
    if t == "date":
        return date.fromisoformat(val_str.strip())
    return val_str.strip()


def read_table(session, table_key, limit, filters=None):
    entry = TABLES[table_key]
    cql_name = entry[0]
    cols = entry[1]
    payload_col = entry[2]
    partition_keys = entry[3]
    clustering_keys = entry[4]
    all_key_cols = partition_keys + clustering_keys

    used_filter = False  # or True / "fallback"
    if not filters:
        query = f"SELECT * FROM {cql_name} LIMIT {limit}"
        result = session.execute(query)
    else:
        # Build WHERE col1 = %s AND col2 = %s ... ALLOW FILTERING
        valid = {k: v for k, v in filters.items() if k in (all_key_cols + cols)}
        if not valid:
            result = session.execute(f"SELECT * FROM {cql_name} LIMIT {limit}")
        else:
            where_parts = " AND ".join(f"{c} = %s" for c in valid.keys())
            query = f"SELECT * FROM {cql_name} WHERE {where_parts} LIMIT {limit} ALLOW FILTERING"
            params = [valid[c] for c in valid.keys()]
            result = session.execute(query, params)
            used_filter = True

    rows = list(result)
    # When filter returned no rows, fall back to unfiltered read so we still show data
    if used_filter and not rows:
        result = session.execute(f"SELECT * FROM {cql_name} LIMIT {limit}")
        rows = list(result)
        used_filter = "fallback"
    col_names = result.column_names if hasattr(result, "column_names") else (cols + [payload_col])
    return rows, col_names, payload_col, used_filter


def row_to_dict(row):
    if hasattr(row, "_asdict"):
        return row._asdict()
    if hasattr(row, "_fields"):
        return dict(zip(row._fields, row))
    return dict(row)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Read records from Faker/bookstore tables; optional primary-key filters with ALLOW FILTERING"
    )
    parser.add_argument("--limit", type=int, default=5, help="Max rows to read per table (default 5)")
    parser.add_argument(
        "--table", action="append", dest="tables", choices=list(TABLES.keys()), metavar="TABLE",
        help="Table(s) to read (default: all)",
    )
    parser.add_argument(
        "--list-keys", action="store_true",
        help="Print each table and its partition/clustering keys, then exit",
    )
    parser.add_argument(
        "--filter", action="append", dest="filters", metavar="COL=VAL",
        help="Filter by column=value (repeat for multiple). Uses ALLOW FILTERING. e.g. --filter user_id=uuid --filter tenant_id=acme",
    )
    args = parser.parse_args()

    if args.list_keys:
        print("Tables and primary keys (partition key | clustering key):")
        print("-" * 60)
        for key in TABLES:
            entry = TABLES[key]
            cql_name, cols, payload_col, pk, ck = entry[0], entry[1], entry[2], entry[3], entry[4]
            print(f"  {key}: {cql_name}")
            print(f"    partition key: {pk}")
            print(f"    clustering key: {ck}")
        return

    # Parse --filter col=val into a dict; values parsed by parse_filter_value when used per-table
    filters = None
    if args.filters:
        filters = {}
        for f in args.filters:
            if "=" not in f:
                continue
            col, val = f.split("=", 1)
            col = col.strip()
            try:
                filters[col] = parse_filter_value(col, val)
            except (ValueError, TypeError):
                filters[col] = val.strip()

    tables_to_read = args.tables if args.tables else list(TABLES.keys())
    cluster, session = make_session()

    try:
        for key in tables_to_read:
            entry = TABLES[key]
            cql_name, cols, payload_col = entry[0], entry[1], entry[2]
            pk, ck = entry[3], entry[4]
            print(f"\n{'='*60}")
            print(f"Table: {cql_name} (key={key})")
            print(f"  PK: {pk}  |  CK: {ck}")
            print("=" * 60)
            rows, col_names, payload_key, used_filter = read_table(session, key, args.limit, filters=filters)
            if not rows:
                print("  (no rows)")
                continue
            if used_filter == "fallback":
                print("  No rows matched filter; showing sample rows without filter:")
            print(f"  Read {len(rows)} row(s)\n")
            for i, row in enumerate(rows, 1):
                print(f"  --- Row {i} ---")
                r = row_to_dict(row)
                for c in col_names:
                    if c in r and r[c] is not None:
                        print(f"    {c}: {r[c]}")
                if payload_key and payload_key in r and r[payload_key]:
                    decoded = decode_payload(r[payload_key])
                    if decoded:
                        print(f"    {payload_key} (BookRecord): {decoded}")
                print()
    finally:
        cluster.shutdown()

    print("Done.")


if __name__ == "__main__":
    main()
