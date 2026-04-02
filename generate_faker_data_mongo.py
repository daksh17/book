#!/usr/bin/env python3
"""
generate_faker_data_mongo.py
Pushes the same Faker + payload data as the Cassandra script into MongoDB collections
that mirror create_tables_faker.cql. Use with a sharded cluster (connect to mongos);
collections are created and sharded by create_collections_faker_mongo.js.

Collections (same schema as C* tables):
  t1_single_pk_single_ck     - shard key: user_id
  t2_single_pk_composite_ck  - shard key: tenant_id, year, month
  t3_composite_pk_single_ck  - shard key: region_id, bucket
  t4_composite_pk_composite_ck - shard key: category, shard
  t5_composite_primary_composite_clustering - shard key: org_id, location, bucket

Usage:
  MONGO_URI=mongodb://localhost:8000 python generate_faker_data_mongo.py --count 500
  MONGO_URI=mongodb://localhost:8000 python generate_faker_data_mongo.py --count 200 --collection t1 --collection t3
  python generate_faker_data_mongo.py --count 1000 --batch-size 50 --payload-bytes 4096

Full benchmark (sweep payload × batch × write concern):
  MONGO_URI=mongodb://localhost:8000 python generate_faker_data_mongo.py --benchmark
  python generate_faker_data_mongo.py --benchmark --benchmark-count 500

Table-pattern benchmark (image style: 250/1K/4K/8K × batch 1/10/100, same as C* script):
  python generate_faker_data_mongo.py --benchmark-table-pattern
  python generate_faker_data_mongo.py --benchmark-table-pattern --benchmark-table-pattern-rows 3000
"""

import os
import time
import uuid
import random
import argparse
from datetime import datetime, date, timedelta
from decimal import Decimal

try:
    from pymongo import MongoClient
    from pymongo.write_concern import WriteConcern
    from bson.decimal128 import Decimal128
except ImportError:
    raise SystemExit("Install pymongo: pip install pymongo")

try:
    from faker import Faker
except ImportError:
    raise SystemExit("Install faker: pip install faker")

import book_record_pb2

# ---- Config from env ----
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:8000")
MONGO_DB = os.getenv("MONGO_DB", "taulu_testing")

fake = Faker()
Faker.seed(42)
random.seed(42)


def build_book_record_payload(target_bytes=None):
    """Same as Cassandra script: protobuf serialized, optional padding."""
    br = book_record_pb2.BookRecord()
    br.title = fake.sentence(nb_words=4)[:-1]
    br.author = fake.name()
    br.pages = fake.random_int(min=50, max=800)
    br.tags.extend(
        random.sample(
            ["sci-fi", "adventure", "magic", "romance", "history", "kids"],
            k=random.randint(0, 3),
        )
    )
    br.isbn = fake.isbn13()
    br.publisher = random.choice(["Acme Books", "Global Press", "OpenReads"])
    raw = br.SerializeToString()
    if target_bytes is not None and target_bytes > 0 and len(raw) < target_bytes:
        raw = raw + b"\x00" * (target_bytes - len(raw))
    return raw


def get_coll(client, name, write_concern):
    return client[MONGO_DB].get_collection(name, write_concern=write_concern)


# ---------------------------------------------------------------------------
# t1: user_id, event_at, event_type, region, payload  (shard key: user_id)
# Use many distinct user_ids so data spreads across all shards.
# ---------------------------------------------------------------------------
def gen_t1(coll, count, get_payload):
    num_users = min(50_000, max(100, count // 5))  # enough distinct shard keys to spread across shards
    user_ids = [uuid.uuid4() for _ in range(num_users)]
    events = ["login", "purchase", "view", "signup", "logout"]
    regions = [fake.country_code() for _ in range(5)]
    docs = [
        {
            "user_id": random.choice(user_ids),
            "event_at": fake.date_time_between(start_date="-1y", end_date="now"),
            "event_type": random.choice(events),
            "region": random.choice(regions),
            "payload": get_payload(),
        }
        for _ in range(count)
    ]
    return docs


# ---------------------------------------------------------------------------
# t2: tenant_id, year, month, order_id, amount, status, payload
# ---------------------------------------------------------------------------
def gen_t2(coll, count, get_payload):
    tenants = [f"tenant_{fake.word()}" for _ in range(5)]
    statuses = ["pending", "completed", "cancelled", "shipped"]
    docs = [
        {
            "tenant_id": random.choice(tenants),
            "year": fake.random_int(min=2022, max=2025),
            "month": fake.random_int(min=1, max=12),
            "order_id": uuid.uuid4(),
            "amount": Decimal128(str(round(random.uniform(10.0, 999.99), 2))),
            "status": random.choice(statuses),
            "payload": get_payload(),
        }
        for _ in range(count)
    ]
    return docs


# ---------------------------------------------------------------------------
# t3: region_id, bucket, created_at, user_id, event_type, payload
# ---------------------------------------------------------------------------
def gen_t3(coll, count, get_payload):
    regions = ["us-east", "eu-west", "ap-south"]
    buckets = list(range(0, 16))
    event_types = ["click", "impression", "conversion"]
    docs = [
        {
            "region_id": random.choice(regions),
            "bucket": random.choice(buckets),
            "created_at": fake.date_time_between(start_date="-1y", end_date="now"),
            "user_id": uuid.uuid4(),
            "event_type": random.choice(event_types),
            "payload": get_payload(),
        }
        for _ in range(count)
    ]
    return docs


# ---------------------------------------------------------------------------
# t4: category, shard, event_date, item_id, name, quantity, payload
# ---------------------------------------------------------------------------
def gen_t4(coll, count, get_payload):
    categories = ["electronics", "books", "clothing", "home"]
    docs = [
        {
            "category": random.choice(categories),
            "shard": random.randint(0, 7),
            "event_date": fake.date_between(start_date="-1y", end_date="today"),
            "item_id": uuid.uuid4(),
            "name": fake.catch_phrase(),
            "quantity": fake.random_int(min=1, max=100),
            "payload": get_payload(),
        }
        for _ in range(count)
    ]
    return docs


# ---------------------------------------------------------------------------
# t5: org_id, location, bucket, logged_at, record_id, level, message, payload
# ---------------------------------------------------------------------------
def gen_t5(coll, count, get_payload):
    orgs = [f"org_{str(uuid.uuid4())[:8]}" for _ in range(5)]
    locations = ["nyc", "london", "tokyo", "berlin"]
    levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    docs = [
        {
            "org_id": random.choice(orgs),
            "location": random.choice(locations),
            "bucket": random.randint(0, 31),
            "logged_at": fake.date_time_between(start_date="-1y", end_date="now"),
            "record_id": uuid.uuid4(),
            "level": random.choice(levels),
            "message": fake.sentence(nb_words=8),
            "payload": get_payload(),
        }
        for _ in range(count)
    ]
    return docs


COLLECTIONS = {
    "t1": ("t1_single_pk_single_ck", gen_t1),
    "t2": ("t2_single_pk_composite_ck", gen_t2),
    "t3": ("t3_composite_pk_single_ck", gen_t3),
    "t4": ("t4_composite_pk_composite_ck", gen_t4),
    "t5": ("t5_composite_primary_composite_clustering", gen_t5),
}


# Image-style table: payload 250, 1K, 4K, 8K × batch 1, 10, 100 → WPS (batches/s), Throughput (MBPS), # Rows
TABLE_PATTERN_PAYLOADS = [250, 1024, 4096, 8192]
TABLE_PATTERN_BATCHES = [1, 10, 100]


def run_benchmark_table_pattern(args):
    """Run benchmark in image pattern: payload 250/1K/4K/8K × batch 1/10/100; output grouped by batch size (same as C* script)."""
    client = MongoClient(MONGO_URI, uuidRepresentation="standard")
    name, gen_fn = COLLECTIONS["t1"]
    rows_per_run = (
        args.benchmark_table_pattern_count
        if getattr(args, "benchmark_table_pattern_count", None) is not None
        else getattr(args, "benchmark_table_pattern_rows", 5000)
    )
    payload_sizes = TABLE_PATTERN_PAYLOADS
    batch_sizes = TABLE_PATTERN_BATCHES
    wc = WriteConcern(w=1)

    print("Benchmark (table pattern): payload 250, 1K, 4K, 8K × batch 1, 10, 100 | {} rows per run".format(rows_per_run))
    print()

    col_payload = 14
    col_wps = 10
    col_mb = 12
    col_rows = 10
    sep = " | "

    try:
        coll = client[MONGO_DB].get_collection(name, write_concern=wc)
        for batch_size in batch_sizes:
            print("Batch Size: {}".format(batch_size))
            header = (
                "Payload (Bytes)".ljust(col_payload) + sep +
                "WPS".rjust(col_wps) + sep +
                "Throughput (MBPS)".rjust(col_mb) + sep +
                "# Rows".rjust(col_rows)
            )
            rule = "-" * (col_payload + col_wps + col_mb + col_rows + 3 * len(sep))
            print(rule)
            print(header)
            print(rule)

            for payload_bytes in payload_sizes:
                target_payload = payload_bytes if payload_bytes > 0 else None
                get_payload = lambda tb=target_payload: build_book_record_payload(tb)
                docs = gen_fn(None, rows_per_run, get_payload)
                payload_len = len(docs[0]["payload"]) if docs else 0
                t0 = time.perf_counter()
                for i in range(0, len(docs), batch_size):
                    batch = docs[i : i + batch_size]
                    coll.insert_many(batch)
                elapsed = time.perf_counter() - t0
                docs_per_sec = rows_per_run / elapsed if elapsed > 0 else 0
                batches_per_sec = docs_per_sec / batch_size if batch_size else 0
                mb_per_sec = (rows_per_run * payload_len) / (1024 * 1024) / elapsed if elapsed > 0 else 0
                if payload_bytes == 1024:
                    payload_label = "1K"
                elif payload_bytes == 4096:
                    payload_label = "4K"
                elif payload_bytes == 8192:
                    payload_label = "8K"
                else:
                    payload_label = str(payload_bytes)
                print(
                    payload_label.ljust(col_payload) + sep +
                    "{:,.0f}".format(batches_per_sec).rjust(col_wps) + sep +
                    "{:.2f}".format(mb_per_sec).rjust(col_mb) + sep +
                    "{:,.0f}".format(docs_per_sec).rjust(col_rows)
                )
            print(rule)
            print()
    finally:
        client.close()


def run_benchmark(args):
    """Sweep payload_sizes × batch_sizes × write_concerns; report docs/s and MB/s (collection t1)."""
    client = MongoClient(MONGO_URI, uuidRepresentation="standard")
    name, gen_fn = COLLECTIONS["t1"]
    count = args.benchmark_count
    payload_sizes = args.benchmark_payload_sizes
    batch_sizes = args.benchmark_batch_sizes
    write_concerns = args.benchmark_write_concerns

    print("Benchmark: collection t1_single_pk_single_ck, {} docs per run".format(count))
    print("Dimensions: payload_bytes={}, batch_sizes={}, write_concerns={}".format(
        payload_sizes, batch_sizes, write_concerns))
    print()

    # Table layout: fixed-width columns for alignment
    col_payload = 10
    col_batch = 7
    col_wc = 10
    col_docs = 12
    col_mb = 10
    sep = " | "
    header = (
        "payload_B".center(col_payload) + sep +
        "batch".center(col_batch) + sep +
        "w_concern".center(col_wc) + sep +
        "docs/s".rjust(col_docs) + sep +
        "MB/s".rjust(col_mb)
    )
    rule = "-" * (col_payload + col_batch + col_wc + col_docs + col_mb + 4 * len(sep))
    print(rule)
    print(header)
    print(rule)

    try:
        for payload_bytes in payload_sizes:
            target_payload = payload_bytes if payload_bytes > 0 else None
            get_payload = lambda tb=target_payload: build_book_record_payload(tb)

            for batch_size in batch_sizes:
                batch_size = max(1, batch_size)
                for wc_str in write_concerns:
                    docs = gen_fn(None, count, get_payload)
                    payload_len = len(docs[0]["payload"]) if docs else 0
                    w = 1 if wc_str == "1" else "majority"
                    wc = WriteConcern(w=w)
                    coll = client[MONGO_DB].get_collection(name, write_concern=wc)
                    t0 = time.perf_counter()
                    for i in range(0, len(docs), batch_size):
                        batch = docs[i : i + batch_size]
                        coll.insert_many(batch)
                    elapsed = time.perf_counter() - t0
                    docs_per_sec = count / elapsed if elapsed > 0 else 0
                    mb_per_sec = (count * payload_len) / (1024 * 1024) / elapsed if elapsed > 0 else 0
                    row = (
                        str(payload_bytes or payload_len).rjust(col_payload) + sep +
                        str(batch_size).rjust(col_batch) + sep +
                        wc_str.ljust(col_wc) + sep +
                        "{:,.0f}".format(docs_per_sec).rjust(col_docs) + sep +
                        "{:.2f}".format(mb_per_sec).rjust(col_mb)
                    )
                    print(row)
    finally:
        client.close()
    print(rule)
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Generate fake data into MongoDB collections (same schema as C* create_tables_faker.cql)"
    )
    parser.add_argument("--count", type=int, default=100, help="Number of documents per collection (default 100)")
    parser.add_argument(
        "--collection",
        action="append",
        dest="collections",
        choices=list(COLLECTIONS.keys()),
        metavar="COLL",
        help="Collection(s) to populate (default: all). Can be repeated.",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=0,
        metavar="N",
        help="Target payload size in bytes (0 = natural protobuf size).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        metavar="N",
        help="Insert in batches of N documents (default 100).",
    )
    parser.add_argument(
        "--write-concern",
        choices=["1", "majority"],
        default="1",
        help="Write concern: 1 (default) or majority.",
    )
    # ---- Benchmark: sweep payload × batch × write concern ----
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run full test sweep: payload_sizes × batch_sizes × write_concerns, report docs/s and MB/s.",
    )
    parser.add_argument(
        "--benchmark-table-pattern",
        action="store_true",
        help="Run benchmark in image table pattern: payload 250/1K/4K/8K × batch 1/10/100; output grouped by batch (same as C*).",
    )
    parser.add_argument(
        "--benchmark-table-pattern-rows",
        type=int,
        default=5000,
        metavar="N",
        help="Rows (docs) per run for --benchmark-table-pattern (default 5000, same as C* script).",
    )
    parser.add_argument(
        "--benchmark-table-pattern-count",
        type=int,
        metavar="N",
        help="Alias for --benchmark-table-pattern-rows (deprecated).",
    )
    parser.add_argument(
        "--benchmark-count",
        type=int,
        default=1000,
        metavar="N",
        help="Documents per benchmark run (default 1000).",
    )
    parser.add_argument(
        "--benchmark-payload-sizes",
        nargs="*",
        type=int,
        default=[250, 1024, 4096, 8192],
        metavar="N",
        help="Payload sizes in bytes for benchmark (default: 250 1024 4096 8192).",
    )
    parser.add_argument(
        "--benchmark-batch-sizes",
        nargs="*",
        type=int,
        default=[1, 10, 50, 100],
        metavar="N",
        help="Batch sizes for benchmark (default: 1 10 50 100).",
    )
    parser.add_argument(
        "--benchmark-write-concerns",
        nargs="*",
        choices=["1", "majority"],
        default=["1", "majority"],
        metavar="WC",
        help="Write concerns for benchmark (default: 1 majority).",
    )
    args = parser.parse_args()

    if args.benchmark_table_pattern:
        run_benchmark_table_pattern(args)
        return

    if args.benchmark:
        run_benchmark(args)
        return

    w = 1 if args.write_concern == "1" else "majority"
    wc = WriteConcern(w=w)
    client = MongoClient(MONGO_URI, uuidRepresentation="standard")
    target_payload = args.payload_bytes if args.payload_bytes > 0 else None
    get_payload = lambda: build_book_record_payload(target_payload)

    colls_to_run = args.collections if args.collections else list(COLLECTIONS.keys())
    batch_size = max(1, args.batch_size)
    total_docs = 0
    sample_payload_len = len(get_payload())
    t0 = time.perf_counter()

    try:
        for key in colls_to_run:
            name, gen_fn = COLLECTIONS[key]
            coll = get_coll(client, name, wc)
            print(f"Populating {name} ({args.count} docs, batch_size={batch_size})...")
            docs = gen_fn(coll, args.count, get_payload)
            for i in range(0, len(docs), batch_size):
                batch = docs[i : i + batch_size]
                coll.insert_many(batch)
            total_docs += args.count
            print(f"  Done {name}")
    finally:
        client.close()

    elapsed = time.perf_counter() - t0
    total_bytes = total_docs * sample_payload_len
    docs_per_sec = total_docs / elapsed if elapsed > 0 else 0
    mb_per_sec = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0

    print()
    print("Done.")
    print(f"Total docs: {total_docs}  |  Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {docs_per_sec:.1f} docs/s  |  {mb_per_sec:.2f} MB/s (payload)")


if __name__ == "__main__":
    main()
