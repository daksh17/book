#!/usr/bin/env python3
"""
generate_faker_data.py
Generates fake data using Faker and inserts into multiple Cassandra tables
with different primary key patterns. Uses BookRecord protobuf for blob payloads.

Throughput levers (tunable via CLI):
  - Payload size: Larger payloads limit rows/sec but can increase MB/sec; use --payload-bytes.
  - Batching: Grouping writes (--batch-size) breaks through ops/sec limits.
  - Atomicity: UNLOGGED batches (default) are faster; LOGGED batches add atomicity and cost.

Tables:
  t1 - Single partition key + single clustering key
  t2 - Single partition key + composite clustering key
  t3 - Composite partition key + single clustering key
  t4 - Composite partition key + composite clustering key
  t5 - Composite primary key (multi PK) + composite clustering key

Usage:
  CASSANDRA_PORT=19442 python generate_faker_data.py --count 500
  python generate_faker_data.py --count 200 --table t1 --consistency QUORUM
  python generate_faker_data.py --count 1000 --batch-size 50 --batch-type UNLOGGED
  python generate_faker_data.py --count 500 --payload-bytes 4096 --batch-size 20

Benchmark mode (single partition, latency cap 70ms, 3D sweep):
  python generate_faker_data.py --benchmark
  python generate_faker_data.py --benchmark --payload-sizes 250 1024 8192 --batch-sizes 1 100 --rows-per-partition 1000 10000

Table-pattern benchmark (image style: 250/1K/4K/8K × batch 1/10/100, WPS/MBPS/# Rows):
  python generate_faker_data.py --benchmark-table-pattern
  python generate_faker_data.py --benchmark-table-pattern --benchmark-table-pattern-rows 3000
"""

import os
import sys
import time
import uuid
import random
import argparse
from datetime import datetime, date, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, BatchType
import ssl

try:
    from faker import Faker
except ImportError:
    raise SystemExit("Install faker: pip install faker")

import book_record_pb2

# ---- Config from env ----
_raw_contacts = os.getenv("CASSANDRA_CONTACT_POINTS", "127.0.0.1").strip().split(",")
CONTACT_POINTS = [c.strip() for c in _raw_contacts if c.strip()]
PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taulu_testing")
USERNAME = os.getenv("CASSANDRA_USERNAME", None)
PASSWORD = os.getenv("CASSANDRA_PASSWORD", None)
USE_SSL = os.getenv("CASSANDRA_USE_SSL", "false").lower() == "true"
CA_CERT = os.getenv("CASSANDRA_CA_CERT", None)

# When connecting to local Docker demo CQL ports, force 127.0.0.1 so we never use
# a Docker-internal IP that times out from the host.
# Main cluster: 19442-19446; DC cluster (Secondary): 19450-19454 (use auth: cassandra/cassandra).
if PORT in (19442, 19443, 19444, 19445, 19446, 19450, 19451, 19452, 19453, 19454):
    CONTACT_POINTS = ["127.0.0.1"]

# ---- Faker ----
fake = Faker()
Faker.seed(42)
random.seed(42)


def make_session():
    auth_provider = None
    if USERNAME and PASSWORD:
        auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
    ssl_options = None
    if USE_SSL and CA_CERT:
        ssl_context = ssl.create_default_context(cafile=CA_CERT)
        ssl_options = {"ssl_context": ssl_context}
    # Use WhiteListRoundRobinPolicy so the driver only uses contact_points (e.g. 127.0.0.1).
    # Otherwise it discovers Docker-internal IPs (e.g. 192.168.0.223) and tries to connect there, which times out from the host.
    lbp = WhiteListRoundRobinPolicy(CONTACT_POINTS)
    cluster = Cluster(
        contact_points=CONTACT_POINTS,
        port=PORT,
        auth_provider=auth_provider,
        ssl_options=ssl_options,
        load_balancing_policy=lbp,
    )
    session = cluster.connect()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        f"WITH replication = {{'class':'SimpleStrategy','replication_factor':'1'}}"
    )
    session.set_keyspace(KEYSPACE)
    return cluster, session


def build_book_record_payload(target_bytes=None):
    """Build a payload (protobuf serialized). If target_bytes is set, pad to that size (throughput lever)."""
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


def execute_rows(session, prepared, row_tuples, batch_size, batch_type, latencies_out=None):
    """
    Execute insert rows one-by-one or in batches.
    batch_size=1: no batching. batch_size>1: group into BatchStatement.
    batch_type UNLOGGED (faster, no atomicity) vs LOGGED (atomic, slower).
    If latencies_out is a list, append per-execute latency in seconds (for benchmark).
    """
    def run_one(cb):
        t0 = time.perf_counter()
        cb()
        if latencies_out is not None:
            latencies_out.append(time.perf_counter() - t0)

    if batch_size <= 1:
        for row in row_tuples:
            run_one(lambda: session.execute(prepared, row))
        return
    bt = BatchType.LOGGED if batch_type == "LOGGED" else BatchType.UNLOGGED
    batch = BatchStatement(batch_type=bt)
    n = 0
    for row in row_tuples:
        batch.add(prepared, row)
        n += 1
        if n >= batch_size:
            run_one(lambda: session.execute(batch))
            batch = BatchStatement(batch_type=bt)
            n = 0
    if n > 0:
        run_one(lambda: session.execute(batch))


# ---------------------------------------------------------------------------
# Table 1: Single partition key + single clustering key
# PRIMARY KEY (user_id, event_at)
# ---------------------------------------------------------------------------
INSERT_T1 = """
INSERT INTO t1_single_pk_single_ck (user_id, event_at, event_type, region, payload)
VALUES (?, ?, ?, ?, ?)
"""


def gen_t1(session, prepared, count, opts):
    get_payload = opts["get_payload"]
    user_ids = [uuid.uuid4() for _ in range(min(20, count // 10 + 1))]
    events = ["login", "purchase", "view", "signup", "logout"]
    regions = [fake.country_code() for _ in range(5)]
    rows = [
        (
            random.choice(user_ids),
            fake.date_time_between(start_date="-1y", end_date="now"),
            random.choice(events),
            random.choice(regions),
            get_payload(),
        )
        for _ in range(count)
    ]
    execute_rows(session, prepared, rows, opts["batch_size"], opts["batch_type"])


# ---------------------------------------------------------------------------
# Table 2: Single partition key + composite clustering key
# PRIMARY KEY (tenant_id, year, month, order_id)
# ---------------------------------------------------------------------------
INSERT_T2 = """
INSERT INTO t2_single_pk_composite_ck
(tenant_id, year, month, order_id, amount, status, payload)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def gen_t2(session, prepared, count, opts):
    get_payload = opts["get_payload"]
    tenants = [f"tenant_{fake.word()}" for _ in range(5)]
    statuses = ["pending", "completed", "cancelled", "shipped"]
    rows = [
        (
            random.choice(tenants),
            fake.random_int(min=2022, max=2025),
            fake.random_int(min=1, max=12),
            uuid.uuid4(),
            Decimal(str(round(random.uniform(10.0, 999.99), 2))),
            random.choice(statuses),
            get_payload(),
        )
        for _ in range(count)
    ]
    execute_rows(session, prepared, rows, opts["batch_size"], opts["batch_type"])


# ---------------------------------------------------------------------------
# Table 3: Composite partition key + single clustering key
# PRIMARY KEY ((region_id, bucket), created_at)
# ---------------------------------------------------------------------------
INSERT_T3 = """
INSERT INTO t3_composite_pk_single_ck
(region_id, bucket, created_at, user_id, event_type, payload)
VALUES (?, ?, ?, ?, ?, ?)
"""


def gen_t3(session, prepared, count, opts):
    get_payload = opts["get_payload"]
    regions = ["us-east", "eu-west", "ap-south"]
    buckets = list(range(0, 16))
    event_types = ["click", "impression", "conversion"]
    rows = [
        (
            random.choice(regions),
            random.choice(buckets),
            fake.date_time_between(start_date="-1y", end_date="now"),
            uuid.uuid4(),
            random.choice(event_types),
            get_payload(),
        )
        for _ in range(count)
    ]
    execute_rows(session, prepared, rows, opts["batch_size"], opts["batch_type"])


# ---------------------------------------------------------------------------
# Table 4: Composite partition key + composite clustering key
# PRIMARY KEY ((category, shard), event_date, item_id)
# ---------------------------------------------------------------------------
INSERT_T4 = """
INSERT INTO t4_composite_pk_composite_ck
(category, shard, event_date, item_id, name, quantity, payload)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def gen_t4(session, prepared, count, opts):
    get_payload = opts["get_payload"]
    categories = ["electronics", "books", "clothing", "home"]
    rows = [
        (
            random.choice(categories),
            random.randint(0, 7),
            fake.date_between(start_date="-1y", end_date="today"),
            uuid.uuid4(),
            fake.catch_phrase(),
            fake.random_int(min=1, max=100),
            get_payload(),
        )
        for _ in range(count)
    ]
    execute_rows(session, prepared, rows, opts["batch_size"], opts["batch_type"])


# ---------------------------------------------------------------------------
# Table 5: Composite primary key (multi PK) + composite clustering key
# PRIMARY KEY ((org_id, location, bucket), logged_at, record_id)
# ---------------------------------------------------------------------------
INSERT_T5 = """
INSERT INTO t5_composite_primary_composite_clustering
(org_id, location, bucket, logged_at, record_id, level, message, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


def gen_t5(session, prepared, count, opts):
    get_payload = opts["get_payload"]
    orgs = [f"org_{fake.uuid4()[:8]}" for _ in range(5)]
    locations = ["nyc", "london", "tokyo", "berlin"]
    levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    rows = [
        (
            random.choice(orgs),
            random.choice(locations),
            random.randint(0, 31),
            fake.date_time_between(start_date="-1y", end_date="now"),
            uuid.uuid4(),
            random.choice(levels),
            fake.sentence(nb_words=8),
            get_payload(),
        )
        for _ in range(count)
    ]
    execute_rows(session, prepared, rows, opts["batch_size"], opts["batch_type"])


def _percentile(sorted_list, p):
    """p in [0, 100]. Returns percentile value."""
    if not sorted_list:
        return 0.0
    k = (len(sorted_list) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_list) else f
    return sorted_list[f] + (k - f) * (sorted_list[c] - sorted_list[f])


def run_single_partition_t1(session, prepared, payload_bytes, batch_size, rows_per_partition, batch_type="UNLOGGED"):
    """
    Write exactly one partition: one user_id, rows_per_partition sort keys (event_at).
    Returns (rows_written, payload_len, latencies_sec_list).
    """
    user_id = uuid.uuid4()
    events = ["login", "purchase", "view", "signup", "logout"]
    regions = [fake.country_code() for _ in range(5)]
    get_payload = lambda: build_book_record_payload(payload_bytes if payload_bytes > 0 else None)
    # Ensure distinct event_at (sort keys) for the partition
    base_ts = fake.date_time_between(start_date="-1y", end_date="now")
    rows = []
    for i in range(rows_per_partition):
        # Distinct clustering key: event_at (and optionally other cols; t1 only has event_at as CK)
        event_at = base_ts + timedelta(microseconds=i)
        rows.append(
            (
                user_id,
                event_at,
                random.choice(events),
                random.choice(regions),
                get_payload(),
            )
        )
    payload_len = len(rows[0][-1])
    latencies = []
    opts = {"batch_size": batch_size, "batch_type": batch_type, "get_payload": lambda: None}
    execute_rows(session, prepared, rows, batch_size, batch_type, latencies_out=latencies)
    return rows_per_partition, payload_len, latencies


# Image-style table: payload 250, 1K, 4K, 8K × batch 1, 10, 100 → WPS (batches/s), Throughput (MBPS), # Rows
TABLE_PATTERN_PAYLOADS = [250, 1024, 4096, 8192]
TABLE_PATTERN_BATCHES = [1, 10, 100]


def run_benchmark_table_pattern(session, args):
    """Run benchmark in image pattern: payload 250/1K/4K/8K × batch 1/10/100; output grouped by batch size."""
    table_name, insert_cql, _ = TABLES["t1"]
    prepared = session.prepare(insert_cql)
    rows_per_run = getattr(args, "benchmark_table_pattern_rows", 5000)
    payload_sizes = TABLE_PATTERN_PAYLOADS
    batch_sizes = TABLE_PATTERN_BATCHES

    print("Benchmark (table pattern): payload 250, 1K, 4K, 8K × batch 1, 10, 100 | {} rows per run".format(rows_per_run))
    print()

    col_payload = 14
    col_wps = 10
    col_mb = 12
    col_rows = 10
    sep = " | "

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
            rows, payload_len, latencies_sec = run_single_partition_t1(
                session, prepared,
                payload_bytes=payload_bytes,
                batch_size=batch_size,
                rows_per_partition=rows_per_run,
                batch_type=args.batch_type,
            )
            total_sec = sum(latencies_sec)
            rows_per_sec = rows / total_sec if total_sec > 0 else 0
            batches_per_sec = rows_per_sec / batch_size if batch_size else 0
            mb_s = (rows * payload_len) / (1024 * 1024) / total_sec if total_sec > 0 else 0
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
                "{:.2f}".format(mb_s).rjust(col_mb) + sep +
                "{:,.0f}".format(rows_per_sec).rjust(col_rows)
            )
        print(rule)
        print()


def run_benchmark_sweep(session, args):
    """Run 3D sweep: payload_sizes x batch_sizes x rows_per_partition. Single partition per test, latency cap 70ms."""
    from cassandra.query import PreparedStatement
    table_name, insert_cql, _ = TABLES["t1"]
    prepared = session.prepare(insert_cql)
    latency_cap_ms = args.latency_cap_ms
    payload_sizes = args.benchmark_payload_sizes
    batch_sizes = args.benchmark_batch_sizes
    rows_per_partition_list = args.benchmark_rows_per_partition

    print("Benchmark: single partition per test, latency cap {} ms".format(latency_cap_ms))
    print("Dimensions: payload_bytes={}, batch_sizes={}, rows_per_partition={}".format(
        payload_sizes, batch_sizes, rows_per_partition_list))
    print()
    header = "payload_B  batch  rows_part  WPS     MB/s   p50_ms  p99_ms   latency_cap_{}ms".format(latency_cap_ms)
    print(header)
    print("-" * len(header))

    for payload_bytes in payload_sizes:
        for batch_size in batch_sizes:
            for rows_per_partition in rows_per_partition_list:
                rows, payload_len, latencies_sec = run_single_partition_t1(
                    session, prepared,
                    payload_bytes=payload_bytes,
                    batch_size=batch_size,
                    rows_per_partition=rows_per_partition,
                    batch_type=args.batch_type,
                )
                total_sec = sum(latencies_sec)
                wps = rows / total_sec if total_sec > 0 else 0
                mb_s = (rows * payload_len) / (1024 * 1024) / total_sec if total_sec > 0 else 0
                sorted_lat = sorted(latencies_sec)
                p50_ms = _percentile(sorted_lat, 50) * 1000
                p99_ms = _percentile(sorted_lat, 99) * 1000
                cap_ok = "OK" if p99_ms <= latency_cap_ms else "P99>{:.0f}ms".format(latency_cap_ms)
                print("{:9}  {:5}  {:9}  {:6.0f}  {:6.2f}  {:6.1f}  {:6.1f}  {}".format(
                    payload_bytes or int(payload_len), batch_size, rows_per_partition, wps, mb_s, p50_ms, p99_ms, cap_ok))
    print()


# ---- Table registry ----
TABLES = {
    "t1": ("t1_single_pk_single_ck", INSERT_T1, gen_t1),
    "t2": ("t2_single_pk_composite_ck", INSERT_T2, gen_t2),
    "t3": ("t3_composite_pk_single_ck", INSERT_T3, gen_t3),
    "t4": ("t4_composite_pk_composite_ck", INSERT_T4, gen_t4),
    "t5": ("t5_composite_primary_composite_clustering", INSERT_T5, gen_t5),
}


def main():
    parser = argparse.ArgumentParser(
        description="Generate fake data into Cassandra tables (Faker + protobuf)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of rows per table (default 100)",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        choices=list(TABLES.keys()),
        metavar="TABLE",
        help="Table(s) to populate (default: all). Can be repeated.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Not used for single-session; kept for API compatibility",
    )
    consistency_choices = [
        "ONE", "TWO", "THREE", "QUORUM", "LOCAL_ONE", "LOCAL_QUORUM",
        "EACH_QUORUM", "ALL", "SERIAL", "LOCAL_SERIAL",
    ]
    parser.add_argument(
        "--consistency",
        choices=consistency_choices,
        default="ONE",
        help="Write consistency level (default: ONE)",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=0,
        metavar="N",
        help="Target payload size in bytes (0 = natural protobuf size). Throughput lever: larger = more data/row, fewer rows/sec.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        metavar="N",
        help="Group N inserts per batch (1 = no batching). Batching breaks through ops/sec ceiling.",
    )
    parser.add_argument(
        "--batch-type",
        choices=["UNLOGGED", "LOGGED"],
        default="UNLOGGED",
        help="UNLOGGED = faster, no atomicity. LOGGED = atomic batch, slower (atomicity has a big tax).",
    )
    # ---- Benchmark mode: single partition, 3D sweep, latency cap 70ms ----
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run benchmark sweep: single partition per test, WPS/MB/s and latency (p50, p99) with 70ms cap.",
    )
    parser.add_argument(
        "--benchmark-table-pattern",
        action="store_true",
        help="Run benchmark in image table pattern: payload 250/1K/4K/8K × batch 1/10/100; output grouped by batch.",
    )
    parser.add_argument(
        "--benchmark-table-pattern-rows",
        type=int,
        default=5000,
        metavar="N",
        help="Rows per run for --benchmark-table-pattern (default 5000).",
    )
    parser.add_argument(
        "--benchmark-payload-sizes",
        nargs="*",
        type=int,
        default=[250, 500, 1024, 2048, 4096, 8192],
        metavar="N",
        help="Payload sizes in bytes for benchmark (default: 250 500 1024 2048 4096 8192 = 250B to 8KB).",
    )
    parser.add_argument(
        "--benchmark-batch-sizes",
        nargs="*",
        type=int,
        default=[1, 10, 100],
        metavar="N",
        help="Batch sizes (ops per batch) for benchmark (default: 1 10 100).",
    )
    parser.add_argument(
        "--benchmark-rows-per-partition",
        nargs="*",
        type=int,
        default=[1000, 5000, 10000],
        metavar="N",
        help="Rows per partition (sort keys) for benchmark (default: 1000 5000 10000).",
    )
    parser.add_argument(
        "--latency-cap-ms",
        type=int,
        default=70,
        metavar="MS",
        help="Latency cap in ms; benchmark reports OK or P99>MS (default: 70).",
    )
    args = parser.parse_args()

    cluster, session = make_session()

    cl = getattr(ConsistencyLevel, args.consistency)
    session.default_consistency_level = cl

    if args.benchmark_table_pattern:
        try:
            run_benchmark_table_pattern(session, args)
        finally:
            cluster.shutdown()
        return

    if args.benchmark:
        try:
            run_benchmark_sweep(session, args)
        finally:
            cluster.shutdown()
        return

    target_payload = args.payload_bytes if args.payload_bytes > 0 else None
    get_payload = lambda: build_book_record_payload(target_payload)
    opts = {
        "batch_size": args.batch_size,
        "batch_type": args.batch_type,
        "get_payload": get_payload,
    }

    tables_to_run = args.tables if args.tables else list(TABLES.keys())
    print(f"Consistency: {args.consistency}")
    print(f"Payload: {'natural' if target_payload is None else f'{target_payload} bytes'}")
    print(f"Batch: size={args.batch_size}, type={args.batch_type}")
    print()

    total_rows = 0
    total_bytes = 0
    sample_payload_len = len(get_payload())
    t0 = time.perf_counter()

    try:
        for key in tables_to_run:
            name, insert_cql, gen_fn = TABLES[key]
            prepared = session.prepare(insert_cql)
            print(f"Populating {name} ({args.count} rows)...")
            gen_fn(session, prepared, args.count, opts)
            total_rows += args.count
            print(f"  Done {name}")
    finally:
        cluster.shutdown()

    elapsed = time.perf_counter() - t0
    total_bytes = total_rows * sample_payload_len
    rows_per_sec = total_rows / elapsed if elapsed > 0 else 0
    mb_per_sec = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0

    print()
    print("Done.")
    print(f"Total rows: {total_rows}  |  Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {rows_per_sec:.1f} rows/s  |  {mb_per_sec:.2f} MB/s (payload)")


if __name__ == "__main__":
    main()
