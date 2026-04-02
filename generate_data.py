#!/usr/bin/env python3
"""
generate_data.py
Generates and inserts N BookRecord protobuf messages into Cassandra.
Usage: python generate_data.py --count 1000 --concurrency 8
"""

import os
import uuid
import random
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import PreparedStatement
import book_record_pb2  # from protoc --python_out=./proto
from tqdm import tqdm
import time
import socket
import ssl
import sys

# ---- Config from env ----
CONTACT_POINTS = os.getenv("CASSANDRA_CONTACT_POINTS", "127.0.0.1").split(",")
PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taulu_testing")
USERNAME = os.getenv("CASSANDRA_USERNAME", None)
PASSWORD = os.getenv("CASSANDRA_PASSWORD", None)
USE_SSL = os.getenv("CASSANDRA_USE_SSL", "false").lower() == "true"
CA_CERT = os.getenv("CASSANDRA_CA_CERT", None)

# ---- Basic data to synthesize ----
CATEGORIES = ["Science Fiction", "Fantasy", "Mystery", "Romance", "Non-Fiction", "Children"]
TITLES = [
    "The Time Traveler's Almanac", "Under The Red Sky", "Secrets of the Old Manor",
    "Guide to Space Fishing", "The Last Librarian", "A Walk in Berlin"
]
AUTHORS = ["A. Author", "B. Writer", "C. Storyteller", "D. S. Smith", "E. K. Patel"]

# ---- Cassandra connection ----
def make_session():
    auth_provider = None
    if USERNAME and PASSWORD:
        auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)

    ssl_options = None
    if USE_SSL:
        if not CA_CERT:
            raise RuntimeError("CASSANDRA_CA_CERT required when CASSANDRA_USE_SSL=true")
        ssl_context = ssl.create_default_context(cafile=CA_CERT)
        ssl_options = {'ssl_context': ssl_context}

    # Use WhiteListRoundRobinPolicy so the driver only uses contact_points (e.g. 127.0.0.1).
    # Otherwise it discovers Docker-internal IPs and tries to connect there, which times out from the host.
    lbp = WhiteListRoundRobinPolicy(CONTACT_POINTS)
    cluster = Cluster(contact_points=CONTACT_POINTS, port=PORT, auth_provider=auth_provider, ssl_options=ssl_options, load_balancing_policy=lbp)
    session = cluster.connect()
    # Ensure keyspace exists (idempotent)
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{'class':'SimpleStrategy','replication_factor':'1'}}")
    session.set_keyspace(KEYSPACE)
    return cluster, session

# ---- Prepared statement ----
INSERT_CQL = """
INSERT INTO my_bookstore_example
  (tenant_id, store_id, category, book_id, updated_at, book_record)
VALUES (?, ?, ?, ?, ?, ?)
"""

def build_book_record(title, author):
    br = book_record_pb2.BookRecord()
    br.title = title
    br.author = author
    br.pages = random.randint(50, 1000)
    br.tags.extend(random.sample(["sci-fi", "adventure", "magic", "romance", "history", "kids"], k=random.randint(0,3)))
    br.isbn = str(random.randint(1000000000000, 9999999999999))
    br.publisher = random.choice(["Acme Books", "Global Press", "OpenReads"])
    return br

def insert_one(session, prepared, tenant_id, store_id, category, book_id, br_bytes):
    # Use datetime.utcnow() so Cassandra driver maps to timestamp type
    updated_at = datetime.utcnow()
    bound = prepared.bind((tenant_id, store_id, category, book_id, updated_at, br_bytes))
    session.execute(bound)

def worker_insert(session, prepared, tenant_id):
    # create a single record and write
    store_id = uuid.uuid4()
    category = random.choice(CATEGORIES)
    book_id = uuid.uuid4()
    br = build_book_record(random.choice(TITLES), random.choice(AUTHORS))
    br_bytes = br.SerializeToString()
    insert_one(session, prepared, tenant_id, store_id, category, book_id, br_bytes)
    return (tenant_id, str(store_id), category, str(book_id))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100, help="Number of records to generate")
    parser.add_argument("--concurrency", type=int, default=8, help="Threads")
    parser.add_argument("--tenant", type=str, default="tenant_123", help="tenant_id for generated rows")
    args = parser.parse_args()

    cluster, session = make_session()
    prepared = session.prepare(INSERT_CQL)

    results = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = []
        for _ in range(args.count):
            futures.append(ex.submit(worker_insert, session, prepared, args.tenant))
        for f in tqdm(as_completed(futures), total=len(futures)):
            try:
                results.append(f.result())
            except Exception as e:
                print("Insert error:", e, file=sys.stderr)

    print(f"Inserted {len(results)} rows")
    cluster.shutdown()

if __name__ == "__main__":
    main()
