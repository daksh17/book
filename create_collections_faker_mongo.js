// create_collections_faker_mongo.js
// Creates the same logical tables as create_tables_faker.cql, as MongoDB collections
// sharded across the cluster (3 shards). Run against mongos.
//
// Usage (no auth):
//   mongosh mongodb://localhost:8000 create_collections_faker_mongo.js
//
// Usage (with auth, replace user/pass):
//   mongosh "mongodb://user:pass@localhost:8000/taulu_testing?authSource=admin" create_collections_faker_mongo.js

const DB = "taulu_testing";

// Enable sharding on the database so data can spread across 3 shards
print("Enabling sharding on database: " + DB);
sh.enableSharding(DB);

const db = db.getSiblingDB(DB);

// ---------------------------------------------------------------------------
// 1. t1_single_pk_single_ck  ~  C* PRIMARY KEY (user_id, event_at)
//    Shard key: user_id (single partition key)
// ---------------------------------------------------------------------------
print("Creating and sharding t1_single_pk_single_ck (shard key: user_id)");
db.createCollection("t1_single_pk_single_ck");
sh.shardCollection(DB + ".t1_single_pk_single_ck", { user_id: 1 });

// ---------------------------------------------------------------------------
// 2. t2_single_pk_composite_ck  ~  C* PRIMARY KEY (tenant_id, year, month, order_id)
//    Shard key: tenant_id (or compound tenant_id + year + month)
// ---------------------------------------------------------------------------
print("Creating and sharding t2_single_pk_composite_ck (shard key: tenant_id, year, month)");
db.createCollection("t2_single_pk_composite_ck");
sh.shardCollection(DB + ".t2_single_pk_composite_ck", { tenant_id: 1, year: 1, month: 1 });

// ---------------------------------------------------------------------------
// 3. t3_composite_pk_single_ck  ~  C* PRIMARY KEY ((region_id, bucket), created_at)
//    Shard key: region_id + bucket (composite partition key)
// ---------------------------------------------------------------------------
print("Creating and sharding t3_composite_pk_single_ck (shard key: region_id, bucket)");
db.createCollection("t3_composite_pk_single_ck");
sh.shardCollection(DB + ".t3_composite_pk_single_ck", { region_id: 1, bucket: 1 });

// ---------------------------------------------------------------------------
// 4. t4_composite_pk_composite_ck  ~  C* PRIMARY KEY ((category, shard), event_date, item_id)
//    Shard key: category + shard
// ---------------------------------------------------------------------------
print("Creating and sharding t4_composite_pk_composite_ck (shard key: category, shard)");
db.createCollection("t4_composite_pk_composite_ck");
sh.shardCollection(DB + ".t4_composite_pk_composite_ck", { category: 1, shard: 1 });

// ---------------------------------------------------------------------------
// 5. t5_composite_primary_composite_clustering  ~  C* PRIMARY KEY ((org_id, location, bucket), ...)
//    Shard key: org_id + location + bucket
// ---------------------------------------------------------------------------
print("Creating and sharding t5_composite_primary_composite_clustering (shard key: org_id, location, bucket)");
db.createCollection("t5_composite_primary_composite_clustering");
sh.shardCollection(DB + ".t5_composite_primary_composite_clustering", { org_id: 1, location: 1, bucket: 1 });

print("Done. Collections are sharded. Verify with: sh.status()");
