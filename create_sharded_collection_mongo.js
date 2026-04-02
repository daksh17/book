/**
 * create_sharded_collection_mongo.js
 *
 * MongoDB equivalent of create_table.cql / create_tables_faker.cql:
 * - Creates database (like keyspace) taulu_testing
 * - Creates one collection: my_bookstore_example
 * - If connected to mongos: enables sharding and shards the collection
 * - If connected to replica set/standalone: creates DB + collection only (no sharding)
 * - Inserts sample documents (like faker data)
 *
 * Replica set (e.g. demo): mongosh "mongodb://localhost:27201/?directConnection=true" --file create_sharded_collection_mongo.js
 * Sharded cluster / Atlas:  mongosh "mongodb://localhost:27017" or your Atlas URI --file create_sharded_collection_mongo.js
 */

// --- Config (same names as CQL for consistency) ---
const DB_NAME = 'taulu_testing';
const COLLECTION_NAME = 'my_bookstore_example';
const SHARD_KEY_FIELD = 'tenant_id';  // hashed for even distribution (like partition key)
const NUM_SAMPLE_DOCS = Number(process.env.NUM_SAMPLE_DOCS || 200);

// --- Detect if we are connected to mongos (sharded cluster router) ---
let isMongos = false;
try {
  const res = db.adminCommand({ isdbgrid: 1 });
  isMongos = !!(res && res.ismongos);
} catch (e) {
  // isdbgrid not available = not mongos (replica set or standalone)
}

const myDb = db.getSiblingDB(DB_NAME);

if (isMongos) {
  // --- 1. Enable sharding on the database (like creating keyspace) ---
  let out = sh.enableSharding(DB_NAME);
  if (!out.ok) {
    print('enableSharding failed:', JSON.stringify(out));
    quit(1);
  }
  print('Sharding enabled on database: ' + DB_NAME);

  // --- 2. Create and shard the collection (shard key ~ partition key: tenant_id hashed) ---
  const shardKey = { [SHARD_KEY_FIELD]: 'hashed' };
  out = sh.shardCollection(DB_NAME + '.' + COLLECTION_NAME, shardKey);
  if (!out.ok) {
    if (out.codeName === 'AlreadyInitialized' || (out.errmsg && out.errmsg.includes('already'))) {
      print('Collection already sharded: ' + COLLECTION_NAME);
    } else {
      print('shardCollection failed:', JSON.stringify(out));
      quit(1);
    }
  } else {
    print('Collection sharded: ' + COLLECTION_NAME + ' with key: ' + JSON.stringify(shardKey));
  }
} else {
  // Replica set or standalone: just ensure collection exists (no sharding)
  myDb.createCollection(COLLECTION_NAME);
  print('Database and collection created (replica set/standalone – sharding skipped).');
}

const coll = myDb.getCollection(COLLECTION_NAME);

// --- 3. Insert sample data (like faker-generated rows) ---
const categories = ['fiction', 'nonfiction', 'tech', 'kids', 'reference'];
const tenants = ['tenant_a', 'tenant_b', 'tenant_c'];

const docs = [];
const now = new Date();
for (let i = 0; i < NUM_SAMPLE_DOCS; i++) {
  docs.push({
    tenant_id: tenants[i % tenants.length],
    store_id: new ObjectId(),  // UUID-like in CQL -> ObjectId here
    category: categories[i % categories.length],
    book_id: new ObjectId(),
    updated_at: new Date(now.getTime() - i * 3600000),
    book_record: {
      title: 'Book ' + (i + 1),
      author: 'Author ' + (i % 10),
      isbn: '978-' + (1000000000 + i),
      pages: 100 + (i % 400),
    },
  });
}

coll.insertMany(docs);
print('Inserted ' + NUM_SAMPLE_DOCS + ' documents into ' + COLLECTION_NAME + '.');

// --- Summary ---
const count = coll.countDocuments();
print('Total documents in ' + COLLECTION_NAME + ': ' + count);
print('Done. Verify: db.taulu_testing.my_bookstore_example.find().limit(3)' + (isMongos ? ' and sh.status()' : ''));
