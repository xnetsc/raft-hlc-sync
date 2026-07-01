# sync-lib

A zero-dependency, database-agnostic distributed data synchronization engine with HLC (Hybrid Logical Clock) conflict resolution, Raft-based leader election, and two-phase commit (2PC).

[中文文档](./README.zh-CN.md)

---

## Features

- **Database agnostic** — Works with SQLite, PostgreSQL, MySQL, or any SQL database via the DatabaseAdapter interface.
- **Transport agnostic** — No built-in WebSocket/gRPC/TCP. You provide the transport via callbacks.
- **Zero npm dependencies** — Pure JavaScript, no native modules.
- **HLC conflict resolution** — Last-write-wins with hybrid logical clocks. Tombstones prevent deleted data from resurrecting.
- **Leader election** — Raft-simplified per-shard election with term-based voting.
- **2PC** — Strong consistency for multi-node writes when needed.
- **Timer injection** — `timerAPI` is required. The engine never calls global `setTimeout`/`setInterval` directly, and schedules all periodic work internally via the injected functions.
- **Time function injection** — `getNow` is required. The engine never calls `Date.now()` or `new Date()` internally. All time acquisition goes through the externally injected `getNow` function, which returns `{ value: number, unit: 's' | 'ms' | 'us' | 'ns' }`. The library internally normalizes to milliseconds.
- **Bidirectional time sync** — Every heartbeat ping/pong carries NTP-style timestamps (t1/t2/t3/t4). The engine automatically measures per-peer RTT and clock offset (sliding window, minimum-RTT selection). Results are readable via `engine.getClockSync(peerId?, unit?)`.
- **Network-aware election** — Election timeouts and candidate priority adapt automatically to measured RTT. Low-latency peers get shorter timeouts and higher priority, reducing convergence time on fast networks while avoiding spurious elections on slow/wide-area links.
- **Leader Lease** — After winning an election and on every subsequent heartbeat, the leader computes a safe lease duration (`electionMin − ε`) and sets a local timer. Within the lease the leader is guaranteed to be the only leader in the cluster. Use `engine.isLeaseValid(shardId?)` / `engine.isLeaseValidForKey(key)` to serve local reads without a quorum round-trip.
- **Dynamic 2PC / proxy timeouts** — `prepareTimeoutMs`, `proxyWaitMs`, and `proxyTimeoutMs` are configured as static lower bounds. When RTT measurements are available the engine automatically scales them up (e.g. `max(prepareTimeoutMs, rtt×4)`) so writes never spuriously time out on high-latency links.
- **HLC skew guard** — Incoming sync entries whose remote `wallTime` exceeds `localNow + maxClockSkewMs` (default 300 s) are silently skipped. Prevents a clock-skewed or misbehaving peer from advancing the local HLC far into the future.
- **Monotonic clock (`memonicNow`)** — Optional. If provided, the engine captures `getNow()` and `memonicNow()` once at construction and derives all subsequent timestamps as `startNow + (memonicNow() − startMemonic)`. Guarantees strict monotonicity even when the system clock is adjusted by NTP, DST, or manual intervention.
- **Receiver-initiated full resync on any outbound connection (v1.1.0+)** — Every time we connect out to a peer whose `lastSeq > localMaxSeq`, the receiver registers a `(-∞, +∞]` need for each registered table + tombstones (LWW key-space: all peers cover the full range; only the upper watermark varies). The dispatcher assigns non-overlapping sub-ranges as claims to available outbound peers in parallel, so multiple peers each fetch a different slice of the still-needed data.
- **Interval-set needs + per-peer claims (v1.1.0+)** — The receiver tracks what's still needed as interval fragments in `_full_sync_needs` (peer-agnostic) and in-flight reservations in `_full_sync_claims` (per-peer). On each successful page apply, the delivered `(pageStart, pageEnd]` is subtracted from the matching need **in the same transaction** as the row/tombstone apply — producing gaps as needed. The dispatcher re-fires on: page arrival, `full_sync_complete`, peer connect, peer disconnect, and the cleanup tick. A send-failure synchronously rolls back the just-inserted claim and retries with another free peer in the same dispatch pass; the claim TTL is only a last-resort fallback for a peer that silently stalls mid-fetch, and is **RTT-adaptive** — `effectiveTtl = min(fullSyncClaimTtlMs, max(fullSyncClaimTtlFloorMs, rtt × fullSyncClaimTtlRttFactor))`, so LAN peers (rtt≈10ms) recover in ~5s (the floor) while WAN peers get tens of seconds. Peers without RTT samples yet fall back to the static `fullSyncClaimTtlMs` ceiling (default 60 s — no healthy node should ever take 30+ seconds to respond to one page).
- **One-request-one-page protocol** — Each `full_sync_resume_req{request:{phase,tableName,rangeStart,rangeEnd,watermarkSeq}}` returns exactly one page (bounded by `fullSyncChunkSize`, default 500) plus a `full_sync_complete{request}` that carries the same scope back so the receiver releases only that claim. The remainder of the original range is re-dispatched on the next tick — possibly to a different peer.
- **Idempotent HLC LWW apply** — Rows and tombstones apply via HLC LWW + tombstone-priority, so retries / parallel duplicates / reconnects are always safe. Cursors are expressed as primary-key tuples using portable OR-expanded WHERE clauses (no dialect-specific tuple comparison).

---

## Installation

```bash
npm install sync-lib

# or copy the directory directly
cp -r sync-lib/ your-project/sync-lib/
```

---

## Quick Start

### Step 1: Implement DatabaseAdapter

The adapter provides basic query methods. Dialect-specific SQL (transactions, upsert, schema, triggers) is handled automatically via the `dialect` parameter.

**SQLite:**

```js
import Database from 'better-sqlite3';

const rawDb = new Database(':memory:');
rawDb.pragma('journal_mode = WAL');

const db = {
    run(sql, params = [])  { return rawDb.prepare(sql).run(...params); },
    get(sql, params = [])  { return rawDb.prepare(sql).get(...params) || null; },
    all(sql, params = [])  { return rawDb.prepare(sql).all(...params); },
    exec(sql)              { rawDb.exec(sql); },
};
```

**PostgreSQL:**

```js
import pg from 'pg';
const pool = new pg.Pool({ connectionString: '...' });

function pgConvert(sql) {
    let i = 0;
    return sql.replace(/\?/g, () => '$' + (++i));
}

const db = {
    async run(sql, p = [])  { const r = await pool.query(pgConvert(sql), p); return { changes: r.rowCount }; },
    async get(sql, p = [])  { const r = await pool.query(pgConvert(sql), p); return r.rows[0] || null; },
    async all(sql, p = [])  { const r = await pool.query(pgConvert(sql), p); return r.rows; },
    async exec(sql)         { await pool.query(sql); },
};
```

**MySQL:**

```js
import mysql from 'mysql2/promise';
const pool = await mysql.createPool({ host: '...', user: '...', password: '...', database: '...' });

const db = {
    async run(sql, p = [])  { const [r] = await pool.execute(sql, p); return { changes: r.affectedRows }; },
    async get(sql, p = [])  { const [rows] = await pool.execute(sql, p); return rows[0] || null; },
    async all(sql, p = [])  { const [rows] = await pool.execute(sql, p); return rows; },
    async exec(sql)         { await pool.query(sql); },
};
```

That's it — just 4 methods. The `dialect` parameter (see Step 2) fills in everything else automatically.

> For custom databases, see [Full DatabaseAdapter Interface](#databaseadapter-interface) and [Custom Adapter Examples](#custom-adapter-examples) below.

#### Built-in dialect support

sync-lib has built-in support for **SQLite**, **PostgreSQL**, and **MySQL**. When you specify `dialect: 'sqlite'` (or `'postgresql'` / `'mysql'`), all dialect-specific methods are auto-filled on your adapter:

- `beginTransaction()` / `commit()` / `rollback()` — transaction control
- `upsertSQL(table, columns, keyColumns)` — conflict-aware insert/update SQL
- `infraSchemaSQL()` — DDL for internal tables (`_sync_log`, `_sync_peers`, `_tombstones`, `_full_sync_needs`, `_full_sync_claims`)
- `triggersSQL(tableName, def)` / `dropTriggersSQL(tableName)` — auto-logging triggers

**If you don't specify a dialect (or specify an unsupported one), you must implement ALL of these methods yourself.** Missing methods will throw an error at construction time.

You can override any auto-filled method by defining it on your `db` adapter — your explicit implementation always takes priority over the built-in dialect.

### Step 2: Create engine and register tables

```js
import { SyncEngine } from 'sync-lib';

const engine = new SyncEngine({
    nodeId: 'node-1',
    db,
    dialect: 'sqlite',   // or 'postgresql' or 'mysql'

    // Required callbacks
    onSendToPeer:    (peerId, msg) => yourTransport.send(peerId, msg),
    onClosePeer:     (peerId)      => yourTransport.close(peerId),
    onLeaderChanged: (shardId, leaderId, isLocal) => {
        console.log(`Shard ${shardId}: leader=${leaderId} local=${isLocal}`);
    },

    // Required: any node can be elected as Leader and must handle proxied write requests
    onExecuteProxyRequest: async (payload) => {
        // Execute the write request locally and return the result
        return await yourBusinessLogic(payload);
    },

    // Optional callbacks
    onWriteCompleted: () => engine.notifyLocalWrite(),
    onError: (ctx, err) => console.error(`[sync] ${ctx}:`, err.message),

    // Optional: scheduling intervals
    heartbeatIntervalMs: 15000,    // tickHeartbeat interval (default: 15s) — also the time sync interval
    pullIntervalMs:      10000,    // tickPull interval (default: 10s)
    cleanupIntervalMs:   3600000,  // tickCleanup interval (default: 1h)
    timeSyncWindowSize:  8,        // per-peer sliding window for RTT samples (default: 8)

    // Required: inject time function — returns { value, unit }, the engine never uses Date.now() internally
    // Supported units: 's' (seconds), 'ms' (milliseconds), 'us' (microseconds), 'ns' (nanoseconds)
    getNow: () => ({ value: Date.now(), unit: 'ms' }),

    // Optional: monotonic clock source. When provided the engine pins startNow = getNow() and
    // startMemonic = memonicNow() at construction time, then computes every subsequent timestamp as
    //   _nowMs() = startNow + (memonicNow() − startMemonic)
    // This makes all internal timestamps strictly monotonic regardless of system clock adjustments.
    // Typical Node.js usage:
    memonicNow: () => ({ value: Number(process.hrtime.bigint()), unit: 'ns' }),

    // Optional: maximum allowed clock skew for incoming sync entries (default: 300000 ms = 5 min).
    // Entries whose remote wallTime exceeds localNow + maxClockSkewMs are skipped silently,
    // preventing a misbehaving peer from pushing the local HLC far into the future.
    maxClockSkewMs: 300000,

    // Optional: dynamic timeout lower bounds (scaled up by RTT when measurements are available)
    prepareTimeoutMs:      5000,   // 2PC prepare timeout  — effective = max(this, rtt×4)
    proxyWaitMs:           6000,   // wait for leader      — effective = max(this, electionMin + hbInterval)
    proxyTimeoutMs:        10000,  // proxy request        — effective = max(this, rtt×6)

    // Required: inject timer functions — the engine never uses global setTimeout/setInterval directly
    timerAPI: { setTimeout, clearTimeout, setInterval, clearInterval },
});

engine.registerTable('users', {
    keyColumns:  ['user_id'],
    dataColumns: ['user_id', 'name', 'email', '_hlc'],   // must include _hlc
    // registerTable validates: keyColumns non-empty, dataColumns non-empty, '_hlc' in dataColumns
    validator:   (row) => { if (!row.user_id) throw new Error('missing user_id'); },
});

engine.initSchema();     // Creates _sync_log, _sync_peers, _tombstones, _full_sync_needs, _full_sync_claims
engine.initTriggers();   // Creates auto-logging triggers (dialect-aware)
engine.start();          // Starts leader election + schedules tickPull/tickHeartbeat/tickCleanup internally
```

### Change-detection sources

Engine needs at least one change-detection source. Both may coexist:

1. **SQL triggers** (default) — `initTriggers()` installs AFTER INSERT/UPDATE/DELETE triggers that write to `_sync_log`. Also installs HLC integrity triggers (non-null, monotonic).
2. **External callback** — useful when triggers are not available (external CDC, business-layer hooks, DB log tailing).

   **Design principle:** sync-lib **drives** the subscription. The external app provides only the callback — it neither needs nor is allowed to know which tables sync-lib cares about. sync-lib reads its own registry (populated via `registerTable`) and, for each registered table, invokes the callback once to tell the external app "watch this table with this filter; when you see a matching change, call this `onChanges`".

   Per-table filter lives on `registerTable(name, { ..., where })`. When set, sync-lib forwards `where` through the callback. SQL triggers ignore it (they always cover the full table).

   Constructor option:
   - `subscribeToChanges: ({ table, where, keyColumns, dataColumns, onChanges }) => void`

   `start()` throws if neither source is configured. HLC integrity triggers are independent; you can install only those via `initTriggers({ changeLog: false })`.

```js
const engine = new SyncEngine({
    // ...
    subscribeToChanges: ({ table, where, onChanges }) => {
        // sync-lib calls this once per registered table. External learns what to watch
        // strictly from these arguments — it never introspects sync-lib's registry.
        myCdc.subscribe(table, where, (op, info) => {
            onChanges(
                [op],                                    // 'INSERT' | 'UPDATE' | 'DELETE'
                [{
                    tableName: table,
                    rowKey:    info.key,                 // object of key columns
                    rowData:   op === 'DELETE' ? null : info.row,   // object of all dataColumns
                    hlc:       info.hlc,                 // HLC timestamp string
                }]
            );
        });
    },
});

engine.registerTable('users',  { keyColumns: ['user_id'],  dataColumns: [...], where: 'deleted_at IS NULL' });
engine.registerTable('orders', { keyColumns: ['order_id'], dataColumns: [...], where: 'status != "cancelled"' });
engine.registerTable('audit',  { keyColumns: ['id'],       dataColumns: [...] });   // no filter

engine.initSchema();
engine.initTriggers({ changeLog: false }); // HLC integrity only; change log comes from callback
engine.initChangeSubscriptions();              // Invokes subscribeToChanges synchronously, once per registered table
engine.start();
```

### Step 3: Connect transport

sync-lib doesn't care what protocol you use. Just call these 3 methods:

```js
// When a peer connects
engine.peerConnected(peerId, { direction: 'inbound' });

// When you receive a message
engine.receiveMessage(peerId, rawString);

// When a peer disconnects
engine.peerDisconnected(peerId);
```

sync-lib talks back to peers via the `onSendToPeer(peerId, msg)` callback you provided.

### Step 4: Write data

> **Important:** All writes (INSERT/UPDATE/DELETE) to registered tables **must** go through `engine.write()` or be manually wrapped with `engine.beginManualTransaction()` / `commitManualTransaction()`. Do NOT write directly via `db.run(...)` outside of these wrappers — the engine needs to manage transactions and 2PC coordination to ensure data is correctly synced across nodes.

**Recommended: `engine.write()` (high-level API)**

The engine handles the entire lifecycle automatically: BEGIN → execute → trigger logs → 2PC prepare/ack → COMMIT → broadcast commit → notify peers.

```js
const result = await engine.write(async (db) => {
    const ts = engine.hlc.tick();
    db.run('INSERT INTO users (user_id, name, _hlc) VALUES (?, ?, ?)',
        ['u1', 'Alice', ts]);
    return { userId: 'u1' };  // return value is passed through
}, userId);  // shardKey for routing
```

**Low-level API (for fine-grained control)**

```js
const writeId = crypto.randomUUID();
engine.beginManualTransaction(writeId);
try {
    db.run('INSERT INTO orders ...', [...]);

    const entries = engine.getManualTransactionEntries(writeId);
    const shardId = engine.getShardId(userId);
    const term = engine.getStatus().shards[shardId].term;
    await engine.waitForPrepareAck(writeId, entries, term, shardId);

    engine.commitManualTransaction(writeId);
    engine.broadcastCommit(writeId);
} catch (err) {
    engine.rollbackManualTransaction(writeId);
    engine.broadcastAbort(writeId, err.message);
    throw err;
}
```

---

## Write Proxying

Non-leader nodes can forward writes to the leader:

```js
if (!engine.isLeaderForKey(userId)) {
    const result = await engine.proxyRequest(userId, {
        method: 'POST', path: '/api/users', body: reqBody,
    });
    return res.status(result.status).json(result.body);
}
// handle locally as leader...
```

The leader executes the request via the `onExecuteProxyRequest` callback.

---

## Custom Adapter Examples

When using a built-in dialect (`sqlite`, `postgresql`, `mysql`), you only need `run/get/all/exec` on your adapter — everything else is auto-filled. The examples below show what the built-in dialect provides, and are useful as reference if you need to customize or use an unsupported database.

### PostgreSQL (full manual adapter)

```js
import pg from 'pg';
const pool = new pg.Pool({ connectionString: '...' });

function pgConvert(sql) {
    let i = 0;
    return sql.replace(/\?/g, () => '$' + (++i));
}

const db = {
    async run(sql, p = []) {
        const r = await pool.query(pgConvert(sql), p);
        return { changes: r.rowCount };
    },
    async get(sql, p = []) {
        const r = await pool.query(pgConvert(sql), p);
        return r.rows[0] || null;
    },
    async all(sql, p = []) {
        const r = await pool.query(pgConvert(sql), p);
        return r.rows;
    },
    async exec(sql) { await pool.query(sql); },

    async beginTransaction() { await pool.query('BEGIN'); },
    async commit()           { await pool.query('COMMIT'); },
    async rollback()         { await pool.query('ROLLBACK'); },

    upsertSQL(table, columns, keyColumns) {
        let i = 0;
        const ph = columns.map(() => '$' + (++i)).join(', ');
        const cols = columns.join(', ');
        const keys = keyColumns.join(', ');
        const updates = columns
            .filter(c => !keyColumns.includes(c))
            .map(c => `${c} = EXCLUDED.${c}`);
        return updates.length === 0
            ? `INSERT INTO ${table} (${cols}) VALUES (${ph}) ON CONFLICT (${keys}) DO NOTHING`
            : `INSERT INTO ${table} (${cols}) VALUES (${ph}) ON CONFLICT (${keys}) DO UPDATE SET ${updates.join(', ')}`;
    },

    infraSchemaSQL() {
        return [
            `CREATE TABLE IF NOT EXISTS _sync_log (
                id BIGSERIAL PRIMARY KEY, table_name TEXT NOT NULL,
                operation TEXT NOT NULL, row_key TEXT NOT NULL,
                row_data TEXT, _hlc TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`,
            `CREATE INDEX IF NOT EXISTS idx_sync_log_id ON _sync_log (id)`,
            `CREATE TABLE IF NOT EXISTS _sync_peers (
                peer_key TEXT PRIMARY KEY, last_sync_id BIGINT DEFAULT 0,
                last_sync_at TIMESTAMPTZ)`,
            `CREATE TABLE IF NOT EXISTS _tombstones (
                table_name TEXT NOT NULL, row_key TEXT NOT NULL,
                _hlc TEXT NOT NULL, PRIMARY KEY (table_name, row_key))`,
            `CREATE TABLE IF NOT EXISTS _full_sync_needs (
                id BIGSERIAL PRIMARY KEY, phase TEXT NOT NULL, table_name TEXT,
                range_start TEXT, range_end TEXT, watermark_seq BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL)`,
            `CREATE INDEX IF NOT EXISTS idx_full_sync_needs_scope ON _full_sync_needs (phase, table_name)`,
            `CREATE TABLE IF NOT EXISTS _full_sync_claims (
                id BIGSERIAL PRIMARY KEY, phase TEXT NOT NULL, table_name TEXT,
                range_start TEXT, range_end TEXT, peer_node_id TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL)`,
            `CREATE INDEX IF NOT EXISTS idx_full_sync_claims_scope ON _full_sync_claims (phase, table_name)`,
            `CREATE INDEX IF NOT EXISTS idx_full_sync_claims_peer ON _full_sync_claims (peer_node_id)`,
        ];
    },

    // Optional: inject PG triggers so initTriggers() works
    // Note: must include HLC validation (same as built-in dialect)
    triggersSQL(tableName, def) {
        const keyExprNew = def.keyColumns.map(c => `'${c}', NEW.${c}`).join(', ');
        const keyExprOld = def.keyColumns.map(c => `'${c}', OLD.${c}`).join(', ');
        const dataExpr = def.dataColumns.map(c => `'${c}', NEW.${c}`).join(', ');
        return [
            // HLC validation: enforce valid, monotonically increasing _hlc
            `CREATE OR REPLACE FUNCTION _sync_trg_${tableName}_hlc_fn() RETURNS TRIGGER AS $$
             BEGIN
                 IF NEW._hlc IS NULL OR NEW._hlc = '' OR NEW._hlc = '0' THEN
                     RAISE EXCEPTION 'sync-lib: _hlc is required for % on ${tableName}', TG_OP;
                 END IF;
                 IF TG_OP = 'UPDATE' AND NEW._hlc <= OLD._hlc THEN
                     RAISE EXCEPTION 'sync-lib: _hlc must advance on UPDATE on ${tableName}';
                 END IF;
                 RETURN NEW;
             END; $$ LANGUAGE plpgsql`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc ON ${tableName}`,
            `CREATE TRIGGER _sync_trg_${tableName}_hlc
             BEFORE INSERT OR UPDATE ON ${tableName}
             FOR EACH ROW EXECUTE FUNCTION _sync_trg_${tableName}_hlc_fn()`,
            // Change logging
            `CREATE OR REPLACE FUNCTION _sync_trg_${tableName}_fn() RETURNS TRIGGER AS $$
             BEGIN
                 IF TG_OP = 'DELETE' THEN
                     INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
                     VALUES ('${tableName}', 'DELETE', json_build_object(${keyExprOld})::text, NULL, OLD._hlc);
                     RETURN OLD;
                 ELSE
                     INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
                     VALUES ('${tableName}', TG_OP, json_build_object(${keyExprNew})::text, json_build_object(${dataExpr})::text, NEW._hlc);
                     RETURN NEW;
                 END IF;
             END; $$ LANGUAGE plpgsql`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName} ON ${tableName}`,
            `CREATE TRIGGER _sync_trg_${tableName}
             AFTER INSERT OR UPDATE OR DELETE ON ${tableName}
             FOR EACH ROW EXECUTE FUNCTION _sync_trg_${tableName}_fn()`,
        ];
    },
    dropTriggersSQL(tableName) {
        return [
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc ON ${tableName}`,
            `DROP FUNCTION IF EXISTS _sync_trg_${tableName}_hlc_fn`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName} ON ${tableName}`,
            `DROP FUNCTION IF EXISTS _sync_trg_${tableName}_fn`,
        ];
    },
};
```

---

### MySQL (full manual adapter)

```js
import mysql from 'mysql2/promise';
const pool = await mysql.createPool({ host: '...', user: '...', password: '...', database: '...' });

const db = {
    async run(sql, p = []) {
        const [r] = await pool.execute(sql, p);
        return { changes: r.affectedRows };
    },
    async get(sql, p = []) {
        const [rows] = await pool.execute(sql, p);
        return rows[0] || null;
    },
    async all(sql, p = []) {
        const [rows] = await pool.execute(sql, p);
        return rows;
    },
    async exec(sql) { await pool.query(sql); },

    async beginTransaction() { await pool.query('START TRANSACTION'); },
    async commit()           { await pool.query('COMMIT'); },
    async rollback()         { await pool.query('ROLLBACK'); },

    upsertSQL(table, columns, keyColumns) {
        const cols = columns.join(', ');
        const ph = columns.map(() => '?').join(', ');
        const updates = columns
            .filter(c => !keyColumns.includes(c))
            .map(c => `${c} = VALUES(${c})`);
        return updates.length === 0
            ? `INSERT IGNORE INTO ${table} (${cols}) VALUES (${ph})`
            : `INSERT INTO ${table} (${cols}) VALUES (${ph}) ON DUPLICATE KEY UPDATE ${updates.join(', ')}`;
    },

    infraSchemaSQL() {
        return [
            `CREATE TABLE IF NOT EXISTS _sync_log (
                id BIGINT AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(255) NOT NULL,
                operation VARCHAR(10) NOT NULL, row_key TEXT NOT NULL,
                row_data LONGTEXT, _hlc VARCHAR(64) NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
            `CREATE TABLE IF NOT EXISTS _sync_peers (
                peer_key VARCHAR(255) PRIMARY KEY, last_sync_id BIGINT DEFAULT 0,
                last_sync_at DATETIME)`,
            `CREATE TABLE IF NOT EXISTS _tombstones (
                table_name VARCHAR(255) NOT NULL, row_key VARCHAR(1024) NOT NULL,
                _hlc VARCHAR(64) NOT NULL, PRIMARY KEY (table_name, row_key(255)))`,
            `CREATE TABLE IF NOT EXISTS _full_sync_needs (
                id BIGINT AUTO_INCREMENT PRIMARY KEY, phase VARCHAR(16) NOT NULL,
                table_name VARCHAR(255), range_start TEXT, range_end TEXT,
                watermark_seq BIGINT NOT NULL, updated_at DATETIME NOT NULL,
                INDEX idx_full_sync_needs_scope (phase, table_name))`,
            `CREATE TABLE IF NOT EXISTS _full_sync_claims (
                id BIGINT AUTO_INCREMENT PRIMARY KEY, phase VARCHAR(16) NOT NULL,
                table_name VARCHAR(255), range_start TEXT, range_end TEXT,
                peer_node_id VARCHAR(255) NOT NULL, expires_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                INDEX idx_full_sync_claims_scope (phase, table_name),
                INDEX idx_full_sync_claims_peer (peer_node_id))`,
        ];
    },

    // Optional: inject MySQL triggers so initTriggers() works
    // Note: must include HLC validation (same as built-in dialect)
    triggersSQL(tableName, def) {
        const keyExpr = def.keyColumns.map(c => `'${c}', NEW.${c}`).join(', ');
        const dataExpr = def.dataColumns.map(c => `'${c}', NEW.${c}`).join(', ');
        const delKeyExpr = def.keyColumns.map(c => `'${c}', OLD.${c}`).join(', ');
        return [
            // HLC validation: enforce valid, monotonically increasing _hlc
            `CREATE TRIGGER _sync_trg_${tableName}_hlc_insert
BEFORE INSERT ON ${tableName} FOR EACH ROW
BEGIN
    IF NEW._hlc IS NULL OR NEW._hlc = '' OR NEW._hlc = '0' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sync-lib: _hlc is required for INSERT on ${tableName}';
    END IF;
END`,
            `CREATE TRIGGER _sync_trg_${tableName}_hlc_update
BEFORE UPDATE ON ${tableName} FOR EACH ROW
BEGIN
    IF NEW._hlc IS NULL OR NEW._hlc = '' OR NEW._hlc = '0' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sync-lib: _hlc is required for UPDATE on ${tableName}';
    END IF;
    IF NEW._hlc <= OLD._hlc THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sync-lib: _hlc must advance on UPDATE on ${tableName}';
    END IF;
END`,
            // Change logging
            `CREATE TRIGGER _sync_trg_${tableName}_insert
AFTER INSERT ON ${tableName} FOR EACH ROW
INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
VALUES ('${tableName}', 'INSERT', JSON_OBJECT(${keyExpr}), JSON_OBJECT(${dataExpr}), NEW._hlc)`,
            `CREATE TRIGGER _sync_trg_${tableName}_update
AFTER UPDATE ON ${tableName} FOR EACH ROW
INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
VALUES ('${tableName}', 'UPDATE', JSON_OBJECT(${keyExpr}), JSON_OBJECT(${dataExpr}), NEW._hlc)`,
            `CREATE TRIGGER _sync_trg_${tableName}_delete
AFTER DELETE ON ${tableName} FOR EACH ROW
INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
VALUES ('${tableName}', 'DELETE', JSON_OBJECT(${delKeyExpr}), NULL, OLD._hlc)`,
        ];
    },
    dropTriggersSQL(tableName) {
        return [
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc_insert`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc_update`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_insert`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_update`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_delete`,
        ];
    },
};
```

---

## API Reference

### SyncEngine

| Method | Description |
|--------|-------------|
| `registerTable(name, def)` | Register a business table for sync |
| `initSchema()` | Create infrastructure tables (`_sync_log`, `_sync_peers`, `_tombstones`, `_full_sync_needs`, `_full_sync_claims`) |
| `initTriggers(opts?)` | Create HLC + change-log triggers (dialect-aware). Pass `{ changeLog: false }` to install only HLC integrity triggers, or `{ hlc: false }` to install only change-log triggers |
| `dropTriggers(opts?)` | Inverse of `initTriggers()`; same options |
| `initChangeSubscriptions()` | Invoke the `subscribeToChanges` callback provided at construction; must be called after `initSchema()` and before `start()` |
| `start()` / `stop()` | Start/stop leader election |
| `peerConnected(id, opts)` | Notify: new peer connected |
| `receiveMessage(id, raw)` | Notify: message received from peer |
| `peerDisconnected(id)` | Notify: peer disconnected |
| `notifyLocalWrite()` | Push local changes to all peers |
| `write(fn, shardKey?)` | **High-level 2PC API**: execute fn in a 2PC transaction, engine handles everything automatically |
| `beginManualTransaction(id)` | Low-level: start 2PC transaction |
| `getManualTransactionEntries(id)` | Low-level: read sync_log entries from current txn |
| `commitManualTransaction(id)` | Low-level: commit 2PC transaction |
| `rollbackManualTransaction(id)` | Low-level: rollback 2PC transaction |
| `waitForPrepareAck(writeId, entries, term, shardId)` | Low-level 2PC: broadcast prepare, wait for acks |
| `broadcastCommit(writeId)` | Low-level 2PC: broadcast commit |
| `broadcastAbort(writeId, reason)` | Low-level 2PC: broadcast abort |
| `proxyRequest(key, payload)` | Forward write request to leader |
| `getShardId(key)` | Get shard ID for a key |
| `isLeaderForShard(shardId)` | Check if local node is leader for shard |
| `isLeaderForKey(key)` | Check if local node is leader for key |
| `getLeaderForShard(shardId)` | Get the current leader node ID for a shard |
| `getLeaderForKey(key)` | Get the current leader node ID for a key |
| `triggerElection()` | Immediately trigger a new election on all shards using the standard election flow. Typically called after updating `getMinQuorum` or other configuration. The current leader steps down first (fires `onLeaderChanged(null)`) then participates as a candidate. |
| `isLeaseValid(shardId?)` | Returns `true` if the local node is the leader for the given shard (default: 0) and its Leader Lease has not expired. Safe to use as a guard for local reads without a quorum round-trip. |
| `isLeaseValidForKey(key)` | Same as `isLeaseValid` but looks up the shard from the provided key string. |
| `hasPeers()` | Returns `true` if at least one peer is currently connected. |
| `getClockSync(peerId?, unit?)` | Get bidirectional time sync result for a peer. Returns `{ rtt, offset, epsilon, samples, lastSyncAt }` where each time value is `{ value, unit }`. `unit` defaults to `'ms'`. Without `peerId` returns a `Map<peerId, result>` for all peers with samples. Returns `null` if no measurements yet. |
| `getStatus()` | Get engine status (nodeId, shards, peers) |

---

## Bidirectional Time Sync

Every heartbeat `ping` the engine sends carries `seq` and `t1` (the local send timestamp). The receiving peer responds with a `pong` containing the original `t1` plus its own receive time `t2` and send time `t3`. When the initiator receives the `pong`, it records `t4` and computes:

```
rtt    = (t4 - t1) - (t3 - t2)          // network round-trip time (peer processing excluded)
offset = ((t2 - t1) + (t3 - t4)) / 2   // clock offset (positive = peer clock is ahead)
epsilon = minRtt / 2                     // error bound
```

Results accumulate in a per-peer sliding window (`timeSyncWindowSize`, default 8). The sample with the minimum RTT is always selected as the best estimate (lowest measurement error).

```js
// Read sync result for a specific peer (defaults to ms)
const sync = engine.getClockSync('peer-1');
// sync = {
//   rtt:     { value: 4.2, unit: 'ms' },
//   offset:  { value: 1.1, unit: 'ms' },   // positive = peer is 1.1ms ahead
//   epsilon: { value: 2.1, unit: 'ms' },   // error bound
//   samples: 8,
//   lastSyncAt: 1713000045000,             // ms timestamp of last measurement
// }

// Or request a specific unit
const syncUs = engine.getClockSync('peer-1', 'us');

// Or get all peers at once
const allSync = engine.getClockSync();  // Map<peerId, result>
```

Since time sync piggybacks on heartbeat pings, no extra timer or message type is needed. The effective sync interval equals `heartbeatIntervalMs` (default 15 s).

---

## Receiver-Initiated Full Resync (v1.1.0+)

### The problem

`_sync_log` is reaped according to TTL; if a peer has been offline long enough its `lastSyncId` falls below local `minLogId` and plain incremental pull silently skips cleaned entries. Additionally, in an LWW key-space every peer covers the full `(-∞, +∞]` range — only the upper watermark (`lastSeq`) differs. So "I'm behind peer X" really means "peer X has newer data somewhere in the full key space."

### The model

1. **Outbound connect triggers fetch** — On each outbound handshake whose `peer.lastSeq > localMaxSeq`, the receiver upserts a need of scope `(-∞, +∞]` for **each registered table + tombstones**, with `watermark = max(existing, peer.lastSeq)`. A second outbound peer that's also ahead merely bumps the watermark — no duplicate needs, since all peers cover the same key space.
2. **Interval-set needs** — `_full_sync_needs` is peer-agnostic: rows track `(phase, table_name, range_start, range_end, watermark_seq)`, with `null` bounds meaning ±∞. After partial delivery a single need may split into multiple fragments (gaps = already-fetched).
3. **Per-peer claims** — `_full_sync_claims` holds in-flight reservations: `(phase, table_name, range_start, range_end, peer_node_id, expires_at)` with a TTL (`fullSyncClaimTtlMs`, default 10 min). The dispatcher computes "unclaimed fragments = need − (active claims)" and assigns one fragment per free peer so parallel fetches don't overlap.
4. **One request = one page** — Sender receives `full_sync_resume_req{request:{phase,tableName,rangeStart,rangeEnd,watermarkSeq}}`, scans a single page bounded by `fullSyncChunkSize` and clipped to `rangeEnd`, and replies with one `full_sync_rows` / `full_sync_tombstones` (carrying explicit `pageStart`, `pageEnd`) plus one `full_sync_complete{request}`. The remainder is re-dispatched on the next tick.
5. **Transactional subtract** — On page arrival, the receiver applies rows/tombstones **and** subtracts `(pageStart, pageEnd]` from the matching need in the **same transaction**. Data and need-state advance or roll back together. Pages commit independently.
6. **Claim release + re-dispatch** — `full_sync_complete` deletes only that `(peer, phase, table)` claim (not other scopes). Peer disconnect deletes all claims held by that peer. The cleanup tick reaps expired claims. **Synchronous send-failure rollback**: if `onSendToPeer` throws while the dispatcher is issuing a `resume_req`, the claim is immediately deleted and the same fragment is retried against the next free peer in the same tick (peer marked dead for this cycle). TTL expiry remains the last-resort safety net for silent hangs. Any of these events re-run the dispatcher so leftover fragments go to a free peer.
7. **Watermark promotion** — Once all fragments for all scopes are drained, the needs table is empty and `lastSyncId` advances via the usual incremental path.

### Relevant configuration

| Option | Default | Description |
|--------|---------|-------------|
| `fullSyncChunkSize` | 500 | Rows per page. Bounds peak memory and per-message I/O. |
| `enableFullResync` | `true` | Set `false` to fall back to incremental-only (comparison only — hits the silent-loss bug). |
| `fullSyncClaimTtlMs` | 600_000 | TTL for in-flight `_full_sync_claims`. A peer that stops responding releases its slots after this window. |

### Infrastructure tables

| Table | Purpose |
|-------|---------|
| `_full_sync_needs` | Receiver-maintained, peer-agnostic interval set. Rows: `(id, phase, table_name, range_start, range_end, watermark_seq, updated_at)`. One row per disjoint still-needed range in a scope. |
| `_full_sync_claims` | Receiver-maintained, per-peer in-flight reservations. Rows: `(id, phase, table_name, range_start, range_end, peer_node_id, expires_at, created_at)`. Deleted on `full_sync_complete`, peer disconnect, or TTL expiry. |

---

## Leader Lease

Once the leader wins an election and after each subsequent heartbeat, it computes a local lease:

```
leaseDuration = electionMin − ε
```

- `electionMin` — shortest possible follower election timeout (derived from RTT or static default)
- `ε` (epsilon) — clock error bound from time sync (`minRtt / 2` of the peer with the largest uncertainty)

**Safety guarantee:** Every follower waits at least `electionMin` of its own clock time before starting a new election. Even if a follower's clock is `ε` ahead of the leader's, the leader's lease (`electionMin − ε`) expires before the follower can fire. No new leader can be elected while the lease is valid.

```js
// On the leader: serve a local read without a quorum round-trip
if (engine.isLeaseValid()) {
    return db.get('SELECT * FROM accounts WHERE id = ?', [userId]);
}
// Lease expired or not leader — fall back to normal quorum read
```

```js
// Key-based variant
if (engine.isLeaseValidForKey(userId)) { ... }
```

The lease is invalidated immediately when the leader steps down (`_leaseDurationMs = 0`). If no time sync data is available yet (`ε = Infinity`), `isLeaseValid()` returns `false` — the lease is never granted until at least one round of time sync has completed.

### Standalone utilities

```js
import { HLC, DIALECTS, applyDialect, logChange, applyEntries } from 'sync-lib';
```

| Export | Description |
|--------|-------------|
| `HLC` | Hybrid Logical Clock class |
| `LeaderElection` | Leader election state machine |
| `DIALECTS` | Built-in dialect definitions (sqlite, postgresql, mysql). Each dialect provides `infraSchemaSQL`, `upsertSQL`, `triggersSQL`, `dropTriggersSQL` |
| `applyDialect(db, name)` | Apply a dialect's methods to a db adapter |
| `validateDialectMethods(db)` | Validate all required dialect methods exist |
| `logChange(db, table, op, key, data, hlc)` | Manually log a change (alternative to triggers) |
| `applyEntries(db, entries, hlc, registry)` | Apply remote entries idempotently |

---

## DatabaseAdapter Interface

```
┌───────────────────────────────────────────────────────────────┐
│  Always required (you must implement):                         │
│                                                               │
│  db.run(sql, params)           → { changes: number }          │
│  db.get(sql, params)           → Object | null                │
│  db.all(sql, params)           → Object[]                     │
│  db.exec(sql)                  → void                         │
│                                                               │
├───────────────────────────────────────────────────────────────┤
│  Auto-filled by dialect (or manually required):               │
│                                                               │
│  db.beginTransaction()         → void                         │
│  db.commit()                   → void                         │
│  db.rollback()                 → void                         │
│  db.upsertSQL(table, cols, keyColumns) → string               │
│  db.infraSchemaSQL()           → string[]                     │
│  db.triggersSQL(table, def)    → string[]                     │
│  db.dropTriggersSQL(table)     → string[]                     │
└───────────────────────────────────────────────────────────────┘
```

With `dialect: 'sqlite' | 'postgresql' | 'mysql'`, the bottom 7 methods are auto-filled. Without a dialect, you must implement all of them yourself.

SQL uses `?` placeholders. If your database uses `$1/$2` (e.g. PostgreSQL), convert inside `run/get/all`.

---

## Table Requirements

Every synced table **must** have an `_hlc` column:

```sql
CREATE TABLE your_table (
    id   TEXT PRIMARY KEY,
    data TEXT,
    _hlc TEXT NOT NULL DEFAULT '0'    -- required by sync-lib
);
```

---

## Architecture

```
sync-lib/
├── index.js              ← Unified exports
├── hlc.js                ← Hybrid Logical Clock (pure algorithm)
├── leader-election.js    ← Raft-simplified election state machine
├── sync-protocol.js      ← Message codec + idempotent data apply
├── sync-engine.js        ← Main engine (composes all above)
└── dialects.js           ← Built-in SQL dialects (SQLite/PG/MySQL)
```

**Design principles**:
- No dependency on any transport (WebSocket, gRPC, TCP)
- No dependency on any database driver
- No dependency on Node.js APIs (fs, os, http)
- Timer functions injected via `timerAPI` (no direct use of global `setTimeout`/`setInterval`)
- Time acquisition injected via `getNow` (no direct use of `Date.now()` / `new Date()`)
- `getNow` returns `{ value: number, unit: 's' | 'ms' | 'us' | 'ns' }`, internally normalized to milliseconds
- All external capabilities injected via callbacks

---

## License

xSync Source Available License 1.0 — see [LICENSE](LICENSE) and [LICENSING.md](LICENSING.md).
