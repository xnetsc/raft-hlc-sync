# sync-lib

零依赖、数据库无关的分布式数据同步引擎，内置 HLC（混合逻辑时钟）冲突解决、Raft 简化版 Leader 选举和两阶段提交（2PC）。

[English](./README.md)

---

## 特性

- **数据库无关** — 通过 DatabaseAdapter 接口支持 SQLite、PostgreSQL、MySQL 或任何 SQL 数据库。
- **传输无关** — 不内置 WebSocket/gRPC/TCP，通过回调注入传输层。
- **零 npm 依赖** — 纯 JavaScript，无原生模块。
- **HLC 冲突解决** — 基于混合逻辑时钟的 Last-Write-Wins 策略，墓碑机制防止已删除数据复活。
- **Leader 选举** — Raft 简化版的分片级选举，基于 term 投票。
- **2PC** — 需要时可使用两阶段提交实现多节点强一致性写入。
- **定时器注入（必须）** — `timerAPI` 为必填项。引擎不直接调用全局 `setTimeout`/`setInterval`，所有定时器均通过注入的函数调用，周期性任务由引擎内部自行调度。
- **时间函数注入（必须）** — `getNow` 为必填项。引擎不直接调用 `Date.now()` 或 `new Date()`，所有时间获取均通过外部注入的 `getNow` 函数。`getNow` 返回 `{ value: number, unit: 's' | 'ms' | 'us' | 'ns' }`，内部统一转换为毫秒。
- **双向对时（内置）** — 每次心跳 ping/pong 同时携带 NTP 风格的时间戳（t1/t2/t3/t4），引擎自动计算每个 peer 的 RTT 和时钟偏差（滑动窗口 + 最小 RTT 选取），通过 `engine.getClockSync(peerId?, unit?)` 读取结果。
- **网络感知选举** — 选举超时和候选优先级自动随测量到的 RTT 调整。低延迟 peer 获得更短的超时和更高的候选优先级，在快速网络上加速收敛，同时防止高延迟/广域链路上的误判选举。
- **Leader 租约（Leader Lease）** — Leader 当选及后续每次心跳时计算一个安全的租约时长（`electionMin − ε`），并设置本地计时器。租约有效期内可保证本节点是集群中唯一的 Leader。通过 `engine.isLeaseValid(shardId?)` / `engine.isLeaseValidForKey(key)` 判断是否可以不经 quorum 直接服务本地读。
- **动态 2PC/代理超时** — `prepareTimeoutMs`、`proxyWaitMs`、`proxyTimeoutMs` 作为静态下界配置。当 RTT 测量数据可用时，引擎自动放大（如 `max(prepareTimeoutMs, rtt×4)`），避免高延迟链路上写入超时误触发。
- **HLC 时钟偏差上限检测** — 接收到的同步条目若远端 `wallTime` 超过 `localNow + maxClockSkewMs`（默认 300 秒），该条目被静默跳过，防止时钟异常节点将本地 HLC 推进到不合理的未来时刻。
- **单调时钟（`memonicNow`）** — 可选。提供后，引擎在构造时各采样一次 `getNow()` 和 `memonicNow()` 作为基准，后续每次取时间计算 `startNow + (memonicNow() − startMemonic)`，保证时间严格单调递增，对 NTP 校准、DST、手动时钟调整完全免疫。
- **接收方主导的全量重同步（v1.1.0+）** — 每次主动连接（outbound）到一个 `lastSeq > localMaxSeq` 的 peer，接收方就为每张已注册业务表 + 墓碑登记一条 `(-∞, +∞]` 的 need（LWW key 空间模型：所有 peer 覆盖相同的全 key 范围，只有上界水位不同）。调度器把这条 need 切成互不重叠的子区间，并发分派给多个可用 outbound peer。
- **区间集 needs + per-peer claims（v1.1.0+）** — `_full_sync_needs` 保存"还没拿到的数据"的区间（peer-agnostic），`_full_sync_claims` 保存"在途取数"的认领（per-peer）。每成功应用一页数据，就在**同一事务**里把 `(pageStart, pageEnd]` 从对应 need 中挖掉 —— 挖光产生空洞是正常的，空洞代表"已经拿到过、无需再取"。调度器在 pages 到达、`full_sync_complete`、peer 连入、peer 断连、cleanup tick 时都会重新触发。发送异常会立即回滚 claim 并换 peer 重试；claim TTL 只作为"对端静默卡死"的最后兜底，且**随 per-peer RTT 动态收缩** —— `effectiveTtl = min(fullSyncClaimTtlMs, max(fullSyncClaimTtlFloorMs, rtt × fullSyncClaimTtlRttFactor))`，LAN 上（rtt≈10ms）几秒（由 floor 决定，默认 5s）就能回收，WAN 放宽到几十秒；还没 RTT 样本的新 peer 退化回静态 `fullSyncClaimTtlMs`（默认 60 秒——当今网络下任何健康节点都不该 30 秒还返回不了一页数据）。
- **一个请求一页** — 每个 `full_sync_resume_req{request:{phase,tableName,rangeStart,rangeEnd,watermarkSeq}}` 只返回一页数据（由 `fullSyncChunkSize` 截断，默认 500）+ 一条 `full_sync_complete{request}`，`request` 回传让接收方释放**对应那一个** claim。剩余区间下一次调度再发（可能发给另一个 peer）。
- **HLC LWW 幂等应用** — 行与墓碑按 HLC LWW + 墓碑优先规则应用；并发重复、重连重传都 100% 幂等。分页游标基于业务主键，OR 展开 WHERE，跨方言可移植。

---

## 安装

```bash
npm install sync-lib

# 或直接复制目录
cp -r sync-lib/ your-project/sync-lib/
```

---

## 快速开始

### 第 1 步：实现 DatabaseAdapter

适配器只需提供基础查询方法。方言相关的 SQL（事务、upsert、schema、触发器）由 `dialect` 参数自动处理。

**SQLite：**

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

**PostgreSQL：**

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

**MySQL：**

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

只需 4 个方法。`dialect` 参数（见第 2 步）会自动填充其余所有方言方法。

> 自定义数据库请参见下方 [DatabaseAdapter 接口](#databaseadapter-接口) 和 [自定义适配器示例](#自定义适配器示例)。

#### 内置方言支持

sync-lib 内置 **SQLite**、**PostgreSQL** 和 **MySQL** 三种方言。指定 `dialect: 'sqlite'`（或 `'postgresql'` / `'mysql'`）后，以下方法会自动填充到你的适配器上：

- `beginTransaction()` / `commit()` / `rollback()` — 事务控制
- `upsertSQL(table, columns, keyColumns)` — 冲突感知的 insert/update SQL
- `infraSchemaSQL()` — 内部表（`_sync_log`、`_sync_peers`、`_tombstones`、`_full_sync_needs`、`_full_sync_claims`）的 DDL
- `triggersSQL(tableName, def)` / `dropTriggersSQL(tableName)` — 自动记录变更的触发器

**如果不指定 dialect（或指定不支持的值），必须自行实现以上所有方法。** 缺失方法会在构造时报错。

你也可以在 `db` 适配器上显式定义任何方法来覆盖内置实现 — 你的显式实现始终优先于内置方言。

### 第 2 步：创建引擎并注册表

```js
import { SyncEngine } from 'sync-lib';

const engine = new SyncEngine({
    nodeId: 'node-1',
    db,
    dialect: 'sqlite',   // 或 'postgresql' 或 'mysql'

    // 必需回调
    onSendToPeer:    (peerId, msg) => yourTransport.send(peerId, msg),
    onClosePeer:     (peerId)      => yourTransport.close(peerId),
    onLeaderChanged: (shardId, leaderId, isLocal) => {
        console.log(`分片 ${shardId}: leader=${leaderId} local=${isLocal}`);
    },

    // 必需：任何节点都可能当选 Leader，必须能处理 Follower 转发来的写请求
    onExecuteProxyRequest: async (payload) => {
        // 在本地执行写请求并返回结果
        return await yourBusinessLogic(payload);
    },

    // 可选回调
    onWriteCompleted: () => engine.notifyLocalWrite(),
    onError: (ctx, err) => console.error(`[sync] ${ctx}:`, err.message),

    // 可选：调度间隔
    heartbeatIntervalMs: 15000,    // 默认 15s（同时也是双向对时的间隔）
    pullIntervalMs:      10000,    // 默认 10s
    cleanupIntervalMs:   3600000,  // 默认 1h
    timeSyncWindowSize:  8,        // 每个 peer 的 RTT 滑动窗口大小（默认 8）

    // 必填：注入时间函数 — 返回 { value, unit }，引擎内部不使用 Date.now()
    // 支持的单位：'s'（秒）、'ms'（毫秒）、'us'（微秒）、'ns'（纳秒）
    getNow: () => ({ value: Date.now(), unit: 'ms' }),

    // 可选：单调时钟来源。提供后，引擎在构造时各采样一次基准值，后续每次取时间计算
    //   _nowMs() = startNow + (memonicNow() − startMemonic)
    // 保证内部时间戳严格单调递增，不受系统时钟调整影响。
    // Node.js 典型用法：
    memonicNow: () => ({ value: Number(process.hrtime.bigint()), unit: 'ns' }),

    // 可选：HLC 接收时钟偏差上限，默认 300000ms（5 分钟）。
    // 远端条目的 wallTime 若超出 localNow + maxClockSkewMs，该条目被静默跳过，
    // 防止时钟异常节点将本地 HLC 推进到不合理的未来时刻。
    maxClockSkewMs: 300000,

    // 可选：动态超时下界（有 RTT 数据时自动放大）
    prepareTimeoutMs:      5000,   // 2PC prepare 超时  — 实际值 = max(this, rtt×4)
    proxyWaitMs:           6000,   // 等待 Leader 超时  — 实际值 = max(this, electionMin + hbInterval)
    proxyTimeoutMs:        10000,  // 代理请求超时       — 实际值 = max(this, rtt×6)

    // 必填：注入定时器函数
    timerAPI: { setTimeout, clearTimeout, setInterval, clearInterval },
});

engine.registerTable('users', {
    keyColumns:  ['user_id'],
    dataColumns: ['user_id', 'name', 'email', '_hlc'],   // 必须包含 _hlc
    // registerTable 会校验：keyColumns 非空、dataColumns 非空、dataColumns 包含 '_hlc'
    validator:   (row) => { if (!row.user_id) throw new Error('缺少 user_id'); },
});

engine.initSchema();     // 创建 _sync_log、_sync_peers、_tombstones、_full_sync_needs、_full_sync_claims
engine.initTriggers();   // 创建同步触发器（方言感知）
engine.start();          // 启动 Leader 选举 + 内部自动调度 tickPull/tickHeartbeat/tickCleanup
```

### 变更检测来源

引擎必须至少配置一种变更检测来源；两者可并存：

1. **SQL 触发器**（默认）— `initTriggers()` 安装 AFTER INSERT/UPDATE/DELETE 触发器把变更写入 `_sync_log`，同时安装 HLC 完整性触发器（非空、单调递增）。
2. **外部回调** — 适用于无法/不便使用触发器的场景（外部 CDC、业务层 hook、数据库日志尾随等）。

   **设计原则**：sync-lib **驱动** 订阅。外部应用只提供这一个回调，既不需要、也无法直接知道 sync-lib 关心哪些表。sync-lib 从自己的表注册表（`registerTable` 声明的）出发，对每张表调用一次回调，告诉外部："请监听这张表（用这个 where 过滤），数据变更时调用我传给你的 `onChanges`"。

   **过滤条件位置**：每张表的 where 配置在 `registerTable(name, { ..., where })` 里。`initChangeSubscriptions()` 会把它透传给回调。SQL 触发器忽略 where（触发器始终覆盖全表）。

   构造选项：
   - `subscribeToChanges: ({ table, where, keyColumns, dataColumns, onChanges }) => void`

`start()` 在两种来源都未配置时抛错。HLC 完整性触发器独立，可通过 `initTriggers({ changeLog: false })` 只安装它。

```js
const engine = new SyncEngine({
    // ...
    subscribeToChanges: ({ table, where, onChanges }) => {
        // sync-lib 对每张已注册表调用一次本回调。外部只从参数里知道"本次要监听哪张表、
        // 用什么过滤条件"，不会、也无法直接访问 sync-lib 的表清单。
        myCdc.subscribe(table, where, (op, info) => {
            onChanges(
                [op],                                    // 'INSERT' | 'UPDATE' | 'DELETE'
                [{
                    tableName: table,
                    rowKey:    info.key,                 // 主键对象
                    rowData:   op === 'DELETE' ? null : info.row,
                    hlc:       info.hlc,                 // HLC 时间戳字符串
                }]
            );
        });
    },
});

engine.registerTable('users',  { keyColumns: ['user_id'],  dataColumns: [...], where: 'deleted_at IS NULL' });
engine.registerTable('orders', { keyColumns: ['order_id'], dataColumns: [...], where: 'status != "cancelled"' });
engine.registerTable('audit',  { keyColumns: ['id'],       dataColumns: [...] });   // 不过滤

engine.initSchema();
engine.initTriggers({ changeLog: false }); // 只装 HLC 完整性触发器；变更记录来自回调
engine.initChangeSubscriptions();              // 对每张已注册表同步调用一次 subscribeToChanges
engine.start();
```


### 第 3 步：接入传输层

sync-lib 不关心你使用什么协议，只需调用以下 3 个方法：

```js
// 当对等节点连接时
engine.peerConnected(peerId, { direction: 'inbound' });

// 当收到消息时
engine.receiveMessage(peerId, rawString);

// 当对等节点断开时
engine.peerDisconnected(peerId);
```

sync-lib 通过你提供的 `onSendToPeer(peerId, msg)` 回调向对等节点发送消息。

### 第 4 步：写入数据

> **重要：** 对已注册表的所有写操作（INSERT/UPDATE/DELETE）**必须**通过 `engine.write()` 或手动使用 `engine.beginManualTransaction()` / `commitManualTransaction()` 包裹。**不要**在这些包裹之外直接调用 `db.run(...)` 写入——引擎需要管理事务和 2PC 协调，以确保数据正确同步到其他节点。

**推荐：`engine.write()`（高阶 API）**

引擎自动处理完整生命周期：BEGIN → 执行 → 触发器记录 → 2PC prepare/ack → COMMIT → 广播 commit → 通知对等节点。

```js
const result = await engine.write(async (db) => {
    const ts = engine.hlc.tick();
    db.run('INSERT INTO users (user_id, name, _hlc) VALUES (?, ?, ?)',
        ['u1', 'Alice', ts]);
    return { userId: 'u1' };  // 返回值透传给调用方
}, userId);  // shardKey 用于路由
```

**低阶 API（需要精细控制时）**

如果需要更精细的控制（如在 prepare 前读取 entries），可使用低阶 API：

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

## 写代理

非 Leader 节点可将写操作转发给 Leader：

```js
if (!engine.isLeaderForKey(userId)) {
    const result = await engine.proxyRequest(userId, {
        method: 'POST', path: '/api/users', body: reqBody,
    });
    return res.status(result.status).json(result.body);
}
// 作为 Leader 本地处理...
```

Leader 通过 `onExecuteProxyRequest` 回调执行请求。

---

## 自定义适配器示例

使用内置方言（`sqlite`、`postgresql`、`mysql`）时，适配器只需 `run/get/all/exec` 四个方法。以下完整示例供参考，适用于需要自定义或使用不支持的数据库的场景。

### PostgreSQL（完整手动适配器）

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

    // 可选：注入 PG 触发器，使 initTriggers() 自动创建
    // 注意：必须包含 HLC 校验逻辑（与内置方言一致）
    triggersSQL(tableName, def) {
        const keyExprNew = def.keyColumns.map(c => `'${c}', NEW.${c}`).join(', ');
        const keyExprOld = def.keyColumns.map(c => `'${c}', OLD.${c}`).join(', ');
        const dataExpr = def.dataColumns.map(c => `'${c}', NEW.${c}`).join(', ');
        return [
            // HLC 校验：强制要求 _hlc 有效且单调递增
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
            // 变更记录
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

### MySQL（完整手动适配器）

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

    // 可选：注入 MySQL 触发器，使 initTriggers() 自动创建
    // 注意：必须包含 HLC 校验逻辑（与内置方言一致）
    triggersSQL(tableName, def) {
        const keyExpr = def.keyColumns.map(c => `'${c}', NEW.${c}`).join(', ');
        const dataExpr = def.dataColumns.map(c => `'${c}', NEW.${c}`).join(', ');
        const delKeyExpr = def.keyColumns.map(c => `'${c}', OLD.${c}`).join(', ');
        return [
            // HLC 校验：强制要求 _hlc 有效且单调递增
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
            // 变更记录
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

## API 参考

### SyncEngine

| 方法 | 说明 |
|------|------|
| `registerTable(name, def)` | 注册业务表用于同步 |
| `initSchema()` | 创建基础设施表（`_sync_log`、`_sync_peers`、`_tombstones`、`_full_sync_needs`、`_full_sync_claims`） |
| `initTriggers(opts?)` | 创建 HLC + 变更记录触发器（方言感知）。传 `{ changeLog: false }` 只装 HLC 完整性触发器；传 `{ hlc: false }` 只装变更记录触发器 |
| `dropTriggers(opts?)` | `initTriggers()` 的反操作，参数相同 |
| `initChangeSubscriptions()` | 调用构造时传入的 `subscribeToChanges` 回调；必须在 `initSchema()` 之后、`start()` 之前调用 |
| `start()` / `stop()` | 启动/停止 Leader 选举 |
| `peerConnected(id, opts)` | 通知：新的对等节点已连接 |
| `receiveMessage(id, raw)` | 通知：收到对等节点消息 |
| `peerDisconnected(id)` | 通知：对等节点已断开 |
| `notifyLocalWrite()` | 将本地变更推送到所有对等节点 |
| `write(fn, shardKey?)` | **高阶 2PC API**：在 2PC 事务中执行 fn，引擎自动处理全部流程 |
| `beginManualTransaction(id)` | 低阶：开始 2PC 事务 |
| `getManualTransactionEntries(id)` | 低阶：读取当前事务的 sync_log 条目 |
| `commitManualTransaction(id)` | 低阶：提交 2PC 事务 |
| `rollbackManualTransaction(id)` | 低阶：回滚 2PC 事务 |
| `waitForPrepareAck(writeId, entries, term, shardId)` | 低阶 2PC：广播 prepare，等待确认 |
| `broadcastCommit(writeId)` | 低阶 2PC：广播 commit |
| `broadcastAbort(writeId, reason)` | 低阶 2PC：广播 abort |
| `proxyRequest(key, payload)` | 将写请求转发给 Leader |
| `getShardId(key)` | 获取 key 对应的分片 ID |
| `isLeaderForShard(shardId)` | 检查本节点是否为该分片的 Leader |
| `isLeaderForKey(key)` | 检查本节点是否为该 key 的 Leader |
| `getLeaderForShard(shardId)` | 获取该分片当前 Leader 的节点 ID |
| `getLeaderForKey(key)` | 获取该 key 所在分片当前 Leader 的节点 ID |
| `triggerElection()` | 立即触发所有分片重新选举，走标准选举流程。通常在更新 `getMinQuorum` 等配置参数后调用。若当前节点是 Leader，先正常降级（触发 `onLeaderChanged(null)`），再以候选人身份参与竞选。 |
| `isLeaseValid(shardId?)` | 若本节点是指定分片（默认 0）的 Leader 且 Leader 租约尚未过期，返回 `true`。可作为免 quorum 本地读的守卫条件。 |
| `isLeaseValidForKey(key)` | 同 `isLeaseValid`，但根据 key 字符串自动查找对应分片。 |
| `hasPeers()` | 若当前至少有一个 peer 处于连接状态，返回 `true`。 |
| `getClockSync(peerId?, unit?)` | 获取与指定 peer 的双向对时结果，返回 `{ rtt, offset, epsilon, samples, lastSyncAt }`，每个时间值均为 `{ value, unit }`。`unit` 默认 `'ms'`。不传 `peerId` 时返回 `Map<peerId, result>`（仅包含已有样本的 peer）。无测量结果时返回 `null`。 |
| `getStatus()` | 获取引擎状态（nodeId、分片、对等节点） |

---

## 接收方主导的全量重同步（v1.1.0+）

### 背景问题

`_sync_log` 会按 TTL 被回收，若某 peer 离线太久，`lastSyncId` 已小于本地 `minLogId`，普通增量拉取会静默跳过清理掉的条目。另一方面在 LWW key 空间里**所有 peer 覆盖的 key 区间是一样的**（都是 `(-∞, +∞]`），只有上界水位 `lastSeq` 不同 —— "我落后于 X" 的真实含义是"X 上在全 key 空间的某处有我没有的新数据"。

### 模型

1. **主动连入即触发取数** — 每次主动连接（outbound）的握手里若 `peer.lastSeq > localMaxSeq`，接收方就为每张已注册业务表 + 墓碑 **upsert 一条 `(-∞, +∞]` 的 need**，`watermark = max(已有水位, peer.lastSeq)`。多个 outbound peer 先后连上只会抬水位，不会产生多条并列 need（所有 peer 共享同一 key 空间）。
2. **区间集 needs** — `_full_sync_needs` 是 peer-agnostic 的，记录 `(phase, table_name, range_start, range_end, watermark_seq)`，`null` 边界表示 ±∞。单条 need 被部分交付后会裂成多个不相交的碎片（空洞 = 已经拿到）。
3. **per-peer claims** — `_full_sync_claims` 记录在途认领 `(phase, table_name, range_start, range_end, peer_node_id, expires_at)`，TTL 由 `fullSyncClaimTtlMs` 控制（默认 10 分钟）。调度器计算"未被任何 claim 覆盖的 need 碎片"，把每个碎片派给一个空闲 outbound peer —— 所以多个 peer 并发取的是**不重叠的子区间**。
4. **一请求一页** — 发送方收到 `full_sync_resume_req{request:{phase,tableName,rangeStart,rangeEnd,watermarkSeq}}` 后，从 `rangeStart` 开始扫描**一页**（由 `fullSyncChunkSize` 截断，并裁剪到 `rangeEnd`），回一条 `full_sync_rows` / `full_sync_tombstones`（显式带 `pageStart`, `pageEnd`）+ 一条 `full_sync_complete{request}`。剩余区间下一次调度再发。
5. **事务内挖空洞** — 每收到一页，接收方**同一事务**里：应用 rows/tombstones（HLC LWW） + 从 need 里挖掉 `(pageStart, pageEnd]`。数据和 need 状态一起推进或一起回滚。单页独立提交。
6. **释放 claim + 再调度** — `full_sync_complete` 只删对应 `(peer, phase, table)` 的那一条 claim（不影响其它 scope）。peer 断连时删该 peer 的全部 claim。cleanup tick 清过期 claim。**同步发送失败回滚**：若调度器派 `resume_req` 时 `onSendToPeer` 抛错，立即删掉刚写的 claim，本轮标记该 peer 不可用，同一碎片换下一个空闲 peer 重试。TTL 过期仍然是"对端静默卡死"的最后兜底。任意事件发生都会立即再跑一次调度器，把剩余碎片派给空闲 peer。
7. **水位推进** — 所有 scope 的碎片都被挖光 → needs 表空 → `lastSyncId` 经普通增量路径自然推进。

### 相关配置

| 配置项 | 默认 | 说明 |
|--------|------|------|
| `fullSyncChunkSize` | 500 | 每页条目数，控制峰值内存与网络 I/O 粒度 |
| `enableFullResync` | `true` | 设为 `false` 关闭，握手后走老路径（仅用于对比实验，会命中静默丢数 bug） |
| `fullSyncClaimTtlMs` | 600_000 | 在途 claim 的 TTL；该 peer 长时间不响应后它占用的区间会被自动释放给别人 |

### 涉及的基础设施表

| 表 | 用途 |
|----|------|
| `_full_sync_needs` | 接收方维护，peer-agnostic 的区间集。行：`(id, phase, table_name, range_start, range_end, watermark_seq, updated_at)`。每条 need 代表一个 scope 里不相交的"还要取"的区间碎片。 |
| `_full_sync_claims` | 接收方维护，per-peer 的在途认领。行：`(id, phase, table_name, range_start, range_end, peer_node_id, expires_at, created_at)`。在 `full_sync_complete`、peer 断连、TTL 过期时删除 |

---

## Leader 租约

Leader 当选及后续每次心跳后，计算一个本地安全租约时长：

```
leaseDuration = electionMin − ε
```

- `electionMin` — follower 最短可能的选举超时（由 RTT 推导或取静态默认值）
- `ε`（epsilon）— 双向对时的时钟误差上界（所有 peer 中不确定性最大的那个的 `minRtt / 2`）

**安全保证：** 每个 follower 在自己的时钟上等满至少 `electionMin` 后才会发起新选举。即使 follower 的时钟比 leader 快 `ε`，leader 的租约（`electionMin − ε`）也会在 follower 触发选举前过期。租约有效期内，集群中不可能存在第二个 Leader。

```js
// 在 Leader 上：免 quorum 的本地读
if (engine.isLeaseValid()) {
    return db.get('SELECT * FROM accounts WHERE id = ?', [userId]);
}
// 租约过期或本节点不是 Leader — 降级为常规 quorum 读
```

```js
// 基于 key 的变体
if (engine.isLeaseValidForKey(userId)) { ... }
```

Leader 降级时租约立即失效（`_leaseDurationMs = 0`）。若尚未完成任何一轮双向对时（`ε = Infinity`），`isLeaseValid()` 返回 `false` — 租约直到至少完成一次对时后才会生效。

---

## 双向对时

引擎每次心跳发出的 `ping` 都携带 `seq`（序号）和 `t1`（本地发送时刻）。对端收到后立即回 `pong`，并附上原始 `t1`、接收时刻 `t2`、发送时刻 `t3`。发起方收到 `pong` 后记录 `t4`，按 NTP 公式计算：

```
rtt    = (t4 - t1) - (t3 - t2)          // 纯网络往返时延（排除对端处理耗时）
offset = ((t2 - t1) + (t3 - t4)) / 2   // 时钟偏差（正值 = 对端时钟靠前）
epsilon = minRtt / 2                     // 误差上界
```

样本在每个 peer 的滑动窗口（`timeSyncWindowSize`，默认 8）中累积，始终选取 RTT 最小的样本作为最优估算（误差最小）。

```js
// 读取某个 peer 的对时结果（默认单位 ms）
const sync = engine.getClockSync('peer-1');
// sync = {
//   rtt:     { value: 4.2, unit: 'ms' },
//   offset:  { value: 1.1, unit: 'ms' },   // 正值表示对端时钟靠前 1.1ms
//   epsilon: { value: 2.1, unit: 'ms' },   // 误差上界
//   samples: 8,
//   lastSyncAt: 1713000045000,              // 最近一次测量时刻（ms）
// }

// 指定单位
const syncUs = engine.getClockSync('peer-1', 'us');

// 获取所有 peer 的结果
const allSync = engine.getClockSync();  // Map<peerId, result>
```

对时随心跳 ping 合并发送，**无需额外定时器或消息类型**，有效对时间隔 = `heartbeatIntervalMs`（默认 15s）。

### 独立工具函数

```js
import { HLC, DIALECTS, applyDialect, logChange, applyEntries } from 'sync-lib';
```

| 导出 | 说明 |
|------|------|
| `HLC` | 混合逻辑时钟类 |
| `LeaderElection` | Leader 选举状态机 |
| `DIALECTS` | 内置方言定义（sqlite、postgresql、mysql）。每种方言提供 `infraSchemaSQL`、`upsertSQL`、`triggersSQL`、`dropTriggersSQL` |
| `applyDialect(db, name)` | 将方言方法应用到 db 适配器 |
| `validateDialectMethods(db)` | 校验 db 是否具备所有必需的方言方法 |
| `logChange(db, table, op, key, data, hlc)` | 手动记录变更（触发器的替代方案） |
| `applyEntries(db, entries, hlc, registry)` | 幂等地应用远程条目 |

---

## DatabaseAdapter 接口

```
┌───────────────────────────────────────────────────────────────┐
│  始终必须实现：                                                │
│                                                               │
│  db.run(sql, params)           → { changes: number }          │
│  db.get(sql, params)           → Object | null                │
│  db.all(sql, params)           → Object[]                     │
│  db.exec(sql)                  → void                         │
│                                                               │
├───────────────────────────────────────────────────────────────┤
│  由 dialect 自动填充（或手动实现）：                            │
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

指定 `dialect: 'sqlite' | 'postgresql' | 'mysql'` 后，下面 7 个方法自动填充。不指定 dialect 则必须全部自行实现。

SQL 使用 `?` 占位符。如果你的数据库使用 `$1/$2`（如 PostgreSQL），请在 `run/get/all` 内部转换。

---

## 业务表要求

每张需要同步的业务表**必须**包含 `_hlc` 列：

```sql
CREATE TABLE your_table (
    id   TEXT PRIMARY KEY,
    data TEXT,
    _hlc TEXT NOT NULL DEFAULT '0'    -- sync-lib 必需
);
```

---

## 架构

```
sync-lib/
├── index.js              ← 统一导出
├── hlc.js                ← 混合逻辑时钟（纯算法）
├── leader-election.js    ← Raft 简化版选举状态机
├── sync-protocol.js      ← 消息编解码 + 幂等数据应用
├── sync-engine.js        ← 主引擎（组合以上模块）
└── dialects.js           ← 内置 SQL 方言（SQLite/PG/MySQL）
```

**设计原则**：
- 不依赖任何传输层（WebSocket、gRPC、TCP）
- 不依赖任何数据库驱动
- 不依赖 Node.js API（fs、os、http）
- 定时器函数通过 `timerAPI` 注入（不直接调用全局 `setTimeout`/`setInterval`）
- 时间获取通过 `getNow` 注入（不直接调用 `Date.now()` / `new Date()`）
- `getNow` 返回 `{ value: number, unit: 's' | 'ms' | 'us' | 'ns' }`，内部统一转换为毫秒
- 所有外部能力通过回调注入

---

## 许可证

xSync Source Available License 1.0 — 详见 [LICENSE](LICENSE) 和 [LICENSING.md](LICENSING.md)。
