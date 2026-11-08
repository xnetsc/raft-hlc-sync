/**
 * 通用同步协议（纯算法模块）
 *
 * 消息编解码 + HLC 感知的幂等数据应用。
 * 不依赖任何具体数据库模块，通过 DatabaseAdapter 接口操作数据。
 *
 * ═══════════════════════════════════════════════════════
 *  外部需实现的 DatabaseAdapter 接口（同步方法）：
 * ═══════════════════════════════════════════════════════
 *
 *   db.run(sql, params)   → { changes: number }
 *   db.get(sql, params)   → Object | null
 *   db.all(sql, params)   → Object[]
 *   db.exec(sql)          → void
 *
 *   params 使用 ? 占位符数组。若目标数据库使用 $1/$2（如 PostgreSQL），
 *   请在 adapter 内部转译。
 *
 *  ── 事务控制（adapter 负责方言差异）──
 *
 *   db.beginTransaction() → void
 *     开启独占写事务。
 *     SQLite : BEGIN IMMEDIATE
 *     PG     : BEGIN
 *     MySQL  : START TRANSACTION
 *
 *   db.commit()           → void   (通用 COMMIT)
 *   db.rollback()         → void   (通用 ROLLBACK)
 *
 *  ── Upsert SQL 生成 ──
 *
 *   db.upsertSQL(tableName, columns, keyColumns) → string
 *     返回带 ? 占位符的 upsert SQL，语义：
 *       若 keyColumns 冲突则更新所有非 key 列，否则插入。
 *     SQLite : INSERT OR REPLACE INTO t (c1,c2) VALUES (?,?)
 *     PG     : INSERT INTO t (c1,c2) VALUES ($1,$2)
 *                ON CONFLICT (k) DO UPDATE SET c1=EXCLUDED.c1,...
 *     MySQL  : INSERT INTO t (c1,c2) VALUES (?,?)
 *                ON DUPLICATE KEY UPDATE c1=VALUES(c1),...
 *
 *  ── Schema（可选覆盖）──
 *
 *   db.infraSchemaSQL?()  → string[]
 *     返回同步基础设施表的 DDL 语句数组。
 *     不实现则由 sync-lib 提供内置 SQLite DDL 作为降级。
 *
 *  ── 触发器（可选注入）──
 *
 *   db.triggersSQL?(tableName, def)  → string[]
 *     返回指定表的同步触发器 DDL 语句数组。
 *     不实现则由 sync-lib 提供内置 SQLite 触发器。
 *     PG / MySQL 用户可注入各自方言的触发器，
 *     从而使用 initTriggers() 自动记录变更，无需手动调用 logChange()。
 *
 *   db.dropTriggersSQL?(tableName)   → string[]
 *     返回删除指定表同步触发器的 SQL 语句数组。
 *     不实现则由 sync-lib 提供内置 SQLite 版本。
 *
 * ═══════════════════════════════════════════════════════
 *  表定义 TableDef（通过 SyncEngine.registerTable 注册）：
 * ═══════════════════════════════════════════════════════
 *
 *   {
 *     keyColumns:    string[],                      // 主键列名
 *     dataColumns:   string[],                      // 全部列名（含 _hlc）
 *     blobColumns?:  string[],                      // BLOB 列（SQLite 触发器中用 hex() 序列化）
 *     validator?:    (rowData: object) => void,     // 验证函数，不合法时抛异常
 *     deserializer?: (rowData: object) => object,   // 反序列化（如 hex→Buffer）
 *   }
 *
 * ═══════════════════════════════════════════════════════
 *  消息类型：
 * ═══════════════════════════════════════════════════════
 *
 *   handshake    — 握手，交换 nodeId 和 lastSeq
 *   sync         — 同步数据条目
 *   ack          — 确认已接收
 *   pull_req     — 主动拉取请求
 *   ping/pong    — 心跳 + 双向对时（ping 携带 seq/t1，pong 携带 seq/t1/t2/t3）
 *   prepare      — 2PC 阶段一
 *   prepare_ack  — 2PC 阶段一响应
 *   commit       — 2PC 阶段二
 *   abort        — 2PC 中止
 *   leader_heartbeat / vote_req / vote_ack — Leader Election
 *   proxy_req / proxy_res — 写请求代理
 */

import { HLC, timeValueToMs } from './hlc.js';

// ╔═══════════════════════════════════════╗
// ║         消息构造（纯函数）              ║
// ╚═══════════════════════════════════════╝

/**
 * 握手消息
 *
 * @param {string} nodeId   - 本节点 nodeId
 * @param {number} lastSeq  - 本节点当前 _sync_log 的最大 id（供对端判断是否需要推送增量）
 * @param {number} [minLogId] - 本节点当前 _sync_log 的最小 id（供对端判断其水位是否已落入已清理区间）
 */
export function makeHandshake(nodeId, lastSeq, minLogId) {
    const msg = { type: 'handshake', nodeId, lastSeq };
    if (minLogId !== undefined) msg.minLogId = minLogId;
    return JSON.stringify(msg);
}

/**
 * 全量重同步——数据行分片
 *
 * 消息语义：发送方声明"本页覆盖区间 (pageStart, pageEnd]"。接收方据此从 needs 里
 * 精确挖掉该区间，并用 LWW 幂等 upsert 应用每行。
 *
 * @param {string} table
 * @param {Array<object>} rows          - 每行包含全部 dataColumns（含 _hlc）
 * @param {object|null} pageStart       - 本页下界（上一页的 nextLastKey）；null=扫描起点
 * @param {object|null} pageEnd         - 本页上界（本页最后一行 key），null=已到 rangeEnd/表尾
 * @param {number} watermarkSeq         - 发送方开始全量时的 _sync_log 最大 id
 */
export function makeFullSyncRows(table, rows, pageStart, pageEnd, watermarkSeq) {
    return JSON.stringify({
        type: 'full_sync_rows',
        table,
        rows,
        pageStart: pageStart || null,
        pageEnd: pageEnd || null,
        watermarkSeq,
    });
}

/**
 * 全量重同步——墓碑分片
 *
 * @param {Array<{table_name, row_key, _hlc}>} tombstones
 * @param {object|null} pageStart - 本页下界 { table_name, row_key } 或 null
 * @param {object|null} pageEnd   - 本页上界 { table_name, row_key } 或 null
 * @param {number} watermarkSeq
 */
export function makeFullSyncTombstones(tombstones, pageStart, pageEnd, watermarkSeq) {
    return JSON.stringify({
        type: 'full_sync_tombstones',
        tombstones,
        pageStart: pageStart || null,
        pageEnd: pageEnd || null,
        watermarkSeq,
    });
}

/**
 * 全量重同步——区间请求
 *
 * 接收方根据本地 _full_sync_needs + _full_sync_claims 算出一个未 claim 的子区间，
 * 向 peer 发起此请求。发送方无状态，按 (rangeStart, rangeEnd] 推送分页即可。
 *
 * @param {{phase, tableName, rangeStart, rangeEnd, watermarkSeq}} request
 */
export function makeFullSyncResumeReq(request) {
    return JSON.stringify({ type: 'full_sync_resume_req', request });
}

/**
 * 全量重同步——完成标记
 *
 * 接收方收到后应:
 *   1. 将对端的 last_sync_id 水位推进为 watermarkSeq
 *   2. 恢复常规 sync 流程
 *
 * @param {number} watermarkSeq - 发送方开始全量时记录的 _sync_log MAX(id)
 */
export function makeFullSyncComplete(watermarkSeq) {
    return JSON.stringify({ type: 'full_sync_complete', watermarkSeq });
}

export function makeSync(entries) {
    return JSON.stringify({ type: 'sync', entries });
}

export function makeAck(lastSeq) {
    return JSON.stringify({ type: 'ack', lastSeq });
}

export function makePullRequest(sinceSeq) {
    return JSON.stringify({ type: 'pull_req', sinceSeq });
}

/**
 * 心跳 ping，同时携带双向对时字段（两用消息）
 *
 * @param {number} [seq]  - 对时序号（per-peer 递增整数）；不传则为纯心跳
 * @param {number} [t1Ms] - 发送方发送时刻（毫秒）
 */
export function makePing(seq, t1Ms) {
    if (seq !== undefined && t1Ms !== undefined) {
        return JSON.stringify({ type: 'ping', seq, t1: t1Ms });
    }
    return JSON.stringify({ type: 'ping' });
}

/**
 * 心跳 pong，同时携带双向对时字段（两用消息）
 *
 * 若对应 ping 携带了 seq/t1，则回传所有四个时间戳：
 * @param {number} [seq]  - 原始 ping 的序号（原样回传）
 * @param {number} [t1Ms] - 原始 ping 携带的 t1（原样回传）
 * @param {number} [t2Ms] - 接收方收到 ping 的时刻（毫秒）
 * @param {number} [t3Ms] - 接收方发出 pong 的时刻（毫秒）
 */
export function makePong(seq, t1Ms, t2Ms, t3Ms) {
    if (seq !== undefined && t1Ms !== undefined && t2Ms !== undefined && t3Ms !== undefined) {
        return JSON.stringify({ type: 'pong', seq, t1: t1Ms, t2: t2Ms, t3: t3Ms });
    }
    return JSON.stringify({ type: 'pong' });
}

export function parseMessage(raw) {
    try {
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

// ----- Leader Election 消息 -----

export function makeLeaderHeartbeat(shardId, term, leaderId) {
    return JSON.stringify({ type: 'leader_heartbeat', shardId, term, leaderId });
}

export function makeLeaderQuery(shardId, queryId, askerId) {
    return JSON.stringify({ type: 'leader_query', shardId, queryId, askerId });
}

export function makeLeaderQueryAck(shardId, queryId, alive, leaderId, term, responderId) {
    return JSON.stringify({ type: 'leader_query_ack', shardId, queryId, alive, leaderId, term, responderId });
}

export function makeVoteReq(shardId, term, candidateId, priority) {
    return JSON.stringify({ type: 'vote_req', shardId, term, candidateId, priority });
}

export function makeVoteAck(shardId, term, candidateId, granted, voterId) {
    return JSON.stringify({ type: 'vote_ack', shardId, term, candidateId, granted, voterId });
}

export function makePreVoteReq(shardId, term, candidateId, priority) {
    return JSON.stringify({ type: 'prevote_req', shardId, term, candidateId, priority });
}

export function makePreVoteAck(shardId, term, candidateId, granted, voterId) {
    return JSON.stringify({ type: 'prevote_ack', shardId, term, candidateId, granted, voterId });
}

// ----- 写请求代理消息（payload 对模块透明） -----

export function makeProxyReq(requestId, payload) {
    return JSON.stringify({ type: 'proxy_req', requestId, payload });
}

export function makeProxyRes(requestId, payload) {
    return JSON.stringify({ type: 'proxy_res', requestId, payload });
}

// ----- 两阶段提交（2PC）消息 -----

export function makePrepare(writeId, entries, term, shardId) {
    return JSON.stringify({ type: 'prepare', writeId, entries, term, shardId: shardId ?? 0 });
}

export function makePrepareAck(writeId, granted, reason) {
    return JSON.stringify({ type: 'prepare_ack', writeId, granted, reason: reason || null });
}

export function makeCommit(writeId) {
    return JSON.stringify({ type: 'commit', writeId });
}

export function makeAbort(writeId, reason) {
    return JSON.stringify({ type: 'abort', writeId, reason });
}

// ╔═══════════════════════════════════════╗
// ║       HLC 感知的幂等数据应用           ║
// ╚═══════════════════════════════════════╝

/**
 * 将远程同步条目幂等地应用到本地数据库（自动包裹事务）
 *
 * @param {DatabaseAdapter} db
 * @param {Array<object>} entries - 同步日志条目
 * @param {HLC} hlc - 本地 HLC 实例
 * @param {Map<string, TableDef>} tableRegistry - 表定义注册表
 * @param {object} [options]
 * @param {function} [options.generateUpsertSQL] - 自定义 upsert SQL 生成
 * @param {number}   [options.maxClockSkewMs]    - HLC 时钟偏差上限（ms）。
 *                     远端条目的 wallTime 若超出 localNowMs + maxClockSkewMs，则跳过该条目，
 *                     防止错误节点将本地 HLC 推进到异常未来时刻。
 *                     Infinity 或未设置时不做检测（向后兼容）。
 * @param {number}   [options.localNowMs]        - 本地当前时间（ms），配合 maxClockSkewMs 使用。
 *                     未设置时即使配置了 maxClockSkewMs 也不做检测。
 * @returns {{ applied: number, skipped: number }}
 */
export function applyEntries(db, entries, hlc, tableRegistry, options) {
    db.beginTransaction();
    try {
        const result = _applyEntriesCore(db, entries, hlc, tableRegistry, options);
        db.commit();
        return result;
    } catch (e) {
        try { db.rollback(); } catch (_) { /* ignore */ }
        throw e;
    }
}

/**
 * 在已有事务中应用条目（不自己开事务）
 * 供 2PC Follower 在手动事务中调用
 */
export function applyEntriesInTransaction(db, entries, hlc, tableRegistry, options) {
    return _applyEntriesCore(db, entries, hlc, tableRegistry, options);
}

/**
 * 核心应用逻辑（不含事务包装）
 * @private
 */
function _applyEntriesCore(db, entries, hlc, tableRegistry, options) {
    let applied = 0;
    let skipped = 0;
    const genUpsert = options?.generateUpsertSQL || null;

    // 时钟偏差上限检测参数
    const maxSkewMs  = options?.maxClockSkewMs ?? Infinity;
    const localNowMs = options?.localNowMs     ?? null;
    const doSkewCheck = isFinite(maxSkewMs) && localNowMs !== null;

    for (const entry of entries) {
        try {
            const { table_name: tableName, operation, row_key: rowKeyStr, row_data: rowDataStr, _hlc: remoteHlc } = entry;
            const def = tableRegistry.get(tableName);
            if (!def) {
                skipped++;
                continue;
            }

            if (!remoteHlc) {
                skipped++;
                continue;
            }

            // 合并远程 HLC 时钟（含时钟偏差上限检测）
            const parsed = HLC.parse(remoteHlc);

            // 偏差检测：若远端 wallTime 超出 localNow + maxSkewMs，说明对端时钟异常靠前，
            // 直接跳过该条目，避免将本地 HLC 推进到不合理的未来时刻。
            if (doSkewCheck && parsed.wallTime > localNowMs + maxSkewMs) {
                skipped++;
                continue;
            }

            hlc.receive(parsed.wallTime, parsed.counter);

            if (operation === 'DELETE') {
                const didApply = _applyDelete(db, tableName, def, rowKeyStr, remoteHlc);
                if (didApply) applied++;
                else skipped++;
            } else if (operation === 'INSERT' || operation === 'UPDATE') {
                const didApply = _applyUpsert(db, tableName, def, rowDataStr, rowKeyStr, remoteHlc, genUpsert);
                if (didApply) applied++;
                else skipped++;
            } else {
                skipped++;
            }
        } catch (err) {
            skipped++;
        }
    }

    return { applied, skipped };
}

/**
 * HLC 感知的幂等删除
 * @returns {boolean} 是否实际执行了删除
 */
function _applyDelete(db, tableName, def, rowKeyStr, remoteHlc) {
    const rowKey = JSON.parse(rowKeyStr);

    // 检查本地行的 _hlc（DELETE 用严格大于 >）
    const localHlc = _getLocalRowHlc(db, tableName, def, rowKey);
    if (localHlc && HLC.compare(localHlc, remoteHlc) > 0) {
        return false;
    }

    // 检查墓碑的 _hlc
    const tombstoneHlc = _getTombstoneHlc(db, tableName, rowKeyStr);
    if (tombstoneHlc && HLC.compare(tombstoneHlc, remoteHlc) >= 0) {
        return false;
    }

    // 删除本地行
    const whereParts = def.keyColumns.map((col) => col + ' = ?');
    const whereValues = def.keyColumns.map((col) => rowKey[col]);
    db.run('DELETE FROM ' + tableName + ' WHERE ' + whereParts.join(' AND '), whereValues);

    // 写入/更新墓碑
    const tombstoneCols = ['table_name', 'row_key', '_hlc'];
    const tombstoneKeys = ['table_name', 'row_key'];
    db.run(db.upsertSQL('_tombstones', tombstoneCols, tombstoneKeys), [tableName, rowKeyStr, remoteHlc]);

    return true;
}

/**
 * HLC 感知的幂等 Upsert
 * @returns {boolean} 是否实际执行了写入
 */
function _applyUpsert(db, tableName, def, rowDataStr, rowKeyStr, remoteHlc, generateUpsertSQL) {
    const rowKey = JSON.parse(rowKeyStr);
    let rowData = JSON.parse(rowDataStr);

    // 检查墓碑
    const tombstoneHlc = _getTombstoneHlc(db, tableName, rowKeyStr);
    if (tombstoneHlc && HLC.compare(tombstoneHlc, remoteHlc) >= 0) {
        return false;
    }

    // 检查本地行的 _hlc
    const localHlc = _getLocalRowHlc(db, tableName, def, rowKey);
    if (localHlc && HLC.compare(localHlc, remoteHlc) >= 0) {
        return false;
    }

    // 业务验证（由外部注册）
    if (def.validator) {
        def.validator(rowData);
    }

    // 删除过期墓碑（数据复活）
    if (tombstoneHlc) {
        db.run('DELETE FROM _tombstones WHERE table_name = ? AND row_key = ?', [tableName, rowKeyStr]);
    }

    // 反序列化（由外部注册，如 hex→Buffer）
    if (def.deserializer) {
        rowData = def.deserializer(rowData);
    }

    // 构建并执行 upsert
    const columns = def.dataColumns.filter((col) => rowData[col] !== undefined);
    const values = columns.map((col) => rowData[col]);

    let sql;
    if (generateUpsertSQL) {
        sql = generateUpsertSQL(tableName, columns, def.keyColumns);
    } else {
        sql = db.upsertSQL(tableName, columns, def.keyColumns);
    }
    db.run(sql, values);

    return true;
}

// ╔═══════════════════════════════════════╗
// ║            辅助查询函数                ║
// ╚═══════════════════════════════════════╝

function _getLocalRowHlc(db, tableName, def, rowKey) {
    const whereParts = def.keyColumns.map((col) => col + ' = ?');
    const whereValues = def.keyColumns.map((col) => rowKey[col]);
    const sql = 'SELECT _hlc FROM ' + tableName + ' WHERE ' + whereParts.join(' AND ');
    const row = db.get(sql, whereValues);
    return row?._hlc || null;
}

function _getTombstoneHlc(db, tableName, rowKeyStr) {
    const row = db.get('SELECT _hlc FROM _tombstones WHERE table_name = ? AND row_key = ?', [tableName, rowKeyStr]);
    return row?._hlc || null;
}

// ╔═══════════════════════════════════════╗
// ║         _sync_log / _sync_peers       ║
// ╚═══════════════════════════════════════╝

export function readSyncLog(db, sinceSeq, limit = 1000) {
    return db.all(
        'SELECT id AS seq, table_name, operation, row_key, row_data, _hlc, created_at FROM _sync_log WHERE id > ? ORDER BY id ASC LIMIT ?',
        [sinceSeq, limit]
    );
}

export function getMaxSeq(db) {
    const row = db.get('SELECT MAX(id) AS max_seq FROM _sync_log', []);
    return row?.max_seq || 0;
}

/**
 * 返回 _sync_log 当前最小 id。空表返回 0。
 * 用于握手时检测对端 lastSeq 是否已落入已清理区间。
 */
export function getMinLogId(db) {
    const row = db.get('SELECT MIN(id) AS min_seq FROM _sync_log', []);
    return row?.min_seq || 0;
}

export function cleanSyncLog(db, maxEntries = 10000) {
    const row = db.get('SELECT COUNT(*) AS cnt FROM _sync_log', []);
    if (row.cnt > maxEntries) {
        const cutoff = row.cnt - maxEntries;
        db.run(
            'DELETE FROM _sync_log WHERE id IN (SELECT id FROM _sync_log ORDER BY id ASC LIMIT ?)',
            [cutoff]
        );
    }
}

export function getPeerLastSyncId(db, peerKey) {
    const row = db.get('SELECT last_sync_id FROM _sync_peers WHERE peer_key = ?', [peerKey]);
    return row?.last_sync_id || 0;
}

/**
 * 读取 peer 水位行（区分"无记录"与"水位为 0"）。
 * 无记录返回 null：调用方不应把缺失水位当作从 0 同步，而应改走最大序号
 * 比对（见 sync-engine 的 _tickPull / _pushToAllPeers）。
 */
export function getPeerWatermark(db, peerKey) {
    const row = db.get('SELECT last_sync_id FROM _sync_peers WHERE peer_key = ?', [peerKey]);
    return row ? row.last_sync_id : null;
}

export function updatePeerSyncId(db, peerKey, lastSyncId, getNow) {
    const cols = ['peer_key', 'last_sync_id', 'last_sync_at'];
    const keys = ['peer_key'];
    const nowMs = timeValueToMs(getNow());
    db.run(db.upsertSQL('_sync_peers', cols, keys), [peerKey, lastSyncId, new Date(nowMs).toISOString()]);
}

/**
 * 读取本节点对某 peer 日志的消费偏移（pull offset）：已确认收到该 peer
 * _sync_log 到哪个序号（对方序号空间）。与 _sync_peers.last_sync_id
 * （对方消费本节点日志的进度，push 方向，本节点序号空间）是两个独立
 * 语义，分别记录、互不借用——混用会让 pull 请求跳过从未收到过的条目，
 * 在非全连接拓扑上形成静默的数据缺口。
 * 无记录返回 null（尚未与该 peer 建立增量拉取关系）。
 */
export function getPeerPullOffset(db, peerKey) {
    const row = db.get('SELECT last_pulled_seq FROM _sync_pull_offsets WHERE peer_key = ?', [peerKey]);
    return row ? row.last_pulled_seq : null;
}

/**
 * 推进 pull offset（单调，只增不减）：乱序到达的批次不得把偏移拉回去。
 * 调用方须保证推进不越过未收到的条目（连续性由引擎的推进点保证）。
 */
export function updatePeerPullOffset(db, peerKey, lastPulledSeq, getNow) {
    const cur = getPeerPullOffset(db, peerKey);
    if (cur !== null && lastPulledSeq <= cur) return;
    const cols = ['peer_key', 'last_pulled_seq', 'last_sync_at'];
    const keys = ['peer_key'];
    const nowMs = timeValueToMs(getNow());
    db.run(db.upsertSQL('_sync_pull_offsets', cols, keys), [peerKey, lastPulledSeq, new Date(nowMs).toISOString()]);
}

// ╔═══════════════════════════════════════╗
// ║             墓碑清理                   ║
// ╚═══════════════════════════════════════╝

export function cleanTombstones(db, getNow, retentionDays = 7) {
    if (typeof getNow !== 'function') {
        throw new Error('cleanTombstones: getNow is required and must be a function.');
    }
    const cutoffMs = timeValueToMs(getNow()) - retentionDays * 24 * 60 * 60 * 1000;
    const rows = db.all('SELECT table_name, row_key, _hlc FROM _tombstones', []);
    let cleaned = 0;
    for (const row of rows) {
        const wallTime = HLC.extractWallTime(row._hlc);
        if (wallTime < cutoffMs) {
            db.run('DELETE FROM _tombstones WHERE table_name = ? AND row_key = ?', [row.table_name, row.row_key]);
            cleaned++;
        }
    }
    return cleaned;
}

// ╔═══════════════════════════════════════╗
// ║         手动变更记录（替代触发器）       ║
// ╚═══════════════════════════════════════╝

/**
 * 手动记录一条数据变更到 _sync_log
 * 适用于不支持触发器的数据库。
 * 外部业务代码在每次写操作后调用此方法。
 *
 * @param {DatabaseAdapter} db
 * @param {string} tableName
 * @param {string} operation - 'INSERT' | 'UPDATE' | 'DELETE'
 * @param {string} rowKeyJson - JSON 编码的主键对象，如 '{"user_id":"u1"}'
 * @param {string|null} rowDataJson - JSON 编码的完整行数据（DELETE 时为 null）
 * @param {string} hlcTimestamp - 本次操作的 HLC 时间戳
 */
export function logChange(db, tableName, operation, rowKeyJson, rowDataJson, hlcTimestamp) {
    db.run(
        'INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc) VALUES (?, ?, ?, ?, ?)',
        [tableName, operation, rowKeyJson, rowDataJson, hlcTimestamp]
    );
}

// ╔═══════════════════════════════════════╗
// ║         全量重同步（快照流）             ║
// ╚═══════════════════════════════════════╝

/**
 * 基于游标的业务表分页扫描（按 keyColumns 升序）。
 *
 * 仅加载最多 `limit` 行到内存;超大表可通过重复调用流式处理。
 * 支持复合主键:使用展开 OR 比较,与 SQLite / PG / MySQL 等方言通用
 * (避免使用部分方言不支持的元组比较 `(a,b) > (?,?)`)。
 *
 * @param {DatabaseAdapter} db
 * @param {string} tableName
 * @param {{keyColumns: string[], dataColumns: string[]}} def
 * @param {object|null} lastKey - 上一页最后一行的 key 列对象;null 表示从头
 * @param {number} limit
 * @returns {{ rows: object[], nextLastKey: object|null }}
 *   nextLastKey 为 null 时代表该表已全部取完。
 */
export function scanTableRowsPage(db, tableName, def, lastKey, limit) {
    const cols = def.dataColumns.join(', ');
    const orderBy = def.keyColumns.map((c) => c + ' ASC').join(', ');
    let sql, params;
    if (!lastKey) {
        sql = 'SELECT ' + cols + ' FROM ' + tableName + ' ORDER BY ' + orderBy + ' LIMIT ?';
        params = [limit];
    } else {
        const { where, params: whereParams } = _buildCursorWhere(def.keyColumns, lastKey);
        sql = 'SELECT ' + cols + ' FROM ' + tableName + ' WHERE ' + where + ' ORDER BY ' + orderBy + ' LIMIT ?';
        params = [...whereParams, limit];
    }
    const rows = db.all(sql, params);
    let nextLastKey = null;
    if (rows.length === limit) {
        const last = rows[rows.length - 1];
        nextLastKey = {};
        for (const c of def.keyColumns) nextLastKey[c] = last[c];
    }
    return { rows, nextLastKey };
}

/**
 * 基于游标的墓碑表分页扫描，按 (table_name, row_key) 升序。
 *
 * @param {object|null} lastKey - { table_name, row_key } 或 null
 * @returns {{ tombstones: Array<{table_name, row_key, _hlc}>, nextLastKey: object|null }}
 */
export function scanTombstonesPage(db, lastKey, limit) {
    let sql, params;
    if (!lastKey) {
        sql = 'SELECT table_name, row_key, _hlc FROM _tombstones ORDER BY table_name ASC, row_key ASC LIMIT ?';
        params = [limit];
    } else {
        sql =
            'SELECT table_name, row_key, _hlc FROM _tombstones WHERE ' +
            '(table_name > ?) OR (table_name = ? AND row_key > ?) ' +
            'ORDER BY table_name ASC, row_key ASC LIMIT ?';
        params = [lastKey.table_name, lastKey.table_name, lastKey.row_key, limit];
    }
    const tombstones = db.all(sql, params);
    let nextLastKey = null;
    if (tombstones.length === limit) {
        const last = tombstones[tombstones.length - 1];
        nextLastKey = { table_name: last.table_name, row_key: last.row_key };
    }
    return { tombstones, nextLastKey };
}

/**
 * 构建游标式分页的 WHERE 子句:
 *   keyColumns = [k1, k2, k3], lastKey = {k1: a, k2: b, k3: c}
 *   → WHERE (k1 > ?) OR (k1 = ? AND k2 > ?) OR (k1 = ? AND k2 = ? AND k3 > ?)
 *   params = [a, a, b, a, b, c]
 * 复合主键下正确且方言通用(无需元组比较语法)。
 */
function _buildCursorWhere(keyCols, lastKey) {
    const parts = [];
    const params = [];
    for (let i = 0; i < keyCols.length; i++) {
        const conds = [];
        for (let j = 0; j < i; j++) {
            conds.push(keyCols[j] + ' = ?');
            params.push(lastKey[keyCols[j]]);
        }
        conds.push(keyCols[i] + ' > ?');
        params.push(lastKey[keyCols[i]]);
        parts.push('(' + conds.join(' AND ') + ')');
    }
    return { where: parts.join(' OR '), params };
}

// ──────────────────────────────────────────────
//  全量重同步：needs（还需要的数据区间）+ claims（在途 fetch）
// ──────────────────────────────────────────────
//
//  模型：
//    - needs: (phase, table_name, range_start, range_end, watermark_seq)
//      每 scope 最多一条 need；range_start=null=-∞，range_end=null=+∞。
//      LWW 业务表 key 空间下，所有 peer 都覆盖 (-∞,+∞)，因此合并时只抬高 watermark_seq。
//
//    - claims: (phase, table_name, range_start, range_end, peer_node_id, expires_at)
//      每条代表"正在从 peer 拉该子区间"。完成/断连/过期即删。
//
//  页到达：同事务 applyFullSyncRows + subtractFromNeeds((pageStart, pageEnd])。
//  区间被挖光 ⇒ 该 need 记录被删除（自然终止）。
//
//  null 语义: range_start=null = 无下界(-∞)，range_end=null = 无上界(+∞)。

// 比较两个 key 对象（按 keyColumns 顺序逐列比较）。
// 两边都为 null 代表 -∞（开始边界）或 +∞（结束边界），由调用方区分。
function _cmpKey(a, b, keyColumns) {
    for (const c of keyColumns) {
        const av = a[c], bv = b[c];
        if (av === bv) continue;
        if (av === undefined || av === null) return -1;
        if (bv === undefined || bv === null) return 1;
        if (av < bv) return -1;
        if (av > bv) return 1;
    }
    return 0;
}

// lower bound: null → -∞；higher bound: null → +∞。比较两个下界。
function _cmpLower(a, b, keyColumns) {
    if (a === null && b === null) return 0;
    if (a === null) return -1;
    if (b === null) return 1;
    return _cmpKey(a, b, keyColumns);
}
function _cmpUpper(a, b, keyColumns) {
    if (a === null && b === null) return 0;
    if (a === null) return 1;
    if (b === null) return -1;
    return _cmpKey(a, b, keyColumns);
}

// 序列化区间端点到 JSON（null 透传）
function _encodeBound(k) { return k === null ? null : JSON.stringify(k); }
function _decodeBound(s) { return s === null || s === undefined ? null : JSON.parse(s); }

/**
 * 查询/合并某 scope 的 need。
 * LWW 模型下每 scope 至多一条 need；若已存在且 watermark 更高则保留旧 watermark，否则抬高。
 * 若不存在则插入 (-∞, +∞, watermarkSeq)。
 */
export function upsertFullSyncNeed(db, phase, tableName, watermarkSeq, getNow) {
    const nowIso = new Date(timeValueToMs(getNow())).toISOString();
    const existing = db.get(
        'SELECT id, watermark_seq FROM _full_sync_needs WHERE phase = ? AND ' +
        (tableName === null ? 'table_name IS NULL' : 'table_name = ?'),
        tableName === null ? [phase] : [phase, tableName]
    );
    if (existing) {
        const newWm = Math.max(existing.watermark_seq, watermarkSeq);
        if (newWm !== existing.watermark_seq) {
            db.run(
                'UPDATE _full_sync_needs SET watermark_seq = ?, updated_at = ? WHERE id = ?',
                [newWm, nowIso, existing.id]
            );
        }
        return existing.id;
    }
    db.run(
        'INSERT INTO _full_sync_needs (phase, table_name, range_start, range_end, watermark_seq, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
        [phase, tableName, null, null, watermarkSeq, nowIso]
    );
    return db.get('SELECT MAX(id) AS id FROM _full_sync_needs', []).id;
}

/**
 * 返回所有 needs（每条解码 range_start/range_end）。
 */
export function getAllFullSyncNeeds(db) {
    const rows = db.all(
        'SELECT id, phase, table_name, range_start, range_end, watermark_seq FROM _full_sync_needs',
        []
    );
    return rows.map(r => ({
        id: r.id,
        phase: r.phase,
        tableName: r.table_name || null,
        rangeStart: _decodeBound(r.range_start),
        rangeEnd: _decodeBound(r.range_end),
        watermarkSeq: r.watermark_seq,
    }));
}

/**
 * 把 (pageStart, pageEnd] 从匹配 scope 的 need 中挖掉。
 * 同事务调用（被 applyFullSync* 包裹）。挖后产生 0/1/2 条 need 片段。
 *
 * @param {object|null} pageStart - null = -∞
 * @param {object|null} pageEnd   - null = +∞
 * @param {string[]} keyColumns   - scope 对应的主键列顺序（tombstones 用 ['table_name','row_key']）
 */
export function subtractFromNeeds(db, phase, tableName, pageStart, pageEnd, keyColumns, getNow) {
    const nowIso = new Date(timeValueToMs(getNow())).toISOString();
    const whereTable = tableName === null ? 'table_name IS NULL' : 'table_name = ?';
    const sqlParams = tableName === null ? [phase] : [phase, tableName];
    const rows = db.all(
        'SELECT id, range_start, range_end, watermark_seq FROM _full_sync_needs WHERE phase = ? AND ' + whereTable,
        sqlParams
    );
    for (const r of rows) {
        const nStart = _decodeBound(r.range_start);
        const nEnd = _decodeBound(r.range_end);
        // 无交集: pageEnd <= need.start  或  pageStart >= need.end
        if (pageEnd !== null && _cmpLower(pageEnd, nStart, keyColumns) <= 0) continue;
        if (pageStart !== null && _cmpUpper(pageStart, nEnd, keyColumns) >= 0) continue;

        // 计算挖去后的剩余: [nStart..pageStart] ∪ [pageEnd..nEnd]
        // left 片段: nStart 存在且 pageStart > nStart
        const leftExists =
            (pageStart !== null) &&
            (nStart === null || _cmpKey(pageStart, nStart, keyColumns) > 0);
        // right 片段: pageEnd < nEnd
        const rightExists =
            (pageEnd !== null) &&
            (nEnd === null || _cmpKey(pageEnd, nEnd, keyColumns) < 0);

        db.run('DELETE FROM _full_sync_needs WHERE id = ?', [r.id]);
        if (leftExists) {
            db.run(
                'INSERT INTO _full_sync_needs (phase, table_name, range_start, range_end, watermark_seq, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
                [phase, tableName, _encodeBound(nStart), _encodeBound(pageStart), r.watermark_seq, nowIso]
            );
        }
        if (rightExists) {
            db.run(
                'INSERT INTO _full_sync_needs (phase, table_name, range_start, range_end, watermark_seq, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
                [phase, tableName, _encodeBound(pageEnd), _encodeBound(nEnd), r.watermark_seq, nowIso]
            );
        }
    }
}

/**
 * 查询活跃（未过期）claims。
 */
export function getActiveClaims(db, getNow) {
    const nowIso = new Date(timeValueToMs(getNow())).toISOString();
    const rows = db.all(
        'SELECT id, phase, table_name, range_start, range_end, peer_node_id, expires_at FROM _full_sync_claims WHERE expires_at > ?',
        [nowIso]
    );
    return rows.map(r => ({
        id: r.id,
        phase: r.phase,
        tableName: r.table_name || null,
        rangeStart: _decodeBound(r.range_start),
        rangeEnd: _decodeBound(r.range_end),
        peerNodeId: r.peer_node_id,
        expiresAt: r.expires_at,
    }));
}

/**
 * 插入一条 claim（把某区间指派给某 peer 在途 fetch）。返回新 claim 的 id。
 */
export function insertClaim(db, phase, tableName, rangeStart, rangeEnd, peerNodeId, ttlMs, getNow) {
    const nowMs = timeValueToMs(getNow());
    const nowIso = new Date(nowMs).toISOString();
    const expiresIso = new Date(nowMs + ttlMs).toISOString();
    db.run(
        'INSERT INTO _full_sync_claims (phase, table_name, range_start, range_end, peer_node_id, expires_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
        [phase, tableName, _encodeBound(rangeStart), _encodeBound(rangeEnd), peerNodeId, expiresIso, nowIso]
    );
    return db.get('SELECT MAX(id) AS id FROM _full_sync_claims', []).id;
}

/**
 * 删除一条指定 id 的 claim（在途发送失败时回滚用）。
 */
export function deleteClaimById(db, claimId) {
    db.run('DELETE FROM _full_sync_claims WHERE id = ?', [claimId]);
}

/**
 * 删掉某 peer 的所有 claims（peer 断连时调用）。
 */
export function deletePeerClaims(db, peerNodeId) {
    db.run('DELETE FROM _full_sync_claims WHERE peer_node_id = ?', [peerNodeId]);
}

/**
 * 删掉某 peer 对应某 scope 的所有 claims（full_sync_complete 时调用）。
 */
export function deletePeerScopeClaims(db, peerNodeId, phase, tableName) {
    const whereTable = tableName === null ? 'table_name IS NULL' : 'table_name = ?';
    const params = tableName === null ? [peerNodeId, phase] : [peerNodeId, phase, tableName];
    db.run(
        'DELETE FROM _full_sync_claims WHERE peer_node_id = ? AND phase = ? AND ' + whereTable,
        params
    );
}

/**
 * 清掉所有已过期 claims。
 */
export function cleanExpiredClaims(db, getNow) {
    const nowIso = new Date(timeValueToMs(getNow())).toISOString();
    db.run('DELETE FROM _full_sync_claims WHERE expires_at <= ?', [nowIso]);
}

/**
 * 计算某 scope 下未被 claim 覆盖的区间碎片列表。
 * 输入：一条 need（rangeStart/rangeEnd），同 scope 的 active claims。
 * 返回：按下界排序的 [{rangeStart, rangeEnd}, ...]
 *
 * 算法：从 need 区间开始，按 claim 下界排序逐一减去；剩余部分即碎片。
 */
export function computeUnclaimedFragments(needRangeStart, needRangeEnd, scopeClaims, keyColumns) {
    // 无 claim: 整条 need 都未 claim
    if (scopeClaims.length === 0) {
        return [{ rangeStart: needRangeStart, rangeEnd: needRangeEnd }];
    }
    // 裁剪每条 claim 到 need 内部
    const clamped = [];
    for (const c of scopeClaims) {
        const cs = _cmpLower(c.rangeStart, needRangeStart, keyColumns) < 0 ? needRangeStart : c.rangeStart;
        const ce = _cmpUpper(c.rangeEnd, needRangeEnd, keyColumns) > 0 ? needRangeEnd : c.rangeEnd;
        // 空/反向区间丢弃 (cs ≥ ce，且都非 null)
        if (cs !== null && ce !== null && _cmpKey(cs, ce, keyColumns) >= 0) continue;
        clamped.push({ rangeStart: cs, rangeEnd: ce });
    }
    if (clamped.length === 0) {
        return [{ rangeStart: needRangeStart, rangeEnd: needRangeEnd }];
    }
    // 按 rangeStart 排序（null=-∞ 最前）
    clamped.sort((a, b) => _cmpLower(a.rangeStart, b.rangeStart, keyColumns));
    // 合并重叠/相邻 claim
    const merged = [clamped[0]];
    for (let i = 1; i < clamped.length; i++) {
        const c = clamped[i];
        const last = merged[merged.length - 1];
        if (last.rangeEnd === null || _cmpLower(c.rangeStart, last.rangeEnd, keyColumns) <= 0) {
            if (last.rangeEnd !== null && (c.rangeEnd === null || _cmpKey(c.rangeEnd, last.rangeEnd, keyColumns) > 0)) {
                last.rangeEnd = c.rangeEnd;
            }
        } else {
            merged.push({ ...c });
        }
    }
    // 从 need 区间里切出 merged 之间的空隙
    const gaps = [];
    let cursor = needRangeStart;
    for (const m of merged) {
        // 若 cursor < m.rangeStart, 有一段空隙
        if (_cmpLower(cursor, m.rangeStart, keyColumns) < 0) {
            gaps.push({ rangeStart: cursor, rangeEnd: m.rangeStart });
        }
        // 推进 cursor 到 max(cursor, m.rangeEnd)
        if (m.rangeEnd === null) { cursor = null; break; }  // +∞, 已到 need 尾
        if (cursor === null || _cmpKey(m.rangeEnd, cursor, keyColumns) > 0) cursor = m.rangeEnd;
    }
    // 最后一段尾巴: cursor 到 needRangeEnd
    if (cursor !== null) {
        if (needRangeEnd === null || _cmpKey(cursor, needRangeEnd, keyColumns) < 0) {
            gaps.push({ rangeStart: cursor, rangeEnd: needRangeEnd });
        }
    }
    return gaps;
}

/**
 * 将全量快照行幂等地 upsert 到本地。
 * 每行自带 _hlc；比较逻辑与 _applyUpsert 一致（HLC LWW + 墓碑优先）。
 *
 * @returns {{ applied: number, skipped: number }}
 */
export function applyFullSyncRows(db, tableName, def, rows, hlc, options) {
    const genUpsert = options?.generateUpsertSQL || null;
    const maxSkewMs = options?.maxClockSkewMs ?? Infinity;
    const localNowMs = options?.localNowMs ?? null;
    const doSkewCheck = isFinite(maxSkewMs) && localNowMs !== null;
    const inTxn = !!options?.inTransaction;

    let applied = 0, skipped = 0;
    if (!inTxn) db.beginTransaction();
    try {
        for (const row of rows) {
            try {
                const remoteHlc = row._hlc;
                if (!remoteHlc) { skipped++; continue; }

                const parsed = HLC.parse(remoteHlc);
                if (doSkewCheck && parsed.wallTime > localNowMs + maxSkewMs) {
                    skipped++;
                    continue;
                }
                hlc.receive(parsed.wallTime, parsed.counter);

                const rowKey = {};
                for (const c of def.keyColumns) rowKey[c] = row[c];
                const rowKeyStr = JSON.stringify(rowKey);

                const tombstoneHlc = _getTombstoneHlc(db, tableName, rowKeyStr);
                if (tombstoneHlc && HLC.compare(tombstoneHlc, remoteHlc) >= 0) { skipped++; continue; }

                const localHlc = _getLocalRowHlc(db, tableName, def, rowKey);
                if (localHlc && HLC.compare(localHlc, remoteHlc) >= 0) { skipped++; continue; }

                if (def.validator) def.validator(row);

                if (tombstoneHlc) {
                    db.run('DELETE FROM _tombstones WHERE table_name = ? AND row_key = ?', [tableName, rowKeyStr]);
                }

                const rowData = def.deserializer ? def.deserializer(row) : row;
                const columns = def.dataColumns.filter((col) => rowData[col] !== undefined);
                const values = columns.map((col) => rowData[col]);
                const sql = genUpsert
                    ? genUpsert(tableName, columns, def.keyColumns)
                    : db.upsertSQL(tableName, columns, def.keyColumns);
                db.run(sql, values);
                applied++;
            } catch (_) {
                skipped++;
            }
        }
        if (!inTxn) db.commit();
    } catch (e) {
        if (!inTxn) { try { db.rollback(); } catch (_) { /* ignore */ } }
        throw e;
    }
    return { applied, skipped };
}

/**
 * 将全量快照的墓碑列表幂等地应用到本地。
 * @returns {{ applied: number, skipped: number }}
 */
export function applyFullSyncTombstones(db, tombstones, hlc, tableRegistry, options) {
    const maxSkewMs = options?.maxClockSkewMs ?? Infinity;
    const localNowMs = options?.localNowMs ?? null;
    const doSkewCheck = isFinite(maxSkewMs) && localNowMs !== null;
    const inTxn = !!options?.inTransaction;

    let applied = 0, skipped = 0;
    if (!inTxn) db.beginTransaction();
    try {
        for (const t of tombstones) {
            try {
                const { table_name: tableName, row_key: rowKeyStr, _hlc: remoteHlc } = t;
                if (!remoteHlc) { skipped++; continue; }

                const parsed = HLC.parse(remoteHlc);
                if (doSkewCheck && parsed.wallTime > localNowMs + maxSkewMs) { skipped++; continue; }
                hlc.receive(parsed.wallTime, parsed.counter);

                const def = tableRegistry.get(tableName);
                if (def) {
                    const rowKey = JSON.parse(rowKeyStr);
                    const localHlc = _getLocalRowHlc(db, tableName, def, rowKey);
                    if (localHlc && HLC.compare(localHlc, remoteHlc) > 0) { skipped++; continue; }
                    const existingTomb = _getTombstoneHlc(db, tableName, rowKeyStr);
                    if (existingTomb && HLC.compare(existingTomb, remoteHlc) >= 0) { skipped++; continue; }
                    const whereParts = def.keyColumns.map((col) => col + ' = ?');
                    const whereValues = def.keyColumns.map((col) => rowKey[col]);
                    db.run('DELETE FROM ' + tableName + ' WHERE ' + whereParts.join(' AND '), whereValues);
                }

                db.run(
                    db.upsertSQL('_tombstones', ['table_name', 'row_key', '_hlc'], ['table_name', 'row_key']),
                    [tableName, rowKeyStr, remoteHlc]
                );
                applied++;
            } catch (_) {
                skipped++;
            }
        }
        if (!inTxn) db.commit();
    } catch (e) {
        if (!inTxn) { try { db.rollback(); } catch (_) { /* ignore */ } }
        throw e;
    }
    return { applied, skipped };
}

