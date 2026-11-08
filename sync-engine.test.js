import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { SyncEngine } from './sync-engine.js';
import { getPeerWatermark } from './sync-protocol.js';

// ═══════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════

function makeGetNow(startMs = 1713000000000) {
    let t = startMs;
    return () => ({ value: t++, unit: 'ms' });
}

function makeTimerAPI() {
    return {
        setTimeout:    (fn, ms) => setTimeout(fn, ms),
        clearTimeout:  (id) => clearTimeout(id),
        setInterval:   (fn, ms) => setInterval(fn, ms),
        clearInterval: (id) => clearInterval(id),
    };
}

function createInMemoryDb(opts = {}) {
    const tables = {};
    const getTable = (name) => { if (!tables[name]) tables[name] = []; return tables[name]; };
    let inTxn = false;
    let txnSnapshot = null;
    // Test code can declare business tables: { tableKeys: { users: ['user_id'] } }
    const tableKeys = opts.tableKeys || {};

    return {
        _tables: tables,
        _tableKeys: tableKeys,
        run(sql, params = []) {
            // Generic business-table INSERT OR REPLACE
            const upsertMatch = sql.match(/^INSERT OR REPLACE INTO (\w+) \(([^)]+)\) VALUES/);
            if (upsertMatch && !['_sync_log', '_sync_peers', '_tombstones'].includes(upsertMatch[1])) {
                const tbl = upsertMatch[1];
                const cols = upsertMatch[2].split(',').map(s => s.trim());
                const keyCols = tableKeys[tbl];
                if (!keyCols) return { changes: 0 };
                const row = Object.fromEntries(cols.map((c, i) => [c, params[i]]));
                const t = getTable(tbl);
                const idx = t.findIndex(r => keyCols.every(k => r[k] === row[k]));
                if (idx >= 0) t[idx] = row; else t.push(row);
                return { changes: 1 };
            }
            // Generic business-table DELETE FROM tbl WHERE k1 = ? AND k2 = ?
            const delMatch = sql.match(/^DELETE FROM (\w+) WHERE (.+)$/);
            if (delMatch && !['_sync_log', '_sync_peers', '_tombstones', '_full_sync_needs', '_full_sync_claims'].includes(delMatch[1])) {
                const tbl = delMatch[1];
                const keyCols = tableKeys[tbl];
                if (!keyCols) return { changes: 0 };
                const t = getTable(tbl);
                const idx = t.findIndex(r => keyCols.every((k, i) => r[k] === params[i]));
                if (idx >= 0) { t.splice(idx, 1); return { changes: 1 }; }
                return { changes: 0 };
            }
            if (sql.startsWith('INSERT') && sql.includes('_sync_log')) {
                const t = getTable('_sync_log');
                const id = t.length + 1;
                t.push({ id, table_name: params[0], operation: params[1], row_key: params[2], row_data: params[3], _hlc: params[4] });
                return { changes: 1 };
            }
            if (sql.startsWith('INSERT OR REPLACE') && sql.includes('_sync_peers')) {
                const t = getTable('_sync_peers');
                const idx = t.findIndex(r => r.peer_key === params[0]);
                const row = { peer_key: params[0], last_sync_id: params[1], last_sync_at: params[2] };
                if (idx >= 0) t[idx] = row; else t.push(row);
                return { changes: 1 };
            }
            if (sql.startsWith('INSERT OR REPLACE') && sql.includes('_tombstones')) {
                const t = getTable('_tombstones');
                const idx = t.findIndex(r => r.table_name === params[0] && r.row_key === params[1]);
                const row = { table_name: params[0], row_key: params[1], _hlc: params[2] };
                if (idx >= 0) t[idx] = row; else t.push(row);
                return { changes: 1 };
            }
            if (sql.startsWith('INSERT OR REPLACE')) {
                return { changes: 1 };
            }
            // _full_sync_needs / _full_sync_claims — generic INSERT/UPDATE/DELETE
            if (sql.startsWith('INSERT INTO _full_sync_needs')) {
                const t = getTable('_full_sync_needs');
                const id = (t._nextId = (t._nextId || 0) + 1);
                t.push({
                    id,
                    phase: params[0],
                    table_name: params[1],
                    range_start: params[2],
                    range_end: params[3],
                    watermark_seq: params[4],
                    updated_at: params[5],
                });
                return { changes: 1 };
            }
            if (sql.startsWith('UPDATE _full_sync_needs SET watermark_seq')) {
                const t = getTable('_full_sync_needs');
                const r = t.find(r => r.id === params[2]);
                if (r) { r.watermark_seq = params[0]; r.updated_at = params[1]; return { changes: 1 }; }
                return { changes: 0 };
            }
            if (sql.startsWith('DELETE FROM _full_sync_needs WHERE id =')) {
                const t = getTable('_full_sync_needs');
                const i = t.findIndex(r => r.id === params[0]);
                if (i >= 0) { t.splice(i, 1); return { changes: 1 }; }
                return { changes: 0 };
            }
            if (sql.startsWith('INSERT INTO _full_sync_claims')) {
                const t = getTable('_full_sync_claims');
                const id = (t._nextId = (t._nextId || 0) + 1);
                t.push({
                    id,
                    phase: params[0],
                    table_name: params[1],
                    range_start: params[2],
                    range_end: params[3],
                    peer_node_id: params[4],
                    expires_at: params[5],
                    created_at: params[6],
                });
                return { changes: 1 };
            }
            if (sql.startsWith('DELETE FROM _full_sync_claims WHERE id =')) {
                const t = getTable('_full_sync_claims');
                const i = t.findIndex(r => r.id === params[0]);
                if (i >= 0) { t.splice(i, 1); return { changes: 1 }; }
                return { changes: 0 };
            }
            if (sql.startsWith('DELETE FROM _full_sync_claims WHERE peer_node_id = ? AND phase = ?')) {
                const t = getTable('_full_sync_claims');
                const tableMatch = sql.includes('table_name IS NULL')
                    ? (r) => r.table_name === null || r.table_name === undefined
                    : (r) => r.table_name === params[2];
                const before = t.length;
                for (let i = t.length - 1; i >= 0; i--) {
                    if (t[i].peer_node_id === params[0] && t[i].phase === params[1] && tableMatch(t[i])) t.splice(i, 1);
                }
                return { changes: before - t.length };
            }
            if (sql.startsWith('DELETE FROM _full_sync_claims WHERE peer_node_id')) {
                const t = getTable('_full_sync_claims');
                const before = t.length;
                for (let i = t.length - 1; i >= 0; i--) {
                    if (t[i].peer_node_id === params[0]) t.splice(i, 1);
                }
                return { changes: before - t.length };
            }
            if (sql.startsWith('DELETE FROM _full_sync_claims WHERE expires_at')) {
                const t = getTable('_full_sync_claims');
                const cutoff = params[0];
                const before = t.length;
                for (let i = t.length - 1; i >= 0; i--) {
                    if (t[i].expires_at <= cutoff) t.splice(i, 1);
                }
                return { changes: before - t.length };
            }
            if (sql.startsWith('DELETE') && sql.includes('_tombstones')) {
                const t = getTable('_tombstones');
                const idx = t.findIndex(r => r.table_name === params[0] && r.row_key === params[1]);
                if (idx >= 0) t.splice(idx, 1);
                return { changes: idx >= 0 ? 1 : 0 };
            }
            if (sql.startsWith('DELETE') && sql.includes('_sync_log')) {
                const t = getTable('_sync_log');
                const limit = params[0];
                t.splice(0, limit);
                return { changes: limit };
            }
            return { changes: 0 };
        },
        get(sql, params = []) {
            if (sql.includes('MAX(id)') && sql.includes('_sync_log')) {
                const t = getTable('_sync_log');
                return { max_seq: t.length > 0 ? Math.max(...t.map(r => r.id)) : 0 };
            }
            if (sql.includes('MIN(id)') && sql.includes('_sync_log')) {
                const t = getTable('_sync_log');
                return { min_seq: t.length > 0 ? Math.min(...t.map(r => r.id)) : 0 };
            }
            if (sql.includes('COUNT(*)') && sql.includes('_sync_log')) {
                return { cnt: getTable('_sync_log').length };
            }
            if (sql.includes('last_sync_id') && sql.includes('_sync_peers')) {
                const t = getTable('_sync_peers');
                const row = t.find(r => r.peer_key === params[0]);
                return row ? { last_sync_id: row.last_sync_id } : null;
            }
            if (sql.includes('_hlc FROM _tombstones')) {
                const t = getTable('_tombstones');
                const row = t.find(r => r.table_name === params[0] && r.row_key === params[1]);
                return row ? { _hlc: row._hlc } : null;
            }
            // SELECT _hlc FROM <tbl> WHERE <keyCol> = ? AND ...
            const selHlcMatch = sql.match(/^SELECT _hlc FROM (\w+) WHERE /);
            if (selHlcMatch && !['_sync_log', '_sync_peers', '_tombstones'].includes(selHlcMatch[1])) {
                const tbl = selHlcMatch[1];
                const keyCols = tableKeys[tbl];
                if (!keyCols) return null;
                const t = getTable(tbl);
                const row = t.find(r => keyCols.every((k, i) => r[k] === params[i]));
                return row ? { _hlc: row._hlc } : null;
            }
            if (sql.startsWith('SELECT id, watermark_seq FROM _full_sync_needs')) {
                const t = getTable('_full_sync_needs');
                const phase = params[0];
                const hasTable = sql.includes('table_name = ?');
                const tbl = hasTable ? params[1] : null;
                const row = t.find(r =>
                    r.phase === phase &&
                    (hasTable ? r.table_name === tbl : (r.table_name === null || r.table_name === undefined))
                );
                return row ? { id: row.id, watermark_seq: row.watermark_seq } : null;
            }
            if (sql.startsWith('SELECT MAX(id) AS id FROM _full_sync_needs')) {
                const t = getTable('_full_sync_needs');
                return { id: t.length > 0 ? Math.max(...t.map(r => r.id)) : 0 };
            }
            if (sql.startsWith('SELECT MAX(id) AS id FROM _full_sync_claims')) {
                const t = getTable('_full_sync_claims');
                return { id: t.length > 0 ? Math.max(...t.map(r => r.id)) : 0 };
            }
            return null;
        },
        all(sql, params = []) {
            if (sql.includes('_sync_log') && sql.includes('WHERE id >')) {
                const sinceSeq = params[0];
                const limit = params[1];
                const t = getTable('_sync_log');
                return t.filter(r => r.id > sinceSeq).slice(0, limit).map(r => ({ seq: r.id, ...r }));
            }
            // _full_sync_needs queries
            if (sql.includes('FROM _full_sync_needs')) {
                const t = getTable('_full_sync_needs');
                if (sql.includes('WHERE phase = ?')) {
                    const phase = params[0];
                    const hasTable = sql.includes('table_name = ?');
                    const tbl = hasTable ? params[1] : null;
                    return t.filter(r =>
                        r.phase === phase &&
                        (hasTable ? r.table_name === tbl : (r.table_name === null || r.table_name === undefined))
                    ).map(r => ({ ...r }));
                }
                return t.map(r => ({ ...r }));
            }
            // _full_sync_claims queries
            if (sql.includes('FROM _full_sync_claims')) {
                const t = getTable('_full_sync_claims');
                if (sql.includes('WHERE expires_at >')) {
                    const cutoff = params[0];
                    return t.filter(r => r.expires_at > cutoff).map(r => ({ ...r }));
                }
                return t.map(r => ({ ...r }));
            }
            if (sql.includes('_tombstones')) return getTable('_tombstones');
            // SELECT col1, col2 FROM tbl ORDER BY keys
            const scanMatch = sql.match(/^SELECT (.+) FROM (\w+) ORDER BY/);
            if (scanMatch && !['_sync_log', '_sync_peers', '_tombstones'].includes(scanMatch[2])) {
                return getTable(scanMatch[2]).slice();
            }
            return [];
        },
        exec() {},
        // Dialect methods (SQLite)
        beginTransaction() {
            inTxn = true;
            txnSnapshot = {};
            for (const k of Object.keys(tables)) txnSnapshot[k] = tables[k].slice();
        },
        commit() { inTxn = false; txnSnapshot = null; },
        rollback() {
            inTxn = false;
            if (txnSnapshot) {
                for (const k of Object.keys(tables)) delete tables[k];
                for (const k of Object.keys(txnSnapshot)) tables[k] = txnSnapshot[k];
            }
            txnSnapshot = null;
        },
        upsertSQL(table, columns, _keyColumns) {
            const cols = columns.join(', ');
            const ph = columns.map(() => '?').join(', ');
            return `INSERT OR REPLACE INTO ${table} (${cols}) VALUES (${ph})`;
        },
        infraSchemaSQL() { return []; },
        triggersSQL() { return []; },
        dropTriggersSQL() { return []; },
    };
}

function createEngine(overrides = {}) {
    const db = createInMemoryDb();
    const getNow = makeGetNow();
    const engine = new SyncEngine({
        nodeId: 'node-0',
        db,
        dialect: 'sqlite',
        getNow,
        timerAPI: makeTimerAPI(),
        onSendToPeer: vi.fn(),
        onClosePeer: vi.fn(),
        onLeaderChanged: vi.fn(),
        onExecuteProxyRequest: vi.fn().mockResolvedValue({ ok: true }),
        numShards: 1,
        heartbeatTimeoutMs: 5000,
        heartbeatIntervalMs: 1000,
        pullIntervalMs: 10000,
        cleanupIntervalMs: 3600000,
        ...overrides,
    });
    // 默认启用 SQL 触发器作为变更检测源（与旧行为一致），
    // 让那些只调用 start() 而不显式 initTriggers() 的已有用例继续通过。
    engine.initTriggers();
    return { engine, db };
}

// ═══════════════════════════════════════════════════
//  Constructor validation
// ═══════════════════════════════════════════════════

describe('SyncEngine constructor', () => {
    it('throws when getNow is missing', () => {
        expect(() => new SyncEngine({
            nodeId: 'n', db: createInMemoryDb(), dialect: 'sqlite',
            timerAPI: makeTimerAPI(),
            onSendToPeer: vi.fn(), onClosePeer: vi.fn(),
            onLeaderChanged: vi.fn(), onExecuteProxyRequest: vi.fn(),
        })).toThrow('getNow is required');
    });

    it('throws when getNow is not a function', () => {
        expect(() => new SyncEngine({
            nodeId: 'n', db: createInMemoryDb(), dialect: 'sqlite',
            getNow: 12345,
            timerAPI: makeTimerAPI(),
            onSendToPeer: vi.fn(), onClosePeer: vi.fn(),
            onLeaderChanged: vi.fn(), onExecuteProxyRequest: vi.fn(),
        })).toThrow('getNow is required');
    });

    it('throws when timerAPI is missing', () => {
        expect(() => new SyncEngine({
            nodeId: 'n', db: createInMemoryDb(), dialect: 'sqlite',
            getNow: makeGetNow(),
            onSendToPeer: vi.fn(), onClosePeer: vi.fn(),
            onLeaderChanged: vi.fn(), onExecuteProxyRequest: vi.fn(),
        })).toThrow('timerAPI is required');
    });

    it('throws when onExecuteProxyRequest is missing', () => {
        expect(() => new SyncEngine({
            nodeId: 'n', db: createInMemoryDb(), dialect: 'sqlite',
            getNow: makeGetNow(), timerAPI: makeTimerAPI(),
            onSendToPeer: vi.fn(), onClosePeer: vi.fn(),
            onLeaderChanged: vi.fn(),
        })).toThrow('onExecuteProxyRequest is required');
    });

    it('creates engine with valid options', () => {
        const { engine } = createEngine();
        expect(engine.nodeId).toBe('node-0');
        expect(engine.hlc).toBeDefined();
    });

    it('passes getNow to HLC', () => {
        const { engine } = createEngine();
        // HLC should have been initialized with getNow
        expect(engine.hlc._getNow).toBeDefined();
        const result = engine.hlc._getNow();
        expect(result).toHaveProperty('value');
        expect(result).toHaveProperty('unit');
    });

    it('accepts getNow returning seconds', () => {
        const { engine } = createEngine({
            getNow: () => ({ value: 1713000, unit: 's' }),
        });
        expect(engine.hlc.wallTime).toBe(1713000000); // converted to ms
    });

    it('accepts getNow returning microseconds', () => {
        const { engine } = createEngine({
            getNow: () => ({ value: 1713000000000000, unit: 'us' }),
        });
        expect(engine.hlc.wallTime).toBe(1713000000000);
    });

    it('accepts getNow returning nanoseconds', () => {
        const { engine } = createEngine({
            getNow: () => ({ value: 1713000000000000000, unit: 'ns' }),
        });
        expect(engine.hlc.wallTime).toBe(1713000000000);
    });
});

// ═══════════════════════════════════════════════════
//  Table registration
// ═══════════════════════════════════════════════════

describe('SyncEngine registerTable', () => {
    it('registers a table', () => {
        const { engine } = createEngine();
        engine.registerTable('users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', 'name', '_hlc'],
        });
        expect(engine.getTableRegistry().has('users')).toBe(true);
    });

    it('throws on missing keyColumns', () => {
        const { engine } = createEngine();
        expect(() => engine.registerTable('t', { dataColumns: ['id', '_hlc'] })).toThrow('keyColumns');
    });

    it('throws on missing dataColumns', () => {
        const { engine } = createEngine();
        expect(() => engine.registerTable('t', { keyColumns: ['id'] })).toThrow('dataColumns');
    });

    it('throws when dataColumns lacks _hlc', () => {
        const { engine } = createEngine();
        expect(() => engine.registerTable('t', {
            keyColumns: ['id'], dataColumns: ['id', 'name'],
        })).toThrow('_hlc');
    });
});

// ═══════════════════════════════════════════════════
//  Lifecycle
// ═══════════════════════════════════════════════════

describe('SyncEngine lifecycle', () => {
    beforeEach(() => { vi.useFakeTimers(); });
    afterEach(() => { vi.useRealTimers(); });

    it('start is idempotent', () => {
        const { engine } = createEngine();
        engine.start();
        engine.start(); // no error
        engine.stop();
    });

    it('stop clears all state', () => {
        const { engine } = createEngine();
        engine.start();
        engine.stop();
        expect(engine._started).toBe(false);
    });
});

// ═══════════════════════════════════════════════════
//  Peer management
// ═══════════════════════════════════════════════════

describe('SyncEngine peer management', () => {
    beforeEach(() => { vi.useFakeTimers(); });
    afterEach(() => { vi.useRealTimers(); });

    it('peerConnected registers a peer and sends handshake', () => {
        const onSendToPeer = vi.fn();
        const { engine } = createEngine({ onSendToPeer });
        engine.start();
        engine.peerConnected('p1', { direction: 'outbound' });

        expect(engine._peers.has('p1')).toBe(true);
        expect(onSendToPeer).toHaveBeenCalledWith('p1', expect.any(String));
        const msg = JSON.parse(onSendToPeer.mock.calls[0][1]);
        expect(msg.type).toBe('handshake');
        expect(msg.nodeId).toBe('node-0');
        engine.stop();
    });

    it('peerDisconnected removes peer', () => {
        const { engine } = createEngine();
        engine.start();
        engine.peerConnected('p1');
        expect(engine._peers.has('p1')).toBe(true);
        engine.peerDisconnected('p1');
        expect(engine._peers.has('p1')).toBe(false);
        engine.stop();
    });

    it('peerDisconnected is safe for unknown peer', () => {
        const { engine } = createEngine();
        engine.peerDisconnected('nonexistent'); // no error
    });
});

// ═══════════════════════════════════════════════════
//  Message handling
// ═══════════════════════════════════════════════════

describe('SyncEngine receiveMessage', () => {
    beforeEach(() => { vi.useFakeTimers(); });
    afterEach(() => { vi.useRealTimers(); });

    it('handles handshake → registers nodeId and maps peer', () => {
        const { engine } = createEngine();
        engine.start();
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        const peer = engine._peers.get('p1');
        expect(peer.nodeId).toBe('remote-1');
        expect(engine._nodeIdToPeerId.get('remote-1')).toBe('p1');
        engine.stop();
    });

    it('handles ping → responds with pong', () => {
        const onSendToPeer = vi.fn();
        const { engine } = createEngine({ onSendToPeer });
        engine.start();
        engine.peerConnected('p1');
        onSendToPeer.mockClear();

        engine.receiveMessage('p1', JSON.stringify({ type: 'ping' }));
        const pongCall = onSendToPeer.mock.calls.find(c => JSON.parse(c[1]).type === 'pong');
        expect(pongCall).toBeTruthy();
        engine.stop();
    });

    it('handles pong → updates lastActivity', () => {
        const { engine } = createEngine();
        engine.start();
        engine.peerConnected('p1');
        const before = engine._peers.get('p1').lastActivity;
        engine.receiveMessage('p1', JSON.stringify({ type: 'pong' }));
        expect(engine._peers.get('p1').lastActivity).toBeGreaterThanOrEqual(before);
        engine.stop();
    });

    it('ignores invalid JSON', () => {
        const { engine } = createEngine();
        engine.start();
        engine.peerConnected('p1');
        engine.receiveMessage('p1', 'not json'); // should not throw
        engine.stop();
    });

    it('ignores messages from unknown peers', () => {
        const { engine } = createEngine();
        engine.start();
        engine.receiveMessage('unknown', JSON.stringify({ type: 'ping' })); // should not throw
        engine.stop();
    });
});

// ═══════════════════════════════════════════════════
//  Heartbeat tick
// ═══════════════════════════════════════════════════

describe('SyncEngine _tickHeartbeat', () => {
    it('sends ping to idle peers', () => {
        let clock = 1000;
        const onSendToPeer = vi.fn();
        const { engine } = createEngine({
            getNow: () => ({ value: clock, unit: 'ms' }),
            onSendToPeer,
            heartbeatIntervalMs: 100,
            heartbeatTimeoutMs: 500,
        });

        engine.peerConnected('p1');
        onSendToPeer.mockClear();

        // Advance time past heartbeat interval
        clock = 1200;
        engine._tickHeartbeat();
        const pingCall = onSendToPeer.mock.calls.find(c => JSON.parse(c[1]).type === 'ping');
        expect(pingCall).toBeTruthy();
    });

    it('closes peers that exceed timeout', () => {
        let clock = 1000;
        const onClosePeer = vi.fn();
        const { engine } = createEngine({
            getNow: () => ({ value: clock, unit: 'ms' }),
            onClosePeer,
            heartbeatIntervalMs: 100,
            heartbeatTimeoutMs: 500,
        });

        engine.peerConnected('p1');

        // Advance time past heartbeat timeout
        clock = 2000;
        engine._tickHeartbeat();
        expect(onClosePeer).toHaveBeenCalledWith('p1', 'heartbeat_timeout');
    });
});

// ═══════════════════════════════════════════════════
//  Shard / Leader queries
// ═══════════════════════════════════════════════════

describe('SyncEngine shard/leader queries', () => {
    it('getShardId is deterministic', () => {
        const { engine } = createEngine();
        const id1 = engine.getShardId('user-123');
        const id2 = engine.getShardId('user-123');
        expect(id1).toBe(id2);
    });

    it('isLeaderForShard returns false initially', () => {
        const { engine } = createEngine();
        expect(engine.isLeaderForShard(0)).toBe(false);
    });

    it('getLeaderForShard returns null initially', () => {
        const { engine } = createEngine();
        expect(engine.getLeaderForShard(0)).toBeNull();
    });
});

// ═══════════════════════════════════════════════════
//  logChange / HLC tick
// ═══════════════════════════════════════════════════

describe('SyncEngine subscribeToChanges', () => {
    // 这些用例构造 engine 后要控制 initTriggers 是否调用，因此不能用默认的 createEngine（它会自动 initTriggers）。
    function rawEngine(overrides = {}) {
        const db = createInMemoryDb();
        const engine = new SyncEngine({
            nodeId: 'node-0',
            db,
            dialect: 'sqlite',
            getNow: makeGetNow(),
            timerAPI: makeTimerAPI(),
            onSendToPeer: vi.fn(),
            onClosePeer: vi.fn(),
            onLeaderChanged: vi.fn(),
            onExecuteProxyRequest: vi.fn().mockResolvedValue({ ok: true }),
            numShards: 1,
            heartbeatIntervalMs: 1000,
            pullIntervalMs: 10000,
            cleanupIntervalMs: 3600000,
            ...overrides,
        });
        return { engine, db };
    }

    it('start() throws when neither triggers nor callback configured', () => {
        const { engine } = rawEngine();
        expect(() => engine.start()).toThrow(/no change-detection source/);
    });

    it('start() passes with only triggers installed', () => {
        const { engine } = rawEngine();
        engine.initTriggers();
        expect(() => engine.start()).not.toThrow();
        engine.stop();
    });

    it('start() passes with only subscribeToChanges callback configured', () => {
        const callback = vi.fn();
        const { engine } = rawEngine({ subscribeToChanges: callback });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', '_hlc'] });
        engine.initChangeSubscriptions();
        expect(callback).toHaveBeenCalledTimes(1);
        expect(() => engine.start()).not.toThrow();
        engine.stop();
    });

    it('start() passes when both triggers and callback configured (coexist)', () => {
        const { engine } = rawEngine({ subscribeToChanges: vi.fn() });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', '_hlc'] });
        engine.initTriggers();
        engine.initChangeSubscriptions();
        expect(() => engine.start()).not.toThrow();
        engine.stop();
    });

    it('initChangeSubscriptions throws when callback not provided', () => {
        const { engine } = rawEngine();
        expect(() => engine.initChangeSubscriptions()).toThrow(/not provided/);
    });

    it('initChangeSubscriptions throws when no tables registered', () => {
        const { engine } = rawEngine({ subscribeToChanges: vi.fn() });
        expect(() => engine.initChangeSubscriptions()).toThrow(/no tables registered/);
    });

    it('constructor rejects non-function subscribeToChanges', () => {
        expect(() => rawEngine({ subscribeToChanges: 'nope' })).toThrow(/must be a function/);
    });

    it('registerTable rejects non-string where', () => {
        const { engine } = rawEngine();
        expect(() => engine.registerTable('t', {
            keyColumns: ['id'], dataColumns: ['id', '_hlc'], where: 42,
        })).toThrow(/where, if provided, must be a string/);
    });

    it('invokes callback once per registered table with that table\'s where', () => {
        const received = [];
        const { engine } = rawEngine({
            subscribeToChanges: (ctx) => { received.push(ctx); },
        });
        engine.registerTable('users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', 'name', '_hlc'],
            where: 'deleted_at IS NULL',
        });
        engine.registerTable('orders', {
            keyColumns: ['order_id'],
            dataColumns: ['order_id', 'total', '_hlc'],
        });
        engine.initChangeSubscriptions();

        expect(received).toHaveLength(2);
        const byTable = Object.fromEntries(received.map(c => [c.table, c]));
        expect(byTable.users).toMatchObject({ table: 'users', where: 'deleted_at IS NULL' });
        expect(byTable.users.keyColumns).toEqual(['user_id']);
        expect(byTable.orders).toMatchObject({ table: 'orders', where: null });
        expect(byTable.orders.dataColumns).toEqual(['order_id', 'total', '_hlc']);
        for (const ctx of received) expect(typeof ctx.onChanges).toBe('function');
    });

    it('onChanges writes events to _sync_log', () => {
        let onChanges;
        const { engine, db } = rawEngine({
            subscribeToChanges: (ctx) => { onChanges = ctx.onChanges; },
        });
        engine.registerTable('users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', 'name', '_hlc'],
        });
        engine.initChangeSubscriptions();

        onChanges(
            ['INSERT', 'UPDATE', 'DELETE'],
            [
                { tableName: 'users', rowKey: { user_id: 'u1' }, rowData: { user_id: 'u1', name: 'A', _hlc: 'hlc-1' }, hlc: 'hlc-1' },
                { tableName: 'users', rowKey: { user_id: 'u1' }, rowData: { user_id: 'u1', name: 'B', _hlc: 'hlc-2' }, hlc: 'hlc-2' },
                { tableName: 'users', rowKey: { user_id: 'u1' }, rowData: null, hlc: 'hlc-3' },
            ]
        );

        const log = db._tables._sync_log;
        expect(log.length).toBe(3);
        expect(log[0]).toMatchObject({ table_name: 'users', operation: 'INSERT', _hlc: 'hlc-1' });
        expect(JSON.parse(log[0].row_key)).toEqual({ user_id: 'u1' });
        expect(JSON.parse(log[0].row_data)).toEqual({ user_id: 'u1', name: 'A', _hlc: 'hlc-1' });
        expect(log[2]).toMatchObject({ table_name: 'users', operation: 'DELETE', _hlc: 'hlc-3' });
        expect(log[2].row_data).toBeNull();
    });

    it('onChanges validates inputs', () => {
        let onChanges;
        const { engine } = rawEngine({
            subscribeToChanges: (ctx) => { onChanges = ctx.onChanges; },
        });
        engine.registerTable('users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', '_hlc'],
        });
        engine.initChangeSubscriptions();

        expect(() => onChanges('INSERT', [])).toThrow(/must both be arrays/);
        expect(() => onChanges(['INSERT'], [])).toThrow(/same length/);
        expect(() => onChanges(['WEIRD'], [{ tableName: 'users', rowKey: { user_id: 'u1' }, rowData: { user_id: 'u1' }, hlc: 'h' }]))
            .toThrow(/invalid eventType/);
        expect(() => onChanges(['INSERT'], [{ tableName: 'ghosts', rowKey: {}, rowData: {}, hlc: 'h' }]))
            .toThrow(/unknown tableName/);
        expect(() => onChanges(['INSERT'], [{ tableName: 'users', rowKey: { user_id: 'u1' }, rowData: null, hlc: 'h' }]))
            .toThrow(/rowData at index 0 is required/);
        expect(() => onChanges(['INSERT'], [{ tableName: 'users', rowKey: { user_id: 'u1' }, rowData: { user_id: 'u1' }, hlc: '' }]))
            .toThrow(/hlc at index 0/);
    });

    it('onChanges batch is atomic: mid-batch failure rolls back earlier writes', () => {
        let onChanges;
        const { engine, db } = rawEngine({
            subscribeToChanges: (ctx) => { onChanges = ctx.onChanges; },
        });
        engine.registerTable('users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', '_hlc'],
        });
        engine.initChangeSubscriptions();

        // Second event is invalid (unknown table). First one must NOT end up in _sync_log.
        expect(() => onChanges(
            ['INSERT', 'INSERT'],
            [
                { tableName: 'users',  rowKey: { user_id: 'u1' }, rowData: { user_id: 'u1', _hlc: 'h-1' }, hlc: 'h-1' },
                { tableName: 'ghosts', rowKey: { id: 'x' },        rowData: { id: 'x' },                   hlc: 'h-2' },
            ]
        )).toThrow(/unknown tableName/);

        expect((db._tables._sync_log || []).length).toBe(0);
    });

    it('end-to-end multi-table: sync-lib drives subscriptions from registerTable, external only sees what it is told', () => {
        const subscriptions = [];          // what external "subscribed" to
        let sharedOnChanges;                // onChanges shared across all invocations

        const { engine, db } = rawEngine({
            subscribeToChanges: ({ table, where, onChanges }) => {
                // External doesn't know the full registry; just records what this invocation told it.
                subscriptions.push({ table, where });
                sharedOnChanges = onChanges;
            },
        });
        engine.registerTable('users',  {
            keyColumns: ['user_id'],  dataColumns: ['user_id', '_hlc'],
            where: 'deleted_at IS NULL',
        });
        engine.registerTable('orders', {
            keyColumns: ['order_id'], dataColumns: ['order_id', '_hlc'],
        });
        engine.initChangeSubscriptions();

        // sync-lib must have invoked the callback once per registered table, with that table's own where.
        expect(subscriptions).toHaveLength(2);
        const byTable = Object.fromEntries(subscriptions.map(s => [s.table, s]));
        expect(byTable.users).toEqual({ table: 'users', where: 'deleted_at IS NULL' });
        expect(byTable.orders).toEqual({ table: 'orders', where: null });

        // External reports events across different tables via the single onChanges; all land in _sync_log.
        sharedOnChanges(
            ['INSERT', 'INSERT'],
            [
                { tableName: 'users',  rowKey: { user_id: 'u1' },  rowData: { user_id: 'u1', _hlc: 'h-1' }, hlc: 'h-1' },
                { tableName: 'orders', rowKey: { order_id: 'o1' }, rowData: { order_id: 'o1', _hlc: 'h-2' }, hlc: 'h-2' },
            ]
        );

        expect(db._tables._sync_log).toHaveLength(2);
        expect(db._tables._sync_log[0]).toMatchObject({ table_name: 'users',  operation: 'INSERT' });
        expect(db._tables._sync_log[1]).toMatchObject({ table_name: 'orders', operation: 'INSERT' });
    });

    it('initTriggers({ changeLog: false }) does NOT satisfy change-detection requirement', () => {
        const { engine } = rawEngine();
        engine.initTriggers({ changeLog: false });
        expect(() => engine.start()).toThrow(/no change-detection source/);
    });

    it('end-to-end: events injected via onChanges flow out to peers via sync message', () => {
        const onSendToPeer = vi.fn();
        let onChanges;
        const { engine } = rawEngine({
            onSendToPeer,
            subscribeToChanges: (ctx) => { onChanges = ctx.onChanges; },
        });
        engine.registerTable('users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', 'name', '_hlc'],
            where: 'deleted_at IS NULL',
        });
        engine.initChangeSubscriptions();
        engine.start();

        // Peer connects (direction=inbound so engine pushes on handshake).
        engine.peerConnected('p1', { direction: 'inbound' });
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        // User detected a change externally and reports it.
        const hlc1 = engine.hlc.tick();
        const hlc2 = engine.hlc.tick();
        const hlc3 = engine.hlc.tick();
        onChanges(
            ['INSERT', 'UPDATE', 'DELETE'],
            [
                { tableName: 'users', rowKey: { user_id: 'u1' }, rowData: { user_id: 'u1', name: 'A', _hlc: hlc1 }, hlc: hlc1 },
                { tableName: 'users', rowKey: { user_id: 'u1' }, rowData: { user_id: 'u1', name: 'A2', _hlc: hlc2 }, hlc: hlc2 },
                { tableName: 'users', rowKey: { user_id: 'u2' }, rowData: null, hlc: hlc3 },
            ]
        );

        // Trigger push. Engine should read _sync_log and forward as 'sync' to peer.
        engine.notifyLocalWrite();

        const syncCalls = onSendToPeer.mock.calls
            .map(([, raw]) => JSON.parse(raw))
            .filter(m => m.type === 'sync');
        expect(syncCalls.length).toBeGreaterThan(0);

        const allEntries = syncCalls.flatMap(m => m.entries);
        const ins = allEntries.find(e => e.table_name === 'users' && e.operation === 'INSERT');
        const upd = allEntries.find(e => e.table_name === 'users' && e.operation === 'UPDATE');
        const del = allEntries.find(e => e.table_name === 'users' && e.operation === 'DELETE');
        expect(ins).toBeTruthy();
        expect(JSON.parse(ins.row_key)).toEqual({ user_id: 'u1' });
        expect(JSON.parse(ins.row_data)).toEqual({ user_id: 'u1', name: 'A', _hlc: hlc1 });
        expect(ins._hlc).toBe(hlc1);
        expect(upd).toBeTruthy();
        expect(JSON.parse(upd.row_key)).toEqual({ user_id: 'u1' });
        expect(JSON.parse(upd.row_data)).toEqual({ user_id: 'u1', name: 'A2', _hlc: hlc2 });
        expect(upd._hlc).toBe(hlc2);
        expect(del).toBeTruthy();
        expect(JSON.parse(del.row_key)).toEqual({ user_id: 'u2' });
        expect(del.row_data).toBeNull();
        expect(del._hlc).toBe(hlc3);

        engine.stop();
    });

    it('initTriggers({ changeLog: false }) installs only HLC triggers via split API', () => {
        const installed = [];
        const db = createInMemoryDb();
        db.hlcTriggersSQL = (t) => [`-- hlc ${t}`];
        db.changeLogTriggersSQL = (t) => [`-- changelog ${t}`];
        db.exec = (sql) => { installed.push(sql); };

        const engine = new SyncEngine({
            nodeId: 'node-0', db,
            getNow: makeGetNow(), timerAPI: makeTimerAPI(),
            onSendToPeer: vi.fn(), onClosePeer: vi.fn(),
            onLeaderChanged: vi.fn(),
            onExecuteProxyRequest: vi.fn().mockResolvedValue({ ok: true }),
            numShards: 1,
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', '_hlc'] });
        engine.initTriggers({ changeLog: false });

        expect(installed).toEqual(['-- hlc users']);
    });
});

describe('SyncEngine full-resync on handshake gap', () => {
    function makeEngine(overrides = {}) {
        const db = createInMemoryDb({ tableKeys: overrides.tableKeys || {} });
        const engine = new SyncEngine({
            nodeId: overrides.nodeId || 'node-A',
            db,
            dialect: 'sqlite',
            getNow: makeGetNow(),
            timerAPI: makeTimerAPI(),
            onSendToPeer: overrides.onSendToPeer || vi.fn(),
            onClosePeer: vi.fn(),
            onLeaderChanged: vi.fn(),
            onExecuteProxyRequest: vi.fn().mockResolvedValue({ ok: true }),
            numShards: 1,
            heartbeatIntervalMs: 1000,
            pullIntervalMs: 10000,
            cleanupIntervalMs: 3600000,
            ...overrides.engineOpts,
        });
        return { engine, db };
    }

    it('outbound handshake with peer ahead triggers full_sync_resume_req (receiver-initiated)', () => {
        const sent = [];
        const { engine } = makeEngine({
            onSendToPeer: (peerId, raw) => sent.push(JSON.parse(raw)),
            tableKeys: { users: ['user_id'] },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        engine.initTriggers();
        engine.start();

        // Local has no data; peer B claims lastSeq=100 → we're behind.
        engine.peerConnected('pB', { direction: 'outbound' });
        sent.length = 0;
        engine.receiveMessage('pB', JSON.stringify({ type: 'handshake', nodeId: 'node-B', lastSeq: 100, minLogId: 0 }));

        const resumeReqs = sent.filter(m => m.type === 'full_sync_resume_req');
        expect(resumeReqs.length).toBeGreaterThanOrEqual(2);  // rows(users) + tombstones
        const phases = new Set(resumeReqs.map(m => m.request.phase));
        expect(phases.has('rows')).toBe(true);
        expect(phases.has('tombstones')).toBe(true);
        for (const r of resumeReqs) expect(r.request.watermarkSeq).toBe(100);

        engine.stop();
    });

    it('no gap when peer lastSeq inside log range — normal sync path', () => {
        const sent = [];
        const { engine, db } = makeEngine({
            onSendToPeer: (peerId, raw) => sent.push(JSON.parse(raw)),
            tableKeys: { users: ['user_id'] },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        engine.initTriggers();
        db._tables._sync_log = [
            { id: 1, table_name: 'users', operation: 'INSERT', row_key: '{"user_id":"u1"}', row_data: '{"user_id":"u1","_hlc":"h1"}', _hlc: 'h1' },
            { id: 2, table_name: 'users', operation: 'INSERT', row_key: '{"user_id":"u2"}', row_data: '{"user_id":"u2","_hlc":"h2"}', _hlc: 'h2' },
        ];
        engine.start();

        engine.peerConnected('pB', { direction: 'inbound' });
        sent.length = 0;
        engine.receiveMessage('pB', JSON.stringify({ type: 'handshake', nodeId: 'node-B', lastSeq: 1, minLogId: 1 }));

        expect(sent.find(m => m.type === 'full_sync_rows')).toBeUndefined();
        const syncMsg = sent.find(m => m.type === 'sync');
        expect(syncMsg).toBeTruthy();
        expect(syncMsg.entries.map(e => e.seq)).toEqual([2]);

        engine.stop();
    });

    it('receiver applies full_sync_rows into business table via HLC LWW', () => {
        const { engine, db } = makeEngine({
            nodeId: 'node-B',
            tableKeys: { users: ['user_id'] },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        engine.initTriggers();
        engine.start();
        engine.peerConnected('pA', { direction: 'outbound' });
        engine.receiveMessage('pA', JSON.stringify({ type: 'handshake', nodeId: 'node-A', lastSeq: 0, minLogId: 0 }));

        const hlc1 = engine.hlc.tick();
        const hlc2 = engine.hlc.tick();
        engine.receiveMessage('pA', JSON.stringify({
            type: 'full_sync_rows',
            table: 'users',
            rows: [
                { user_id: 'u1', name: 'Alice', _hlc: hlc1 },
                { user_id: 'u2', name: 'Bob',   _hlc: hlc2 },
            ],
            nextLastKey: null,
            watermarkSeq: 99,
        }));
        engine.receiveMessage('pA', JSON.stringify({ type: 'full_sync_complete', watermarkSeq: 99 }));

        expect(db._tables.users).toHaveLength(2);
        expect(db._tables.users.find(r => r.user_id === 'u1')).toMatchObject({ user_id: 'u1', name: 'Alice' });
        expect(db._tables.users.find(r => r.user_id === 'u2')).toMatchObject({ user_id: 'u2', name: 'Bob' });

        engine.stop();
    });

    it('receiver does not overwrite newer local row (HLC LWW)', () => {
        const { engine, db } = makeEngine({
            nodeId: 'node-B',
            tableKeys: { users: ['user_id'] },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        engine.initTriggers();
        engine.start();

        // Local has u1 with a very new HLC
        const newerHlc = engine.hlc.tick();
        db._tables.users = [{ user_id: 'u1', name: 'LocalNew', _hlc: newerHlc }];

        // Remote pushes older snapshot
        engine.peerConnected('pA', { direction: 'outbound' });
        engine.receiveMessage('pA', JSON.stringify({ type: 'handshake', nodeId: 'node-A', lastSeq: 0, minLogId: 0 }));
        engine.receiveMessage('pA', JSON.stringify({
            type: 'full_sync_rows',
            table: 'users',
            rows: [{ user_id: 'u1', name: 'RemoteOld', _hlc: '000000000000000:0000:nodeA' }],
            nextLastKey: null,
            watermarkSeq: 99,
        }));

        expect(db._tables.users[0]).toMatchObject({ user_id: 'u1', name: 'LocalNew' });
        engine.stop();
    });

    it('full_sync_tombstones removes local rows whose delete is newer', () => {
        const { engine, db } = makeEngine({
            nodeId: 'node-B',
            tableKeys: { users: ['user_id'] },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', '_hlc'] });
        engine.initTriggers();
        engine.start();

        const oldHlc = '000000000000000:0000:nodeA';
        db._tables.users = [{ user_id: 'u1', _hlc: oldHlc }];

        engine.peerConnected('pA', { direction: 'outbound' });
        engine.receiveMessage('pA', JSON.stringify({ type: 'handshake', nodeId: 'node-A', lastSeq: 0, minLogId: 0 }));
        const delHlc = engine.hlc.tick();
        engine.receiveMessage('pA', JSON.stringify({
            type: 'full_sync_tombstones',
            tombstones: [{ table_name: 'users', row_key: '{"user_id":"u1"}', _hlc: delHlc }],
        }));

        expect(db._tables.users).toHaveLength(0);
        expect(db._tables._tombstones).toHaveLength(1);
        engine.stop();
    });

    it('enableFullResync: false keeps old (silent-loss) behavior', () => {
        const sent = [];
        const { engine, db } = makeEngine({
            onSendToPeer: (peerId, raw) => sent.push(JSON.parse(raw)),
            tableKeys: { users: ['user_id'] },
            engineOpts: { enableFullResync: false },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', '_hlc'] });
        engine.initTriggers();
        for (let i = 50; i < 55; i++) {
            db._tables._sync_log = db._tables._sync_log || [];
            db._tables._sync_log.push({
                id: i, table_name: 'users', operation: 'INSERT',
                row_key: '{"user_id":"u' + i + '"}', row_data: '{"user_id":"u' + i + '","_hlc":"x"}', _hlc: 'x',
            });
        }
        engine.start();

        engine.peerConnected('pB', { direction: 'inbound' });
        sent.length = 0;
        engine.receiveMessage('pB', JSON.stringify({ type: 'handshake', nodeId: 'node-B', lastSeq: 5, minLogId: 0 }));

        expect(sent.find(m => m.type === 'full_sync_rows')).toBeUndefined();
        expect(sent.find(m => m.type === 'sync')).toBeTruthy();
        engine.stop();
    });
});

describe('SyncEngine full-resync needs/claims dispatcher', () => {
    function makeEngine(overrides = {}) {
        const db = createInMemoryDb({ tableKeys: overrides.tableKeys || {} });
        const engine = new SyncEngine({
            nodeId: overrides.nodeId || 'node-A',
            db,
            dialect: 'sqlite',
            getNow: makeGetNow(),
            timerAPI: makeTimerAPI(),
            onSendToPeer: overrides.onSendToPeer || vi.fn(),
            onClosePeer: vi.fn(),
            onLeaderChanged: vi.fn(),
            onExecuteProxyRequest: vi.fn().mockResolvedValue({ ok: true }),
            numShards: 1,
            heartbeatIntervalMs: 1000,
            pullIntervalMs: 10000,
            cleanupIntervalMs: 3600000,
            ...overrides.engineOpts,
        });
        return { engine, db };
    }

    it('page apply subtracts delivered range from needs (LWW: shrinks from left)', () => {
        const { engine, db } = makeEngine({
            tableKeys: { users: ['user_id'] },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        engine.initTriggers();
        engine.start();
        engine.peerConnected('pB', { direction: 'outbound' });
        engine.receiveMessage('pB', JSON.stringify({ type: 'handshake', nodeId: 'node-B', lastSeq: 100, minLogId: 0 }));

        // Peer B returns first page (null, u5].
        const hlc1 = engine.hlc.tick();
        engine.receiveMessage('pB', JSON.stringify({
            type: 'full_sync_rows',
            table: 'users',
            rows: [{ user_id: 'u1', name: 'A', _hlc: hlc1 }],
            pageStart: null,
            pageEnd:   { user_id: 'u5' },
            watermarkSeq: 100,
        }));

        // The (-∞, +∞] need should have been replaced by (u5, +∞].
        const rowNeeds = db._tables._full_sync_needs.filter(n => n.phase === 'rows' && n.table_name === 'users');
        expect(rowNeeds.length).toBe(1);
        expect(rowNeeds[0].range_start).toBe(JSON.stringify({ user_id: 'u5' }));
        expect(rowNeeds[0].range_end).toBeNull();

        engine.stop();
    });

    it('middle-page carving + unclaimed fragments drives parallel 2-peer dispatch', () => {
        const sent = [];
        const { engine, db } = makeEngine({
            onSendToPeer: (peerId, raw) => sent.push({ peerId, msg: JSON.parse(raw) }),
            tableKeys: { users: ['user_id'] },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        engine.initTriggers();
        engine.start();

        engine.peerConnected('pB', { direction: 'outbound' });
        engine.receiveMessage('pB', JSON.stringify({ type: 'handshake', nodeId: 'node-B', lastSeq: 100, minLogId: 0 }));
        engine.peerConnected('pC', { direction: 'outbound' });
        engine.receiveMessage('pC', JSON.stringify({ type: 'handshake', nodeId: 'node-C', lastSeq: 100, minLogId: 0 }));

        const firstAssignee = db._tables._full_sync_claims.find(c => c.phase === 'rows' && c.table_name === 'users');
        const firstPeerId = firstAssignee.peer_node_id === 'node-B' ? 'pB' : 'pC';

        // Simulate that peer returning a MIDDLE page (pageStart=u3, pageEnd=u5) + complete — carves a hole.
        const hlc1 = engine.hlc.tick();
        engine.receiveMessage(firstPeerId, JSON.stringify({
            type: 'full_sync_rows',
            table: 'users',
            rows: [{ user_id: 'u4', name: 'Mid', _hlc: hlc1 }],
            pageStart: { user_id: 'u3' },
            pageEnd:   { user_id: 'u5' },
            watermarkSeq: 100,
        }));
        // Release the claim so both peers are free for the gaps.
        engine.receiveMessage(firstPeerId, JSON.stringify({
            type: 'full_sync_complete',
            request: { phase: 'rows', tableName: 'users', rangeStart: null, rangeEnd: null, watermarkSeq: 100 },
        }));

        // Needs now have 2 fragments for rows(users).
        const rowNeeds = db._tables._full_sync_needs.filter(n => n.phase === 'rows' && n.table_name === 'users');
        expect(rowNeeds.length).toBe(2);

        // Dispatcher must have sent resume_reqs for rows(users) to BOTH peers — one fragment each.
        const rowReqs = sent.filter(s => s.msg.type === 'full_sync_resume_req' &&
                                          s.msg.request.phase === 'rows' &&
                                          s.msg.request.tableName === 'users');
        const targetedPeers = new Set(rowReqs.map(s => s.peerId));
        expect(targetedPeers.has('pB')).toBe(true);
        expect(targetedPeers.has('pC')).toBe(true);

        engine.stop();
    });

    it('peerDisconnected releases that peer\'s claims and re-dispatches', () => {
        const sent = [];
        const { engine, db } = makeEngine({
            onSendToPeer: (peerId, raw) => sent.push({ peerId, msg: JSON.parse(raw) }),
            tableKeys: { users: ['user_id'] },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        engine.initTriggers();
        engine.start();

        engine.peerConnected('pB', { direction: 'outbound' });
        engine.receiveMessage('pB', JSON.stringify({ type: 'handshake', nodeId: 'node-B', lastSeq: 100, minLogId: 0 }));

        // pB now owns claims.
        expect(db._tables._full_sync_claims.some(c => c.peer_node_id === 'node-B')).toBe(true);

        // Another peer joins but is not issued a claim yet (no free fragments — pB holds whole range).
        engine.peerConnected('pC', { direction: 'outbound' });
        engine.receiveMessage('pC', JSON.stringify({ type: 'handshake', nodeId: 'node-C', lastSeq: 100, minLogId: 0 }));
        sent.length = 0;

        // pB disconnects. Claims released; dispatcher should re-issue to pC.
        engine.peerDisconnected('pB');
        expect(db._tables._full_sync_claims.some(c => c.peer_node_id === 'node-B')).toBe(false);
        expect(db._tables._full_sync_claims.some(c => c.peer_node_id === 'node-C')).toBe(true);

        const resumeReqs = sent.filter(s => s.msg.type === 'full_sync_resume_req' && s.peerId === 'pC');
        expect(resumeReqs.length).toBeGreaterThanOrEqual(2);  // rows(users) + tombstones

        engine.stop();
    });

    it('full_sync_complete deletes only the matching scope claim (tombstones claim preserved)', () => {
        const sent = [];
        const { engine, db } = makeEngine({
            onSendToPeer: (peerId, raw) => sent.push({ peerId, msg: JSON.parse(raw) }),
            tableKeys: { users: ['user_id'] },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        engine.initTriggers();
        engine.start();

        engine.peerConnected('pB', { direction: 'outbound' });
        engine.receiveMessage('pB', JSON.stringify({ type: 'handshake', nodeId: 'node-B', lastSeq: 100, minLogId: 0 }));

        const rowClaimBefore = db._tables._full_sync_claims.find(c => c.peer_node_id === 'node-B' && c.phase === 'rows' && c.table_name === 'users');
        const tombClaimBefore = db._tables._full_sync_claims.find(c => c.peer_node_id === 'node-B' && c.phase === 'tombstones');
        expect(rowClaimBefore).toBeTruthy();
        expect(tombClaimBefore).toBeTruthy();

        // Complete only rows(users). After the handler deletes the scope claim the dispatcher
        // will re-issue (range still (-∞,+∞] because no page was applied), producing a NEW claim id.
        // Tombstones claim must be untouched (same id).
        engine.receiveMessage('pB', JSON.stringify({
            type: 'full_sync_complete',
            request: { phase: 'rows', tableName: 'users', rangeStart: null, rangeEnd: null, watermarkSeq: 100 },
        }));

        const rowClaimAfter = db._tables._full_sync_claims.find(c => c.peer_node_id === 'node-B' && c.phase === 'rows' && c.table_name === 'users');
        const tombClaimAfter = db._tables._full_sync_claims.find(c => c.peer_node_id === 'node-B' && c.phase === 'tombstones');
        expect(rowClaimAfter).toBeTruthy();
        expect(rowClaimAfter.id).not.toBe(rowClaimBefore.id);         // replaced
        expect(tombClaimAfter.id).toBe(tombClaimBefore.id);           // preserved

        engine.stop();
    });

    it('send-failure on a resume_req synchronously rolls back the claim and retries with another peer (primary recovery path)', () => {
        const sent = [];
        // Fail only resume_reqs to pB; let handshakes and everything else through.
        const onSendToPeer = (peerId, raw) => {
            const msg = JSON.parse(raw);
            if (peerId === 'pB' && msg.type === 'full_sync_resume_req') {
                throw new Error('pB send failed');
            }
            sent.push({ peerId, msg });
        };
        const { engine, db } = makeEngine({ onSendToPeer, tableKeys: { users: ['user_id'] } });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', '_hlc'] });
        engine.initTriggers();
        engine.start();

        engine.peerConnected('pB', { direction: 'outbound' });
        engine.peerConnected('pC', { direction: 'outbound' });
        engine.receiveMessage('pB', JSON.stringify({ type: 'handshake', nodeId: 'node-B', lastSeq: 100, minLogId: 0 }));
        engine.receiveMessage('pC', JSON.stringify({ type: 'handshake', nodeId: 'node-C', lastSeq: 100, minLogId: 0 }));

        // Every resume_req must have landed on pC (pB marked dead synchronously within the dispatch).
        const resumeReqs = sent.filter(s => s.msg.type === 'full_sync_resume_req');
        expect(resumeReqs.length).toBeGreaterThan(0);
        expect(resumeReqs.every(s => s.peerId === 'pC')).toBe(true);

        // Rollback invariant: no leftover claim owned by node-B.
        expect(db._tables._full_sync_claims.some(c => c.peer_node_id === 'node-B')).toBe(false);
        // And every pC claim is fresh (not expired) — these are the retries.
        expect(db._tables._full_sync_claims.some(c => c.peer_node_id === 'node-C')).toBe(true);

        engine.stop();
    });

    it('TTL cleanup is the fallback for a peer that silently stalls mid-fetch (no send error, just no response)', () => {
        // Scenario: pB accepted the resume_req (send returned ok) but never replies — no rollback path fires.
        // The claim's TTL is the ONLY recovery mechanism in this case.
        const sent = [];
        const { engine, db } = makeEngine({
            onSendToPeer: (peerId, raw) => sent.push({ peerId, msg: JSON.parse(raw) }),
            tableKeys: { users: ['user_id'] },
            engineOpts: { cleanupIntervalMs: 60000 },
        });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', '_hlc'] });
        engine.initTriggers();
        engine.start();

        engine.peerConnected('pB', { direction: 'outbound' });
        engine.receiveMessage('pB', JSON.stringify({ type: 'handshake', nodeId: 'node-B', lastSeq: 100, minLogId: 0 }));

        const stalledClaims = db._tables._full_sync_claims.filter(c => c.peer_node_id === 'node-B');
        expect(stalledClaims.length).toBeGreaterThan(0);
        const stalledIds = stalledClaims.map(c => c.id);

        // Simulate pB silently stuck: no response ever comes back. Time passes, TTL expires, pB is gone.
        for (const c of db._tables._full_sync_claims) c.expires_at = '1970-01-01T00:00:00.000Z';
        engine.peerDisconnected('pB');
        // Note: peerDisconnected also releases pB's claims directly — but in the true silent-stall case
        // (connection still alive, peer just not responding), peerDisconnected never fires. The TTL path
        // below is what runs in that case. We simulate it by re-inserting equivalent stale rows.
        for (const stale of stalledClaims) {
            db._tables._full_sync_claims.push({ ...stale, expires_at: '1970-01-01T00:00:00.000Z' });
        }

        engine._tickCleanup();

        // Every stale claim — whether released by peerDisconnected or aged out by TTL — is gone.
        for (const id of stalledIds) {
            expect(db._tables._full_sync_claims.some(c => c.id === id)).toBe(false);
        }
        expect(db._tables._full_sync_claims.some(c => c.expires_at === '1970-01-01T00:00:00.000Z')).toBe(false);

        engine.stop();
    });

    it('claim TTL adapts to per-peer RTT: fast LAN collapses to floor, slow WAN widens up to ceiling', () => {
        const { engine } = makeEngine({ tableKeys: { users: ['user_id'] } });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', '_hlc'] });
        engine.initTriggers();
        engine.start();

        // No RTT samples yet → falls back to the static ceiling.
        expect(engine._computeClaimTtlMs('pX')).toBe(engine._fullSyncClaimTtlMs);

        // Inject RTT samples for two peers (bypass the handshake dance).
        engine._timeSyncStates.set('pFast', { rtts: [10], minRtt: 10, epsilon: 5, offset: 0 });
        engine._timeSyncStates.set('pSlow', { rtts: [500], minRtt: 500, epsilon: 0, offset: 0 });
        engine._timeSyncStates.set('pHuge', { rtts: [60000], minRtt: 60000, epsilon: 0, offset: 0 });

        // Fast peer: 10ms × 50 = 500ms → clamped up to the 5s floor.
        expect(engine._computeClaimTtlMs('pFast')).toBe(engine._fullSyncClaimTtlFloorMs);
        // Slow peer: 500ms × 50 = 25s, well under the 10-min ceiling.
        expect(engine._computeClaimTtlMs('pSlow')).toBe(25000);
        // Pathologically slow: 60s × 50 = 3000s → clamped down to the ceiling (10 min).
        expect(engine._computeClaimTtlMs('pHuge')).toBe(engine._fullSyncClaimTtlMs);

        engine.stop();
    });
});

describe('SyncEngine logChange', () => {
    it('records change in _sync_log with HLC timestamp', () => {
        const { engine, db } = createEngine();
        const hlcTs = engine.logChange('users', 'INSERT', { user_id: 'u1' }, { user_id: 'u1', name: 'A' });

        expect(typeof hlcTs).toBe('string');
        expect(hlcTs).toContain('node-0');

        const t = db._tables._sync_log;
        expect(t.length).toBe(1);
        expect(t[0].table_name).toBe('users');
        expect(t[0].operation).toBe('INSERT');
    });
});

// ═══════════════════════════════════════════════════
//  getStatus
// ═══════════════════════════════════════════════════

describe('SyncEngine getStatus', () => {
    it('returns correct structure', () => {
        const { engine } = createEngine();
        const status = engine.getStatus();
        expect(status).toHaveProperty('started', false);
        expect(status).toHaveProperty('nodeId', 'node-0');
        expect(status).toHaveProperty('numShards', 1);
        expect(status).toHaveProperty('shards');
        expect(status).toHaveProperty('peers');
        expect(status).toHaveProperty('nodeIdMap');
        expect(status).toHaveProperty('currentMaxSeq');
    });

    it('includes connected peers', () => {
        const { engine } = createEngine();
        engine.peerConnected('p1');
        const status = engine.getStatus();
        expect(status.peers.length).toBe(1);
        expect(status.peers[0].peerId).toBe('p1');
    });
});

// ═══════════════════════════════════════════════════
//  notifyLocalWrite / push
// ═══════════════════════════════════════════════════

describe('SyncEngine notifyLocalWrite', () => {
    it('pushes data to connected peers', () => {
        const onSendToPeer = vi.fn();
        const { engine, db } = createEngine({ onSendToPeer });

        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));
        onSendToPeer.mockClear();

        // Insert data into sync log
        const hlcTs = engine.hlc.tick();
        db.run('INSERT INTO fake_sync_log VALUES (?)', []); // no-op for mock
        db._tables._sync_log = db._tables._sync_log || [];
        db._tables._sync_log.push({
            id: 1, table_name: 'users', operation: 'INSERT',
            row_key: '{"id":"u1"}', row_data: '{"id":"u1"}', _hlc: hlcTs,
        });

        engine.notifyLocalWrite();
        const syncCall = onSendToPeer.mock.calls.find(c => JSON.parse(c[1]).type === 'sync');
        expect(syncCall).toBeTruthy();
    });
});

// ═══════════════════════════════════════════════════
//  Deduplication (handshake)
// ═══════════════════════════════════════════════════

describe('SyncEngine peer deduplication', () => {
    it('handles duplicate connections from same nodeId', () => {
        const onClosePeer = vi.fn();
        const { engine } = createEngine({ onClosePeer });

        engine.peerConnected('p1', { direction: 'outbound' });
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        engine.peerConnected('p2', { direction: 'inbound' });
        engine.receiveMessage('p2', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        // One of the connections should be closed
        expect(onClosePeer).toHaveBeenCalled();
    });
});

// ═══════════════════════════════════════════════════
//  Manual transaction API
// ═══════════════════════════════════════════════════

describe('SyncEngine manual transaction', () => {
    it('beginManualTransaction / commitManualTransaction', () => {
        const { engine } = createEngine();
        engine.beginManualTransaction('txn-1');
        expect(engine.hasActiveManualTransaction()).toBe(true);
        engine.commitManualTransaction('txn-1');
        expect(engine.hasActiveManualTransaction()).toBe(false);
    });

    it('throws on duplicate active transaction', () => {
        const { engine } = createEngine();
        engine.beginManualTransaction('txn-1');
        expect(() => engine.beginManualTransaction('txn-2')).toThrow('Already has active');
        engine.rollbackManualTransaction('txn-1');
    });

    it('rollbackManualTransaction cleans up', () => {
        const { engine } = createEngine();
        engine.beginManualTransaction('txn-1');
        engine.rollbackManualTransaction('txn-1');
        expect(engine.hasActiveManualTransaction()).toBe(false);
    });

    it('getManualTransactionEntries throws for wrong writeId', () => {
        const { engine } = createEngine();
        engine.beginManualTransaction('txn-1');
        expect(() => engine.getManualTransactionEntries('wrong')).toThrow('No active manual transaction');
        engine.rollbackManualTransaction('txn-1');
    });

    it('commitManualTransaction throws for wrong writeId', () => {
        const { engine } = createEngine();
        engine.beginManualTransaction('txn-1');
        expect(() => engine.commitManualTransaction('wrong')).toThrow('No active manual transaction');
        engine.rollbackManualTransaction('txn-1');
    });

    it('write() joins an active manual transaction instead of opening its own', async () => {
        const { engine } = createEngine();
        engine.beginManualTransaction('outer-1');
        const result = await engine.write(async () => 'joined-result');
        expect(result).toBe('joined-result');
        // 事务仍归外层持有：未被 write() 提前提交/关闭，外层 writeId 仍可提交
        expect(engine.hasActiveManualTransaction()).toBe(true);
        engine.commitManualTransaction('outer-1');
        expect(engine.hasActiveManualTransaction()).toBe(false);
    });

    it('write() nested inside write() joins the outer write transaction', async () => {
        const { engine } = createEngine();
        const result = await engine.write(async () => {
            const inner = await engine.write(async () => 'inner-result');
            return inner;
        });
        expect(result).toBe('inner-result');
        expect(engine.hasActiveManualTransaction()).toBe(false);
    });

    it('standalone write() still works after a joined write and manual commit', async () => {
        const { engine } = createEngine();
        engine.beginManualTransaction('outer-1');
        await engine.write(async () => {});
        engine.commitManualTransaction('outer-1');
        const result = await engine.write(async () => 'standalone-result');
        expect(result).toBe('standalone-result');
        expect(engine.hasActiveManualTransaction()).toBe(false);
    });
});

// ═══════════════════════════════════════════════════
//  triggerElection
// ═══════════════════════════════════════════════════

describe('SyncEngine triggerElection', () => {
    beforeEach(() => { vi.useFakeTimers(); });
    afterEach(() => { vi.useRealTimers(); });

    it('triggers election for all shards', () => {
        const onSendToPeer = vi.fn();
        const { engine } = createEngine({ onSendToPeer });
        engine.start();
        engine.triggerElection();
        engine.stop();
    });

    // ── _testOptions 透传 ─────────────────────────────────────────────────
    it('_testOptions forwarded to LeaderElection: fast election succeeds', async () => {
        const { engine } = createEngine({
            _testOptions: { minElectionMs: 50, maxElectionMs: 100, heartbeatIntervalMs: 40 },
        });
        engine.start();
        await vi.advanceTimersByTimeAsync(200);
        expect(engine.isLeaderForShard(0)).toBe(true);
        engine.stop();
    });

    // ── 10d 对应：全分片重新选举 ──────────────────────────────────────────
    it('all shards enter candidate state immediately after triggerElection', async () => {
        // numShards=2，每个分片独立选举
        const { engine } = createEngine({
            numShards: 2,
            _testOptions: { minElectionMs: 50, maxElectionMs: 100, heartbeatIntervalMs: 40 },
        });
        engine.start();
        await vi.advanceTimersByTimeAsync(200); // 全分片成为 leader

        const before = engine.getStatus().shards;
        expect(before.every(s => s.state === 'leader')).toBe(true);

        engine.triggerElection();

        // 触发后所有分片应立刻退出 leader 状态（candidate 或 follower）
        const after = engine.getStatus().shards;
        expect(after.every(s => s.state !== 'leader')).toBe(true);
        engine.stop();
    });

    it('all shards re-elect after triggerElection', async () => {
        const onLeaderChanged = vi.fn();
        const { engine } = createEngine({
            numShards: 2,
            onLeaderChanged,
            _testOptions: { minElectionMs: 50, maxElectionMs: 100, heartbeatIntervalMs: 40 },
        });
        engine.start();
        await vi.advanceTimersByTimeAsync(200);

        const countBefore = onLeaderChanged.mock.calls.filter(c => c[2] === true).length;
        expect(countBefore).toBeGreaterThanOrEqual(2); // numShards=2

        engine.triggerElection();
        await vi.advanceTimersByTimeAsync(10); // 推进一个 tick 让 quorum 检查触发

        const countAfter = onLeaderChanged.mock.calls.filter(c => c[2] === true).length;
        expect(countAfter).toBeGreaterThan(countBefore); // 重新选举产生了新的 isLocal=true 事件
        engine.stop();
    });

    it('triggerElection includes stepdown events for each shard', async () => {
        const onLeaderChanged = vi.fn();
        const { engine } = createEngine({
            numShards: 2,
            onLeaderChanged,
            _testOptions: { minElectionMs: 50, maxElectionMs: 100, heartbeatIntervalMs: 40 },
        });
        engine.start();
        await vi.advanceTimersByTimeAsync(200);
        onLeaderChanged.mockClear();

        engine.triggerElection();

        // 降级事件（leaderNodeId=null）应出现，每个分片各一个
        const stepdownCalls = onLeaderChanged.mock.calls.filter(c => c[1] === null);
        expect(stepdownCalls.length).toBeGreaterThanOrEqual(2);
        engine.stop();
    });
});

// ═══════════════════════════════════════════════════
//  hasPeers
// ═══════════════════════════════════════════════════

describe('SyncEngine hasPeers', () => {
    it('returns false when no peers', () => {
        const { engine } = createEngine();
        expect(engine.hasPeers()).toBe(false);
    });

    it('returns true when peers exist', () => {
        const { engine } = createEngine();
        engine.peerConnected('p1');
        expect(engine.hasPeers()).toBe(true);
    });
});

// ═══════════════════════════════════════════════════
//  _nowMs internal conversion
// ═══════════════════════════════════════════════════

describe('SyncEngine _nowMs conversion', () => {
    it('converts seconds to ms', () => {
        const { engine } = createEngine({ getNow: () => ({ value: 100, unit: 's' }) });
        expect(engine._nowMs()).toBe(100000);
    });

    it('converts microseconds to ms', () => {
        const { engine } = createEngine({ getNow: () => ({ value: 100000, unit: 'us' }) });
        expect(engine._nowMs()).toBe(100);
    });

    it('converts nanoseconds to ms', () => {
        const { engine } = createEngine({ getNow: () => ({ value: 100000000, unit: 'ns' }) });
        expect(engine._nowMs()).toBe(100);
    });

    it('passes through ms directly', () => {
        const { engine } = createEngine({ getNow: () => ({ value: 12345, unit: 'ms' }) });
        expect(engine._nowMs()).toBe(12345);
    });

    it('memonicNow path: elapsed added to startNow', () => {
        // startNow = 1000ms, startMemonic = 0ns
        // after 500_000_000 ns elapsed → +500ms → expect 1500ms
        let memonicVal = 0;
        const { engine } = createEngine({
            getNow:     () => ({ value: 1000, unit: 'ms' }),
            memonicNow: () => ({ value: memonicVal, unit: 'ns' }),
        });
        // baseline: elapsed=0 → 1000ms
        expect(engine._nowMs()).toBe(1000);

        // advance monotonic by 500ms (500_000_000 ns)
        memonicVal = 500_000_000;
        expect(engine._nowMs()).toBe(1500);
    });

    it('memonicNow path: getNow jumping backward does NOT affect _nowMs', () => {
        // getNow simulates a backward clock jump — _nowMs must stay monotonic
        let wallMs = 2000;
        let memonicVal = 0;
        const { engine } = createEngine({
            getNow:     () => ({ value: wallMs, unit: 'ms' }),
            memonicNow: () => ({ value: memonicVal, unit: 'ns' }),
        });
        // startNow = 2000ms (captured at construction), startMemonic = 0
        memonicVal = 100_000_000; // +100ms elapsed
        expect(engine._nowMs()).toBe(2100);

        // system clock jumps back — but memonicNow keeps advancing
        wallMs = 1500;            // backward jump, should be ignored
        memonicVal = 200_000_000; // +200ms total elapsed
        expect(engine._nowMs()).toBe(2200); // still monotonic
    });

    it('memonicNow path: different units (ns) are converted correctly', () => {
        let nanoVal = 0;
        const { engine } = createEngine({
            getNow:     () => ({ value: 5000, unit: 'ms' }),
            memonicNow: () => ({ value: nanoVal, unit: 'ns' }),
        });
        nanoVal = 1_000_000; // 1ms in ns
        expect(engine._nowMs()).toBeCloseTo(5001, 5);
    });

    it('without memonicNow: falls back to getNow directly', () => {
        let t = 100;
        const { engine } = createEngine({ getNow: () => ({ value: t, unit: 'ms' }) });
        expect(engine._nowMs()).toBe(100);
        t = 200;
        expect(engine._nowMs()).toBe(200);
    });
});

// ═══════════════════════════════════════════════════
//  Leader heartbeat handling (via receiveMessage)
// ═══════════════════════════════════════════════════

describe('SyncEngine leader heartbeat', () => {
    beforeEach(() => { vi.useFakeTimers(); });
    afterEach(() => { vi.useRealTimers(); });

    it('processes leader_heartbeat message', () => {
        const onLeaderChanged = vi.fn();
        const { engine } = createEngine({ onLeaderChanged });
        engine.start();
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        engine.receiveMessage('p1', JSON.stringify({
            type: 'leader_heartbeat', shardId: 0, term: 1, leaderId: 'remote-1',
        }));

        expect(onLeaderChanged).toHaveBeenCalledWith(0, 'remote-1', false);
        engine.stop();
    });
});

// ═══════════════════════════════════════════════════
//  Vote message handling
// ═══════════════════════════════════════════════════

describe('SyncEngine vote handling', () => {
    beforeEach(() => { vi.useFakeTimers(); });
    afterEach(() => { vi.useRealTimers(); });

    it('processes vote_req and sends vote_ack', () => {
        const onSendToPeer = vi.fn();
        const { engine } = createEngine({ onSendToPeer });
        engine.start();
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));
        onSendToPeer.mockClear();

        engine.receiveMessage('p1', JSON.stringify({
            type: 'vote_req', shardId: 0, term: 1, candidateId: 'remote-1', priority: 0.5,
        }));

        const ackCall = onSendToPeer.mock.calls.find(c => {
            const msg = JSON.parse(c[1]);
            return msg.type === 'vote_ack';
        });
        expect(ackCall).toBeTruthy();
        engine.stop();
    });
});

// ═══════════════════════════════════════════════════
//  双向对时（Bidirectional Time Sync via ping/pong）
// ═══════════════════════════════════════════════════

describe('SyncEngine bidirectional time sync', () => {
    it('ping with seq/t1 triggers pong with t2/t3', () => {
        const onSendToPeer = vi.fn();
        const { engine } = createEngine({ onSendToPeer });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));
        onSendToPeer.mockClear();

        engine.receiveMessage('p1', JSON.stringify({ type: 'ping', seq: 0, t1: 1000 }));

        const pongCalls = onSendToPeer.mock.calls.filter(c => JSON.parse(c[1]).type === 'pong');
        expect(pongCalls.length).toBe(1);
        const pong = JSON.parse(pongCalls[0][1]);
        expect(pong.seq).toBe(0);
        expect(pong.t1).toBe(1000);
        expect(typeof pong.t2).toBe('number');
        expect(typeof pong.t3).toBe('number');
        expect(pong.t3).toBeGreaterThanOrEqual(pong.t2);
    });

    it('plain ping (no seq) triggers plain pong (no timestamps)', () => {
        const onSendToPeer = vi.fn();
        const { engine } = createEngine({ onSendToPeer });
        engine.peerConnected('p1');
        onSendToPeer.mockClear();

        engine.receiveMessage('p1', JSON.stringify({ type: 'ping' }));

        const pongCalls = onSendToPeer.mock.calls.filter(c => JSON.parse(c[1]).type === 'pong');
        expect(pongCalls.length).toBe(1);
        const pong = JSON.parse(pongCalls[0][1]);
        expect(pong.seq).toBeUndefined();
    });

    it('getClockSync returns null before any measurement', () => {
        const { engine } = createEngine();
        engine.peerConnected('p1');
        expect(engine.getClockSync('p1')).toBeNull();
    });

    it('getClockSync returns null for unknown peerId', () => {
        const { engine } = createEngine();
        expect(engine.getClockSync('unknown')).toBeNull();
    });

    it('computes RTT and offset after a full ping-pong cycle', () => {
        let clockMs = 1000;
        const getNow = () => ({ value: clockMs, unit: 'ms' });
        const { engine } = createEngine({ getNow });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        // Inject pending request as if _tickHeartbeat had sent a ping at t1=1000
        const state = engine._timeSyncStates.get('p1');
        state.pendingSeqs.set(0, 1000);

        // t4=1012, peer replied with t2=1010, t3=1011
        // rtt = (1012-1000) - (1011-1010) = 12 - 1 = 11
        // offset = ((1010-1000) + (1011-1012)) / 2 = (10 - 1) / 2 = 4.5
        clockMs = 1012;
        engine.receiveMessage('p1', JSON.stringify({
            type: 'pong', seq: 0, t1: 1000, t2: 1010, t3: 1011,
        }));

        const sync = engine.getClockSync('p1');
        expect(sync).not.toBeNull();
        expect(sync.rtt.value).toBeCloseTo(11);
        expect(sync.rtt.unit).toBe('ms');
        expect(sync.offset.value).toBeCloseTo(4.5);
        expect(sync.epsilon.value).toBeCloseTo(5.5);
        expect(sync.samples).toBe(1);
        expect(sync.lastSyncAt).toBe(1012);
    });

    it('getClockSync converts to requested unit', () => {
        let clockMs = 1000;
        const getNow = () => ({ value: clockMs, unit: 'ms' });
        const { engine } = createEngine({ getNow });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        engine._timeSyncStates.get('p1').pendingSeqs.set(99, 1000);
        clockMs = 1020;
        engine.receiveMessage('p1', JSON.stringify({
            type: 'pong', seq: 99, t1: 1000, t2: 1008, t3: 1009,
        }));

        const syncMs = engine.getClockSync('p1', 'ms');
        const syncUs = engine.getClockSync('p1', 'us');
        const syncS  = engine.getClockSync('p1', 's');

        expect(syncUs.rtt.value).toBeCloseTo(syncMs.rtt.value * 1000);
        expect(syncUs.rtt.unit).toBe('us');
        expect(syncS.rtt.value).toBeCloseTo(syncMs.rtt.value / 1000);
        expect(syncS.rtt.unit).toBe('s');
    });

    it('getClockSync returns all peers when called without peerId', () => {
        let clockMs = 1000;
        const getNow = () => ({ value: clockMs, unit: 'ms' });
        const { engine } = createEngine({ getNow });
        engine.peerConnected('p1');
        engine.peerConnected('p2');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));
        engine.receiveMessage('p2', JSON.stringify({ type: 'handshake', nodeId: 'remote-2', lastSeq: 0 }));

        engine._timeSyncStates.get('p1').pendingSeqs.set(1, 1000);
        clockMs = 1020;
        engine.receiveMessage('p1', JSON.stringify({ type: 'pong', seq: 1, t1: 1000, t2: 1008, t3: 1009 }));

        const allSync = engine.getClockSync();
        expect(allSync instanceof Map).toBe(true);
        expect(allSync.has('p1')).toBe(true);
        expect(allSync.has('p2')).toBe(false); // p2 has no samples yet
    });

    it('selects best offset from minimum RTT sample in sliding window', () => {
        let clockMs = 0;
        const getNow = () => ({ value: clockMs, unit: 'ms' });
        const { engine } = createEngine({ getNow });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        const state = engine._timeSyncStates.get('p1');

        // Sample 1: rtt=(20-0)-(6-5)=19, offset=((5-0)+(6-20))/2=-4.5
        state.pendingSeqs.set(1, 0);
        clockMs = 20;
        engine.receiveMessage('p1', JSON.stringify({ type: 'pong', seq: 1, t1: 0, t2: 5, t3: 6 }));

        // Sample 2: rtt=(108-100)-(105-104)=7, offset=((104-100)+(105-108))/2=0.5
        state.pendingSeqs.set(2, 100);
        clockMs = 108;
        engine.receiveMessage('p1', JSON.stringify({ type: 'pong', seq: 2, t1: 100, t2: 104, t3: 105 }));

        const sync = engine.getClockSync('p1');
        expect(sync.rtt.value).toBeCloseTo(7);
        expect(sync.offset.value).toBeCloseTo(0.5);
        expect(sync.epsilon.value).toBeCloseTo(3.5);
        expect(sync.samples).toBe(2);
    });

    it('sliding window evicts oldest samples when full', () => {
        let clockMs = 0;
        const getNow = () => ({ value: clockMs, unit: 'ms' });
        const { engine } = createEngine({ getNow, timeSyncWindowSize: 3 });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        const state = engine._timeSyncStates.get('p1');
        for (let i = 0; i < 4; i++) {
            const t1 = i * 100;
            state.pendingSeqs.set(i, t1);
            clockMs = t1 + 20;
            engine.receiveMessage('p1', JSON.stringify({
                type: 'pong', seq: i, t1, t2: t1 + 8, t3: t1 + 9,
            }));
        }

        expect(state.rtts.length).toBe(3);
        expect(state.offsets.length).toBe(3);
    });

    it('discards pong with unknown seq', () => {
        let clockMs = 1000;
        const getNow = () => ({ value: clockMs, unit: 'ms' });
        const { engine } = createEngine({ getNow });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        engine.receiveMessage('p1', JSON.stringify({
            type: 'pong', seq: 999, t1: 1000, t2: 1005, t3: 1006,
        }));

        expect(engine.getClockSync('p1')).toBeNull();
    });

    it('discards pong with negative RTT', () => {
        let clockMs = 1000;
        const getNow = () => ({ value: clockMs, unit: 'ms' });
        const { engine } = createEngine({ getNow });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        engine._timeSyncStates.get('p1').pendingSeqs.set(0, 1000);
        // rtt = (1010-1000) - (100-1) = 10-99 = -89 → discard
        clockMs = 1010;
        engine.receiveMessage('p1', JSON.stringify({
            type: 'pong', seq: 0, t1: 1000, t2: 1, t3: 100,
        }));

        expect(engine.getClockSync('p1')).toBeNull();
    });

    it('_tickHeartbeat sends ping with seq/t1 to all peers', () => {
        const onSendToPeer = vi.fn();
        const { engine } = createEngine({ onSendToPeer });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));
        onSendToPeer.mockClear();

        engine._tickHeartbeat();

        const pingCalls = onSendToPeer.mock.calls.filter(c => JSON.parse(c[1]).type === 'ping');
        expect(pingCalls.length).toBe(1);
        const req = JSON.parse(pingCalls[0][1]);
        expect(typeof req.seq).toBe('number');
        expect(typeof req.t1).toBe('number');
    });

    it('cleans up time sync state on peerDisconnected', () => {
        const { engine } = createEngine();
        engine.peerConnected('p1');
        expect(engine._timeSyncStates.has('p1')).toBe(true);
        engine.peerDisconnected('p1');
        expect(engine._timeSyncStates.has('p1')).toBe(false);
    });

    it('_updateClockOffset smoothly converges _clockOffsetMs toward best-epsilon offset', () => {
        let clockMs = 1000;
        const getNow = () => ({ value: clockMs, unit: 'ms' });
        const { engine } = createEngine({ getNow });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        // Initially zero
        expect(engine._clockOffsetMs).toBe(0);

        // Inject pong: rtt=10ms, offset=20ms
        engine._timeSyncStates.get('p1').pendingSeqs.set(0, 1000);
        clockMs = 1012; // t4=1012, peer: t2=1008, t3=1009
        // rtt=(1012-1000)-(1009-1008)=11, offset=((1008-1000)+(1009-1012))/2=2.5
        engine.receiveMessage('p1', JSON.stringify({
            type: 'pong', seq: 0, t1: 1000, t2: 1008, t3: 1009,
        }));

        // After one pong: _clockOffsetMs = 0*0.8 + 2.5*0.2 = 0.5
        expect(engine._clockOffsetMs).toBeCloseTo(0.5);
    });

    it('HLC tick uses corrected time (_clockOffsetMs applied)', () => {
        // Fix local time at exactly 1000ms
        const getNow = () => ({ value: 1000, unit: 'ms' });
        const { engine } = createEngine({ getNow });
        engine.peerConnected('p1');
        engine.receiveMessage('p1', JSON.stringify({ type: 'handshake', nodeId: 'remote-1', lastSeq: 0 }));

        // Manually set a known offset
        engine._clockOffsetMs = 50;

        const ts = engine.hlc.tick();
        // HLC wallTime should be at least 1050 (1000 + 50ms offset)
        const wallTime = parseInt(ts.split('.')[0], 10);
        expect(wallTime).toBeGreaterThanOrEqual(1050);
    });

    it('_getElectionPriority returns higher value for lower RTT', () => {
        const { engine: engineLowRtt }  = createEngine();
        const { engine: engineHighRtt } = createEngine();

        // Set up p1 on engineLowRtt with small RTT
        engineLowRtt.peerConnected('p1');
        engineLowRtt._timeSyncStates.get('p1').rtts = [2];
        engineLowRtt._timeSyncStates.get('p1').minRtt = 2;

        // Set up p1 on engineHighRtt with large RTT
        engineHighRtt.peerConnected('p1');
        engineHighRtt._timeSyncStates.get('p1').rtts = [300];
        engineHighRtt._timeSyncStates.get('p1').minRtt = 300;

        const lowPriority  = engineLowRtt._getElectionPriority();
        const highPriority = engineHighRtt._getElectionPriority();
        // Low RTT should yield higher priority
        expect(lowPriority).toBeGreaterThan(highPriority);
    });

    it('_getElectionPriority falls back to random when no RTT data', () => {
        const { engine } = createEngine();
        const p = engine._getElectionPriority();
        expect(p).toBeGreaterThanOrEqual(0);
        expect(p).toBeLessThan(1);
    });

    it('_getBestNetworkRttMs returns null with no data, min RTT with data', () => {
        const { engine } = createEngine();
        expect(engine._getBestNetworkRttMs()).toBeNull();

        engine.peerConnected('p1');
        engine.peerConnected('p2');
        engine._timeSyncStates.get('p1').rtts = [10];
        engine._timeSyncStates.get('p1').minRtt = 10;
        engine._timeSyncStates.get('p2').rtts = [5];
        engine._timeSyncStates.get('p2').minRtt = 5;

        expect(engine._getBestNetworkRttMs()).toBe(5);
    });

    it('_getBestEpsilonMs returns Infinity with no data, max epsilon with data', () => {
        const { engine } = createEngine();
        expect(engine._getBestEpsilonMs()).toBe(Infinity);

        engine.peerConnected('p1');
        engine.peerConnected('p2');
        engine._timeSyncStates.get('p1').rtts = [10]; engine._timeSyncStates.get('p1').epsilon = 5;
        engine._timeSyncStates.get('p2').rtts = [20]; engine._timeSyncStates.get('p2').epsilon = 10;

        expect(engine._getBestEpsilonMs()).toBe(10); // max epsilon
    });

    it('isLeaseValid returns false before start (no sync data)', () => {
        const { engine } = createEngine();
        expect(engine.isLeaseValid()).toBe(false);
        expect(engine.isLeaseValidForKey('some-key')).toBe(false);
    });

    it('start() uses exactly 3 setInterval calls (heartbeat + pull + cleanup)', () => {
        const setIntervalCalls = [];
        const timerAPI = {
            setTimeout: vi.fn(), clearTimeout: vi.fn(),
            setInterval: vi.fn((fn, ms) => { setIntervalCalls.push(ms); return setIntervalCalls.length; }),
            clearInterval: vi.fn(),
        };
        const { engine } = createEngine({ timerAPI });
        engine.start();
        expect(setIntervalCalls.length).toBe(3);
        engine.stop();
    });
});

// ═══════════════════════════════════════════════════
//  Missing peer watermark: max-seq comparison (no replay from 0)
// ═══════════════════════════════════════════════════

describe('SyncEngine missing peer watermark', () => {
    function seedLog(db, count) {
        const t = db._tables._sync_log || (db._tables._sync_log = []);
        for (let i = 1; i <= count; i++) {
            t.push({ id: i, table_name: 'users', operation: 'INSERT', row_key: JSON.stringify({ user_id: 'u' + i }), row_data: JSON.stringify({ user_id: 'u' + i }), _hlc: 'hlc-' + i });
        }
    }
    function sentMessages(onSendToPeer) {
        return onSendToPeer.mock.calls.map(c => JSON.parse(c[1]));
    }
    function connectAndHandshake(engine, { direction = 'outbound', lastSeq } = {}) {
        engine.peerConnected('p1', { direction });
        const hs = { type: 'handshake', nodeId: 'remote-1' };
        if (lastSeq !== undefined) hs.lastSeq = lastSeq;
        engine.receiveMessage('p1', JSON.stringify(hs));
    }

    it('getPeerWatermark distinguishes a missing row from a zero watermark', () => {
        const db = createInMemoryDb();
        expect(getPeerWatermark(db, 'nobody')).toBe(null);
        db._tables._sync_peers = [{ peer_key: 'zero', last_sync_id: 0, last_sync_at: null }];
        expect(getPeerWatermark(db, 'zero')).toBe(0);
    });

    it('pull with an existing watermark sends pull_req from that watermark', () => {
        const onSendToPeer = vi.fn();
        const { engine, db } = createEngine({ onSendToPeer });
        db._tables._sync_peers = [{ peer_key: 'remote-1', last_sync_id: 7, last_sync_at: null }];
        connectAndHandshake(engine, { lastSeq: 0 });
        onSendToPeer.mockClear();
        engine._tickPull();
        const pulls = sentMessages(onSendToPeer).filter(m => m.type === 'pull_req');
        expect(pulls).toHaveLength(1);
        expect(pulls[0].sinceSeq).toBe(7);
    });

    it('pull without a watermark does not replay from 0 when the peer is not ahead', () => {
        const onSendToPeer = vi.fn();
        const { engine, db } = createEngine({ onSendToPeer });
        seedLog(db, 5);
        connectAndHandshake(engine, { lastSeq: 5 });
        onSendToPeer.mockClear();
        engine._tickPull();
        expect(sentMessages(onSendToPeer).filter(m => m.type === 'pull_req')).toHaveLength(0);
    });

    it('pull without a watermark registers full-sync needs when the peer is ahead (outbound)', () => {
        const onSendToPeer = vi.fn();
        const { engine, db } = createEngine({ onSendToPeer });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        seedLog(db, 5);
        connectAndHandshake(engine, { lastSeq: 9 });
        // 握手本身已登记过一次 need；清掉后验证 tick 路径能独立登记
        db._tables._full_sync_needs.length = 0;
        onSendToPeer.mockClear();
        engine._tickPull();
        expect(sentMessages(onSendToPeer).filter(m => m.type === 'pull_req')).toHaveLength(0);
        const needs = db._tables._full_sync_needs;
        expect(needs.some(n => n.phase === 'rows' && n.table_name === 'users' && n.watermark_seq === 9)).toBe(true);
        expect(needs.some(n => n.phase === 'tombstones' && n.watermark_seq === 9)).toBe(true);
    });

    it('pull without a watermark does nothing for inbound peers', () => {
        const onSendToPeer = vi.fn();
        const { engine, db } = createEngine({ onSendToPeer });
        engine.registerTable('users', { keyColumns: ['user_id'], dataColumns: ['user_id', 'name', '_hlc'] });
        seedLog(db, 5);
        connectAndHandshake(engine, { direction: 'inbound', lastSeq: 9 });
        db._tables._full_sync_needs.length = 0;
        onSendToPeer.mockClear();
        engine._tickPull();
        expect(sentMessages(onSendToPeer).filter(m => m.type === 'pull_req')).toHaveLength(0);
        expect(db._tables._full_sync_needs).toHaveLength(0);
    });

    it('push without a watermark starts from the peer-reported max seq, not 0', () => {
        const onSendToPeer = vi.fn();
        const { engine, db } = createEngine({ onSendToPeer });
        seedLog(db, 6);
        connectAndHandshake(engine, { lastSeq: 4 });
        // 握手推送已建水位行；删掉它，模拟水位行被清理后的状态
        db._tables._sync_peers.length = 0;
        onSendToPeer.mockClear();
        engine._pushToAllPeers();
        const syncs = sentMessages(onSendToPeer).filter(m => m.type === 'sync');
        expect(syncs).toHaveLength(1);
        expect(syncs[0].entries.map(e => e.seq)).toEqual([5, 6]);
        const row = db._tables._sync_peers.find(r => r.peer_key === 'remote-1');
        expect(row?.last_sync_id).toBe(6);
    });

    it('push without a watermark and without a reported max seq sends nothing', () => {
        const onSendToPeer = vi.fn();
        const { engine, db } = createEngine({ onSendToPeer });
        seedLog(db, 6);
        connectAndHandshake(engine, {}); // handshake 不带 lastSeq
        onSendToPeer.mockClear();
        engine._pushToAllPeers();
        expect(sentMessages(onSendToPeer).filter(m => m.type === 'sync')).toHaveLength(0);
    });
});
