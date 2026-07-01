import { describe, it, expect, vi } from 'vitest';
import {
    makeHandshake, makeSync, makeAck, makePullRequest,
    makePing, makePong, parseMessage,
    makeLeaderHeartbeat, makeVoteReq, makeVoteAck,
    makeProxyReq, makeProxyRes,
    makePrepare, makePrepareAck, makeCommit, makeAbort,
    applyEntries, applyEntriesInTransaction,
    readSyncLog, getMaxSeq, cleanSyncLog,
    getPeerLastSyncId, updatePeerSyncId,
    cleanTombstones, logChange,
} from './sync-protocol.js';
import { HLC } from './hlc.js';

// ═══════════════════════════════════════════════════
//  Helper: in-memory mock DB (SQLite dialect)
// ═══════════════════════════════════════════════════

function createMockDb() {
    const tables = {};
    const getTable = (name) => {
        if (!tables[name]) tables[name] = [];
        return tables[name];
    };

    return {
        _tables: tables,

        run(sql, params = []) {
            if (sql.startsWith('INSERT') && sql.includes('_sync_log')) {
                const t = getTable('_sync_log');
                const id = t.length + 1;
                t.push({
                    id, table_name: params[0], operation: params[1],
                    row_key: params[2], row_data: params[3], _hlc: params[4],
                });
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
            if (sql.startsWith('INSERT OR REPLACE') && !sql.includes('_sync') && !sql.includes('_tombstones')) {
                // Generic table upsert
                const match = sql.match(/INTO (\w+)/);
                const tableName = match ? match[1] : 'unknown';
                const t = getTable(tableName);
                // Simple: just push
                t.push(Object.fromEntries(params.map((v, i) => [`col_${i}`, v])));
                return { changes: 1 };
            }
            if (sql.startsWith('DELETE')) {
                if (sql.includes('_tombstones')) {
                    const t = getTable('_tombstones');
                    const idx = t.findIndex(r => r.table_name === params[0] && r.row_key === params[1]);
                    if (idx >= 0) t.splice(idx, 1);
                    return { changes: idx >= 0 ? 1 : 0 };
                }
                if (sql.includes('_sync_log')) {
                    const t = getTable('_sync_log');
                    const limit = params[0];
                    t.splice(0, limit);
                    return { changes: limit };
                }
                // Generic DELETE
                const match = sql.match(/FROM (\w+)/);
                const tableName = match ? match[1] : 'unknown';
                const t = getTable(tableName);
                // Simple: remove matching entry
                return { changes: 0 };
            }
            return { changes: 0 };
        },

        get(sql, params = []) {
            if (sql.includes('MAX(id)') && sql.includes('_sync_log')) {
                const t = getTable('_sync_log');
                const max = t.length > 0 ? Math.max(...t.map(r => r.id)) : 0;
                return { max_seq: max };
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
            if (sql.includes('_hlc FROM')) {
                // Local row HLC query
                return null; // no local row by default
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
            if (sql.includes('_tombstones')) {
                return getTable('_tombstones');
            }
            return [];
        },

        exec(sql) {},

        upsertSQL(table, columns, keyColumns) {
            const cols = columns.join(', ');
            const ph = columns.map(() => '?').join(', ');
            return `INSERT OR REPLACE INTO ${table} (${cols}) VALUES (${ph})`;
        },

        beginTransaction() {},
        commit() {},
        rollback() {},
    };
}

function makeGetNow(startMs = 1713000000000) {
    let t = startMs;
    return () => ({ value: t++, unit: 'ms' });
}

// ═══════════════════════════════════════════════════
//  Message construction & parsing
// ═══════════════════════════════════════════════════

describe('Message construction', () => {
    it('makeHandshake', () => {
        const raw = makeHandshake('n1', 42);
        const msg = JSON.parse(raw);
        expect(msg).toEqual({ type: 'handshake', nodeId: 'n1', lastSeq: 42 });
    });

    it('makeSync', () => {
        const entries = [{ seq: 1, table_name: 't1' }];
        const msg = JSON.parse(makeSync(entries));
        expect(msg.type).toBe('sync');
        expect(msg.entries).toEqual(entries);
    });

    it('makeAck', () => {
        const msg = JSON.parse(makeAck(99));
        expect(msg).toEqual({ type: 'ack', lastSeq: 99 });
    });

    it('makePullRequest', () => {
        const msg = JSON.parse(makePullRequest(10));
        expect(msg).toEqual({ type: 'pull_req', sinceSeq: 10 });
    });

    it('makePing / makePong', () => {
        expect(JSON.parse(makePing())).toEqual({ type: 'ping' });
        expect(JSON.parse(makePong())).toEqual({ type: 'pong' });
    });

    it('makeLeaderHeartbeat', () => {
        const msg = JSON.parse(makeLeaderHeartbeat(0, 5, 'leader-1'));
        expect(msg).toEqual({ type: 'leader_heartbeat', shardId: 0, term: 5, leaderId: 'leader-1' });
    });

    it('makeVoteReq', () => {
        const msg = JSON.parse(makeVoteReq(0, 3, 'cand-1', 0.7));
        expect(msg).toEqual({ type: 'vote_req', shardId: 0, term: 3, candidateId: 'cand-1', priority: 0.7 });
    });

    it('makeVoteAck', () => {
        const msg = JSON.parse(makeVoteAck(0, 3, 'cand-1', true, 'voter-1'));
        expect(msg).toEqual({ type: 'vote_ack', shardId: 0, term: 3, candidateId: 'cand-1', granted: true, voterId: 'voter-1' });
    });

    it('makeProxyReq / makeProxyRes', () => {
        const req = JSON.parse(makeProxyReq('r1', { action: 'write' }));
        expect(req).toEqual({ type: 'proxy_req', requestId: 'r1', payload: { action: 'write' } });
        const res = JSON.parse(makeProxyRes('r1', { ok: true }));
        expect(res).toEqual({ type: 'proxy_res', requestId: 'r1', payload: { ok: true } });
    });

    it('makePrepare', () => {
        const msg = JSON.parse(makePrepare('w1', [{ seq: 1 }], 2, 0));
        expect(msg).toEqual({ type: 'prepare', writeId: 'w1', entries: [{ seq: 1 }], term: 2, shardId: 0 });
    });

    it('makePrepareAck', () => {
        expect(JSON.parse(makePrepareAck('w1', true, null))).toEqual({
            type: 'prepare_ack', writeId: 'w1', granted: true, reason: null,
        });
        expect(JSON.parse(makePrepareAck('w1', false, 'busy'))).toEqual({
            type: 'prepare_ack', writeId: 'w1', granted: false, reason: 'busy',
        });
    });

    it('makeCommit / makeAbort', () => {
        expect(JSON.parse(makeCommit('w1'))).toEqual({ type: 'commit', writeId: 'w1' });
        expect(JSON.parse(makeAbort('w1', 'timeout'))).toEqual({ type: 'abort', writeId: 'w1', reason: 'timeout' });
    });
});

describe('parseMessage', () => {
    it('parses valid JSON', () => {
        expect(parseMessage('{"type":"ping"}')).toEqual({ type: 'ping' });
    });

    it('returns null on invalid JSON', () => {
        expect(parseMessage('not json')).toBeNull();
        expect(parseMessage('')).toBeNull();
    });
});

// ═══════════════════════════════════════════════════
//  readSyncLog / getMaxSeq / cleanSyncLog
// ═══════════════════════════════════════════════════

describe('Sync log operations', () => {
    it('readSyncLog returns entries after sinceSeq', () => {
        const db = createMockDb();
        logChange(db, 'users', 'INSERT', '{"id":"u1"}', '{"id":"u1","name":"A"}', 'hlc1');
        logChange(db, 'users', 'UPDATE', '{"id":"u1"}', '{"id":"u1","name":"B"}', 'hlc2');

        const entries = readSyncLog(db, 0, 100);
        expect(entries.length).toBe(2);
        expect(entries[0].seq).toBe(1);
        expect(entries[1].seq).toBe(2);
    });

    it('readSyncLog respects sinceSeq', () => {
        const db = createMockDb();
        logChange(db, 'users', 'INSERT', '{"id":"u1"}', '{}', 'hlc1');
        logChange(db, 'users', 'INSERT', '{"id":"u2"}', '{}', 'hlc2');

        const entries = readSyncLog(db, 1, 100);
        expect(entries.length).toBe(1);
        expect(entries[0].seq).toBe(2);
    });

    it('readSyncLog respects limit', () => {
        const db = createMockDb();
        for (let i = 0; i < 10; i++) {
            logChange(db, 'users', 'INSERT', `{"id":"u${i}"}`, '{}', `hlc${i}`);
        }
        const entries = readSyncLog(db, 0, 3);
        expect(entries.length).toBe(3);
    });

    it('getMaxSeq returns 0 for empty log', () => {
        expect(getMaxSeq(createMockDb())).toBe(0);
    });

    it('getMaxSeq returns max id', () => {
        const db = createMockDb();
        logChange(db, 't', 'INSERT', '{}', '{}', 'h');
        logChange(db, 't', 'INSERT', '{}', '{}', 'h');
        expect(getMaxSeq(db)).toBe(2);
    });

    it('cleanSyncLog removes oldest entries when exceeding max', () => {
        const db = createMockDb();
        for (let i = 0; i < 5; i++) {
            logChange(db, 't', 'INSERT', '{}', '{}', 'h');
        }
        cleanSyncLog(db, 3);
        // After cleanup, should have trimmed oldest 2
        expect(db._tables._sync_log.length).toBe(3);
    });
});

// ═══════════════════════════════════════════════════
//  Peer sync ID
// ═══════════════════════════════════════════════════

describe('Peer sync operations', () => {
    it('getPeerLastSyncId returns 0 for unknown peer', () => {
        expect(getPeerLastSyncId(createMockDb(), 'unknown')).toBe(0);
    });

    it('updatePeerSyncId stores and getPeerLastSyncId retrieves', () => {
        const db = createMockDb();
        const getNow = () => ({ value: 1713000000000, unit: 'ms' });
        updatePeerSyncId(db, 'peer-1', 42, getNow);
        expect(getPeerLastSyncId(db, 'peer-1')).toBe(42);
    });

    it('updatePeerSyncId updates existing entry', () => {
        const db = createMockDb();
        const getNow = () => ({ value: 1713000000000, unit: 'ms' });
        updatePeerSyncId(db, 'peer-1', 10, getNow);
        updatePeerSyncId(db, 'peer-1', 20, getNow);
        expect(getPeerLastSyncId(db, 'peer-1')).toBe(20);
    });

    it('updatePeerSyncId stores ISO timestamp from getNow', () => {
        const db = createMockDb();
        const getNow = () => ({ value: 1713000000000, unit: 'ms' });
        updatePeerSyncId(db, 'peer-1', 1, getNow);
        const row = db._tables._sync_peers[0];
        expect(row.last_sync_at).toMatch(/^\d{4}-\d{2}-\d{2}T/); // ISO format
    });

    it('updatePeerSyncId works with seconds unit', () => {
        const db = createMockDb();
        const getNow = () => ({ value: 1713000000, unit: 's' });
        updatePeerSyncId(db, 'peer-1', 1, getNow);
        const row = db._tables._sync_peers[0];
        expect(row.last_sync_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    });
});

// ═══════════════════════════════════════════════════
//  cleanTombstones
// ═══════════════════════════════════════════════════

describe('cleanTombstones', () => {
    it('throws when getNow is not a function', () => {
        expect(() => cleanTombstones(createMockDb(), null, 7)).toThrow('getNow is required');
        expect(() => cleanTombstones(createMockDb())).toThrow('getNow is required');
    });

    it('removes tombstones older than retention period', () => {
        const db = createMockDb();
        const nowMs = 1713000000000;
        const oldHlc = `${(nowMs - 8 * 24 * 60 * 60 * 1000).toString().padStart(15, '0')}.00000.n1`;
        const newHlc = `${nowMs.toString().padStart(15, '0')}.00000.n1`;

        db._tables._tombstones = [
            { table_name: 'users', row_key: '{"id":"old"}', _hlc: oldHlc },
            { table_name: 'users', row_key: '{"id":"new"}', _hlc: newHlc },
        ];

        const cleaned = cleanTombstones(db, () => ({ value: nowMs, unit: 'ms' }), 7);
        expect(cleaned).toBe(1);
        expect(db._tables._tombstones.length).toBe(1);
        expect(db._tables._tombstones[0].row_key).toBe('{"id":"new"}');
    });

    it('cleans nothing when all tombstones are recent', () => {
        const db = createMockDb();
        const nowMs = 1713000000000;
        const recentHlc = `${nowMs.toString().padStart(15, '0')}.00000.n1`;

        db._tables._tombstones = [
            { table_name: 't', row_key: '{"id":"1"}', _hlc: recentHlc },
        ];

        expect(cleanTombstones(db, () => ({ value: nowMs, unit: 'ms' }), 7)).toBe(0);
    });

    it('works with seconds unit', () => {
        const db = createMockDb();
        const nowMs = 1713000000000;
        const nowSec = nowMs / 1000;
        const oldHlc = `${(nowMs - 8 * 24 * 60 * 60 * 1000).toString().padStart(15, '0')}.00000.n1`;

        db._tables._tombstones = [
            { table_name: 't', row_key: '{"id":"1"}', _hlc: oldHlc },
        ];

        const cleaned = cleanTombstones(db, () => ({ value: nowSec, unit: 's' }), 7);
        expect(cleaned).toBe(1);
    });
});

// ═══════════════════════════════════════════════════
//  logChange
// ═══════════════════════════════════════════════════

describe('logChange', () => {
    it('inserts into _sync_log', () => {
        const db = createMockDb();
        logChange(db, 'users', 'INSERT', '{"user_id":"u1"}', '{"user_id":"u1","name":"A"}', 'hlc-ts');
        const entries = readSyncLog(db, 0, 10);
        expect(entries.length).toBe(1);
        expect(entries[0].table_name).toBe('users');
        expect(entries[0].operation).toBe('INSERT');
        expect(entries[0].row_key).toBe('{"user_id":"u1"}');
        expect(entries[0]._hlc).toBe('hlc-ts');
    });
});

// ═══════════════════════════════════════════════════
//  applyEntries (幂等数据应用)
// ═══════════════════════════════════════════════════

describe('applyEntries', () => {
    const tableRegistry = new Map([
        ['users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', 'name', '_hlc'],
            blobColumns: [],
            validator: null,
            deserializer: null,
        }],
    ]);

    function makeHlcTs(wallTime, counter, nodeId) {
        return `${wallTime.toString().padStart(15, '0')}.${counter.toString().padStart(5, '0')}.${nodeId}`;
    }

    it('applies INSERT entries', () => {
        const db = createMockDb();
        const hlc = new HLC('local', makeGetNow());
        const entries = [{
            table_name: 'users',
            operation: 'INSERT',
            row_key: '{"user_id":"u1"}',
            row_data: '{"user_id":"u1","name":"Alice","_hlc":"' + makeHlcTs(2000, 0, 'remote') + '"}',
            _hlc: makeHlcTs(2000, 0, 'remote'),
        }];

        const result = applyEntries(db, entries, hlc, tableRegistry);
        expect(result.applied).toBe(1);
        expect(result.skipped).toBe(0);
    });

    it('skips entries for unregistered tables', () => {
        const db = createMockDb();
        const hlc = new HLC('local', makeGetNow());
        const entries = [{
            table_name: 'unknown_table',
            operation: 'INSERT',
            row_key: '{"id":"1"}',
            row_data: '{"id":"1"}',
            _hlc: makeHlcTs(2000, 0, 'remote'),
        }];

        const result = applyEntries(db, entries, hlc, tableRegistry);
        expect(result.skipped).toBe(1);
    });

    it('skips entries without _hlc', () => {
        const db = createMockDb();
        const hlc = new HLC('local', makeGetNow());
        const entries = [{
            table_name: 'users',
            operation: 'INSERT',
            row_key: '{"user_id":"u1"}',
            row_data: '{"user_id":"u1"}',
            _hlc: null,
        }];

        const result = applyEntries(db, entries, hlc, tableRegistry);
        expect(result.skipped).toBe(1);
    });

    it('applies multiple entries', () => {
        const db = createMockDb();
        const hlc = new HLC('local', makeGetNow());
        const entries = [
            {
                table_name: 'users', operation: 'INSERT',
                row_key: '{"user_id":"u1"}',
                row_data: '{"user_id":"u1","name":"A","_hlc":"' + makeHlcTs(2000, 0, 'r') + '"}',
                _hlc: makeHlcTs(2000, 0, 'r'),
            },
            {
                table_name: 'users', operation: 'INSERT',
                row_key: '{"user_id":"u2"}',
                row_data: '{"user_id":"u2","name":"B","_hlc":"' + makeHlcTs(2001, 0, 'r') + '"}',
                _hlc: makeHlcTs(2001, 0, 'r'),
            },
        ];

        const result = applyEntries(db, entries, hlc, tableRegistry);
        expect(result.applied).toBe(2);
    });

    it('applies DELETE entries', () => {
        const db = createMockDb();
        const hlc = new HLC('local', makeGetNow());
        const entries = [{
            table_name: 'users',
            operation: 'DELETE',
            row_key: '{"user_id":"u1"}',
            row_data: null,
            _hlc: makeHlcTs(2000, 0, 'remote'),
        }];

        const result = applyEntries(db, entries, hlc, tableRegistry);
        expect(result.applied).toBe(1);
    });

    it('skips entries with unknown operation', () => {
        const db = createMockDb();
        const hlc = new HLC('local', makeGetNow());
        const entries = [{
            table_name: 'users',
            operation: 'TRUNCATE',
            row_key: '{"user_id":"u1"}',
            row_data: null,
            _hlc: makeHlcTs(2000, 0, 'remote'),
        }];

        const result = applyEntries(db, entries, hlc, tableRegistry);
        expect(result.skipped).toBe(1);
    });

    it('advances local HLC on receive', () => {
        const db = createMockDb();
        const hlc = new HLC('local', makeGetNow());
        const remoteTs = makeHlcTs(9999999999999, 0, 'remote');
        const entries = [{
            table_name: 'users', operation: 'INSERT',
            row_key: '{"user_id":"u1"}',
            row_data: '{"user_id":"u1","name":"A","_hlc":"' + remoteTs + '"}',
            _hlc: remoteTs,
        }];

        applyEntries(db, entries, hlc, tableRegistry);
        expect(hlc.wallTime).toBeGreaterThanOrEqual(9999999999999);
    });
});

describe('applyEntriesInTransaction', () => {
    it('applies without wrapping in transaction', () => {
        const db = createMockDb();
        const beginSpy = vi.spyOn(db, 'beginTransaction');
        const commitSpy = vi.spyOn(db, 'commit');
        const hlc = new HLC('local', makeGetNow());
        const tableRegistry = new Map([
            ['t', { keyColumns: ['id'], dataColumns: ['id', '_hlc'], blobColumns: [], validator: null, deserializer: null }],
        ]);

        const entries = [{
            table_name: 't', operation: 'INSERT',
            row_key: '{"id":"1"}',
            row_data: '{"id":"1","_hlc":"000000000002000.00000.r"}',
            _hlc: '000000000002000.00000.r',
        }];

        const result = applyEntriesInTransaction(db, entries, hlc, tableRegistry);
        expect(result.applied).toBe(1);
        expect(beginSpy).not.toHaveBeenCalled();
        expect(commitSpy).not.toHaveBeenCalled();
    });
});
