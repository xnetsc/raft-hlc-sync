import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { LeaderElection } from './leader-election.js';

// ═══════════════════════════════════════════════════
//  Helper: fake timers via vitest
// ═══════════════════════════════════════════════════

function makeTimerAPI() {
    return {
        setTimeout:    (fn, ms) => setTimeout(fn, ms),
        clearTimeout:  (id) => clearTimeout(id),
        setInterval:   (fn, ms) => setInterval(fn, ms),
        clearInterval: (id) => clearInterval(id),
    };
}

function makeElection(overrides = {}) {
    const callbacks = {
        onSendHeartbeat: vi.fn(),
        onSendVoteReq:   vi.fn(),
        onSendVoteAck:   vi.fn(),
        onLeaderChanged: vi.fn(),
    };
    const opts = {
        shardId: 0,
        nodeId: 'node-0',
        getMinQuorum: () => 1,
        timerAPI: makeTimerAPI(),
        _testOptions: {
            heartbeatIntervalMs: 100,
            minElectionMs: 50,
            maxElectionMs: 100,
        },
        ...callbacks,
        ...overrides,
    };
    const el = new LeaderElection(opts);
    return { el, callbacks };
}

// ═══════════════════════════════════════════════════
//  Constructor
// ═══════════════════════════════════════════════════

describe('LeaderElection constructor', () => {
    it('throws when timerAPI is missing', () => {
        expect(() => new LeaderElection({
            shardId: 0, nodeId: 'n0', getMinQuorum: () => 1,
            onSendHeartbeat: vi.fn(), onSendVoteReq: vi.fn(),
            onSendVoteAck: vi.fn(), onLeaderChanged: vi.fn(),
        })).toThrow('timerAPI is required');
    });

    it('throws when timerAPI is incomplete', () => {
        expect(() => new LeaderElection({
            shardId: 0, nodeId: 'n0', getMinQuorum: () => 1,
            onSendHeartbeat: vi.fn(), onSendVoteReq: vi.fn(),
            onSendVoteAck: vi.fn(), onLeaderChanged: vi.fn(),
            timerAPI: { setTimeout: vi.fn() },
        })).toThrow('timerAPI is required');
    });

    it('initializes in follower state', () => {
        const { el } = makeElection();
        expect(el.isLeader()).toBe(false);
        expect(el.getLeaderNodeId()).toBeNull();
        expect(el.getStatus().state).toBe('follower');
        expect(el.getStatus().term).toBe(0);
    });
});

// ═══════════════════════════════════════════════════
//  Lifecycle
// ═══════════════════════════════════════════════════

describe('LeaderElection lifecycle', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('start is idempotent', () => {
        const { el } = makeElection();
        el.start();
        el.start(); // no error
        el.stop();
    });

    it('stop clears all timers', () => {
        const { el } = makeElection();
        el.start();
        el.stop();
        expect(el._started).toBe(false);
    });

    afterEach(() => { vi.useRealTimers(); });
});

// ═══════════════════════════════════════════════════
//  Single-node election (quorum=1)
// ═══════════════════════════════════════════════════

describe('Single-node election', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('elects self as leader after election timeout', async () => {
        const { el, callbacks } = makeElection({ getMinQuorum: () => 1 });
        el.start();
        await vi.advanceTimersByTimeAsync(200); // past max election timeout
        expect(el.isLeader()).toBe(true);
        expect(el.getLeaderNodeId()).toBe('node-0');
        expect(callbacks.onLeaderChanged).toHaveBeenCalledWith(0, 'node-0', true);
        el.stop();
    });

    it('sends heartbeat after becoming leader', async () => {
        const { el, callbacks } = makeElection();
        el.start();
        await vi.advanceTimersByTimeAsync(200);
        expect(callbacks.onSendHeartbeat).toHaveBeenCalledWith(0, expect.any(Number), 'node-0');
        el.stop();
    });

    it('sends periodic heartbeats', async () => {
        const { el, callbacks } = makeElection();
        el.start();
        await vi.advanceTimersByTimeAsync(200); // become leader
        callbacks.onSendHeartbeat.mockClear();
        await vi.advanceTimersByTimeAsync(300); // 3 heartbeat intervals (100ms each)
        expect(callbacks.onSendHeartbeat.mock.calls.length).toBeGreaterThanOrEqual(2);
        el.stop();
    });

    afterEach(() => { vi.useRealTimers(); });
});

// ═══════════════════════════════════════════════════
//  Node management
// ═══════════════════════════════════════════════════

describe('Node management', () => {
    it('addNode ignores self', () => {
        const { el } = makeElection();
        el.addNode('node-0'); // self
        expect(el._connectedNodeIds.size).toBe(0);
    });

    it('addNode adds remote node', () => {
        const { el } = makeElection();
        el.addNode('node-1');
        expect(el._connectedNodeIds.has('node-1')).toBe(true);
    });

    it('removeNode clears leader if removed node was leader', () => {
        const { el, callbacks } = makeElection();
        el._currentLeaderNodeId = 'node-1';
        el.removeNode('node-1');
        expect(el.getLeaderNodeId()).toBeNull();
        expect(callbacks.onLeaderChanged).toHaveBeenCalledWith(0, null, false);
    });

    it('removeNode does nothing for unknown node', () => {
        const { el, callbacks } = makeElection();
        el.removeNode('nonexistent');
        expect(callbacks.onLeaderChanged).not.toHaveBeenCalled();
    });
});

// ═══════════════════════════════════════════════════
//  Heartbeat reception
// ═══════════════════════════════════════════════════

describe('receiveHeartbeat', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('ignores heartbeats when not started', () => {
        const { el, callbacks } = makeElection();
        el.receiveHeartbeat(1, 'node-1');
        expect(callbacks.onLeaderChanged).not.toHaveBeenCalled();
    });

    it('ignores heartbeats with lower term', () => {
        const { el, callbacks } = makeElection();
        el.start();
        el._currentTerm = 5;
        el.addNode('node-1');
        el.receiveHeartbeat(3, 'node-1');
        expect(el.getLeaderNodeId()).toBeNull();
    });

    it('sets leader on valid heartbeat from connected node', () => {
        const { el, callbacks } = makeElection();
        el.start();
        el.addNode('node-1');
        el.receiveHeartbeat(1, 'node-1');
        expect(el.getLeaderNodeId()).toBe('node-1');
        expect(callbacks.onLeaderChanged).toHaveBeenCalledWith(0, 'node-1', false);
    });

    it('steps down when receiving heartbeat with higher term', async () => {
        const { el } = makeElection();
        el.start();
        await vi.advanceTimersByTimeAsync(200); // become leader
        expect(el.isLeader()).toBe(true);
        el.addNode('node-1');
        el.receiveHeartbeat(999, 'node-1');
        expect(el.isLeader()).toBe(false);
        expect(el._currentTerm).toBe(999);
        el.stop();
    });

    afterEach(() => { vi.useRealTimers(); });
});

// ═══════════════════════════════════════════════════
//  Vote request / ack
// ═══════════════════════════════════════════════════

describe('receiveVoteReq', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('grants vote for first request in term', () => {
        const { el, callbacks } = makeElection();
        el.start();
        el.addNode('node-1');
        el.receiveVoteReq(1, 'node-1', 0.5);
        expect(callbacks.onSendVoteAck).toHaveBeenCalledWith(0, 1, 'node-1', true);
    });

    it('denies vote for already-voted term (lower priority)', () => {
        const { el, callbacks } = makeElection();
        el.start();
        el.addNode('node-1');
        el.addNode('node-2');
        el.receiveVoteReq(1, 'node-1', 0.5);
        callbacks.onSendVoteAck.mockClear();
        el.receiveVoteReq(1, 'node-2', 0.3); // lower priority
        expect(callbacks.onSendVoteAck).toHaveBeenCalledWith(0, 1, 'node-2', false);
    });

    it('grants vote for higher priority candidate in same term', () => {
        const { el, callbacks } = makeElection();
        el.start();
        el.addNode('node-1');
        el.addNode('node-2');
        el.receiveVoteReq(1, 'node-1', 0.3);
        callbacks.onSendVoteAck.mockClear();
        el.receiveVoteReq(1, 'node-2', 0.9); // higher priority
        expect(callbacks.onSendVoteAck).toHaveBeenCalledWith(0, 1, 'node-2', true);
    });

    it('grants re-vote for same candidate in same term', () => {
        const { el, callbacks } = makeElection();
        el.start();
        el.addNode('node-1');
        el.receiveVoteReq(1, 'node-1', 0.5);
        callbacks.onSendVoteAck.mockClear();
        el.receiveVoteReq(1, 'node-1', 0.5);
        expect(callbacks.onSendVoteAck).toHaveBeenCalledWith(0, 1, 'node-1', true);
    });

    afterEach(() => { vi.useRealTimers(); });
});

describe('receiveVoteAck', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('becomes leader when receiving enough votes', () => {
        const { el, callbacks } = makeElection({ getMinQuorum: () => 2 });
        el.start();
        el.addNode('node-1');

        // Trigger election manually
        el.triggerElection();
        const term = el._currentTerm;

        // Simulate vote ack from node-1
        el.receiveVoteAck(term, 'node-0', true, 'node-1');
        expect(el.isLeader()).toBe(true);
        el.stop();
    });

    it('ignores vote ack for wrong candidate', () => {
        const { el } = makeElection();
        el.start();
        el.addNode('node-1');
        el.triggerElection();
        const term = el._currentTerm;
        el.receiveVoteAck(term, 'wrong-candidate', true, 'node-1');
        // Should not crash, state unchanged
    });

    it('ignores vote ack for wrong term', () => {
        const { el } = makeElection();
        el.start();
        el.addNode('node-1');
        el.triggerElection();
        el.receiveVoteAck(999, 'node-0', true, 'node-1');
        // Should not crash
    });

    afterEach(() => { vi.useRealTimers(); });
});

// ═══════════════════════════════════════════════════
//  Step down / quorum check
// ═══════════════════════════════════════════════════

describe('Step down on quorum loss', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('leader steps down when connected nodes drop below quorum', async () => {
        const { el, callbacks } = makeElection({ getMinQuorum: () => 2 });
        el.start();
        el.addNode('node-1');

        el.triggerElection();
        const term = el._currentTerm;
        el.receiveVoteAck(term, 'node-0', true, 'node-1');
        expect(el.isLeader()).toBe(true);

        // Remove node-1 → only 1 node, quorum=2, should step down on heartbeat
        el.removeNode('node-1');
        await vi.advanceTimersByTimeAsync(200); // heartbeat fires
        expect(el.isLeader()).toBe(false);
        el.stop();
    });

    afterEach(() => { vi.useRealTimers(); });
});

// ═══════════════════════════════════════════════════
//  triggerElection
// ═══════════════════════════════════════════════════

describe('triggerElection', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('does nothing when not started', () => {
        const { el, callbacks } = makeElection();
        el.triggerElection();
        expect(callbacks.onSendVoteReq).not.toHaveBeenCalled();
    });

    it('sends vote request on triggerElection', () => {
        const { el, callbacks } = makeElection();
        el.start();
        el.triggerElection();
        expect(callbacks.onSendVoteReq).toHaveBeenCalledWith(0, expect.any(Number), 'node-0', expect.any(Number));
    });

    it('leader steps down before re-electing', async () => {
        const { el, callbacks } = makeElection();
        el.start();
        await vi.advanceTimersByTimeAsync(200); // become leader
        expect(el.isLeader()).toBe(true);

        callbacks.onLeaderChanged.mockClear();
        el.triggerElection();
        // Should have called onLeaderChanged(null) for step down
        const calls = callbacks.onLeaderChanged.mock.calls;
        expect(calls.some(c => c[1] === null)).toBe(true);
        el.stop();
    });

    // ── 10b 对应：单节点 Leader 主动触发重新选举 ──────────────────────────
    it('single-node leader: immediately candidate after triggerElection (not yet leader)', async () => {
        const { el } = makeElection({ getMinQuorum: () => 1 });
        el.start();
        await vi.advanceTimersByTimeAsync(200); // become leader
        expect(el.isLeader()).toBe(true);

        el.triggerElection();

        // 因为 quorum 检查被延迟了一个 tick，调用方能立刻观察到 candidate 状态
        expect(el.isLeader()).toBe(false);
        expect(el.getStatus().state).toBe('candidate');
        el.stop();
    });

    it('single-node leader: re-elects after one tick', async () => {
        const { el } = makeElection({ getMinQuorum: () => 1 });
        el.start();
        await vi.advanceTimersByTimeAsync(200);
        expect(el.isLeader()).toBe(true);

        el.triggerElection();
        expect(el.isLeader()).toBe(false);   // candidate 状态

        await vi.advanceTimersByTimeAsync(1); // tick 推进 → quorum 检查 → becomeLeader
        expect(el.isLeader()).toBe(true);
        el.stop();
    });

    it('single-node leader: leaderChanges contains stepdown(null) then re-election', async () => {
        const leaderChanges = [];
        const { el } = makeElection({
            getMinQuorum: () => 1,
            onLeaderChanged: (_sid, id) => leaderChanges.push(id),
        });
        el.start();
        await vi.advanceTimersByTimeAsync(200);
        leaderChanges.length = 0; // 清除首次选举事件

        el.triggerElection();
        await vi.advanceTimersByTimeAsync(10);

        expect(leaderChanges).toContain(null);       // 降级事件
        expect(leaderChanges).toContain('node-0');   // 重新当选事件
        el.stop();
    });

    // ── 10c 对应：Follower 主动触发选举 ──────────────────────────────────
    it('follower triggerElection: immediately sends vote request', () => {
        const { el, callbacks } = makeElection({ getMinQuorum: () => 2 });
        el.start();
        el.addNode('node-1');

        callbacks.onSendVoteReq.mockClear();
        el.triggerElection();

        // follower 触发选举后立刻变为 candidate 并广播投票请求
        expect(el.getStatus().state).toBe('candidate');
        expect(callbacks.onSendVoteReq).toHaveBeenCalledWith(
            0, expect.any(Number), 'node-0', expect.any(Number)
        );
        el.stop();
    });

    it('follower triggerElection: wins after receiving quorum acks', async () => {
        const leaderChanges = [];
        const { el } = makeElection({
            getMinQuorum: () => 2,
            onLeaderChanged: (_sid, id) => leaderChanges.push(id),
        });
        el.start();
        el.addNode('node-1');

        el.triggerElection();
        const term = el._currentTerm;
        expect(el.getStatus().state).toBe('candidate');

        // 收到法定票数 ack → 当选
        el.receiveVoteAck(term, 'node-0', true, 'node-1');
        expect(el.isLeader()).toBe(true);
        expect(leaderChanges).toContain('node-0');
        el.stop();
    });

    afterEach(() => { vi.useRealTimers(); });
});

// ═══════════════════════════════════════════════════
//  网络感知：getPriority + getNetworkRttMs
// ═══════════════════════════════════════════════════

describe('LeaderElection network-aware priority', () => {
    beforeEach(() => { vi.useFakeTimers(); });
    afterEach(() => { vi.useRealTimers(); });

    it('uses getPriority callback instead of Math.random', async () => {
        const getPriority = vi.fn().mockReturnValue(0.99);
        const onSendVoteReq = vi.fn();
        const { el } = makeElection({ getPriority, onSendVoteReq });
        el.start();
        await vi.advanceTimersByTimeAsync(200);

        const calls = onSendVoteReq.mock.calls;
        expect(calls.length).toBeGreaterThan(0);
        const priority = calls[0][3]; // (shardId, term, candidateId, priority)
        expect(priority).toBe(0.99);
        expect(getPriority).toHaveBeenCalled();
        el.stop();
    });

    it('falls back to Math.random when getPriority not provided', async () => {
        const onSendVoteReq = vi.fn();
        const { el } = makeElection({ onSendVoteReq }); // no getPriority
        el.start();
        await vi.advanceTimersByTimeAsync(200);

        const calls = onSendVoteReq.mock.calls;
        expect(calls.length).toBeGreaterThan(0);
        const priority = calls[0][3];
        expect(priority).toBeGreaterThanOrEqual(0);
        expect(priority).toBeLessThan(1);
        el.stop();
    });
});

describe('LeaderElection dynamic election timeout via getNetworkRttMs', () => {
    it('uses shorter timeout range when RTT is small (LAN ~1ms)', () => {
        const delays = [];
        const timerAPI = {
            setTimeout:    vi.fn((fn, ms) => { delays.push(ms); return 1; }),
            clearTimeout:  vi.fn(),
            setInterval:   vi.fn(() => 2),
            clearInterval: vi.fn(),
        };
        // rtt=1ms → dynMin=max(100*1.5, 1*20)=150, dynMax=max(100*3, 1*40)=300
        const { el } = makeElection({
            timerAPI,
            getNetworkRttMs: () => 1,
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 50, maxElectionMs: 100 },
        });
        el.start();
        expect(delays.length).toBeGreaterThan(0);
        const d = delays[0];
        expect(d).toBeGreaterThanOrEqual(150);
        expect(d).toBeLessThanOrEqual(300);
        el.stop();
    });

    it('uses longer timeout range when RTT is high (WAN ~200ms)', () => {
        const delays = [];
        const timerAPI = {
            setTimeout:    vi.fn((fn, ms) => { delays.push(ms); return 1; }),
            clearTimeout:  vi.fn(),
            setInterval:   vi.fn(() => 2),
            clearInterval: vi.fn(),
        };
        // rtt=200ms → dynMin=max(100*1.5, 200*20)=4000, dynMax=max(100*3, 200*40)=8000
        const { el } = makeElection({
            timerAPI,
            getNetworkRttMs: () => 200,
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 50, maxElectionMs: 100 },
        });
        el.start();
        expect(delays.length).toBeGreaterThan(0);
        const d = delays[0];
        expect(d).toBeGreaterThanOrEqual(4000);
        expect(d).toBeLessThanOrEqual(8000);
        el.stop();
    });

    it('falls back to default range when getNetworkRttMs returns null', () => {
        const delays = [];
        const timerAPI = {
            setTimeout:    vi.fn((fn, ms) => { delays.push(ms); return 1; }),
            clearTimeout:  vi.fn(),
            setInterval:   vi.fn(() => 2),
            clearInterval: vi.fn(),
        };
        const { el } = makeElection({
            timerAPI,
            getNetworkRttMs: () => null,
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 50, maxElectionMs: 200 },
        });
        el.start();
        const d = delays[0];
        expect(d).toBeGreaterThanOrEqual(50);
        expect(d).toBeLessThanOrEqual(200);
        el.stop();
    });
});

// ═══════════════════════════════════════════════════
//  Leader 租约（Leader Lease）
// ═══════════════════════════════════════════════════

describe('LeaderElection lease', () => {
    beforeEach(() => { vi.useFakeTimers(); });
    afterEach(() => { vi.useRealTimers(); });

    it('isLeaseValid returns false before becoming leader', () => {
        const { el } = makeElection();
        el.start();
        expect(el.isLeaseValid()).toBe(false);
        el.stop();
    });

    it('isLeaseValid returns true immediately after becoming leader (epsilon=0)', async () => {
        let nowMs = 0;
        const getNowMs = () => nowMs;
        const { el } = makeElection({ getNowMs, getEpsilonMs: () => 0 });
        el.start();
        await vi.advanceTimersByTimeAsync(200);
        expect(el.isLeader()).toBe(true);
        expect(el.isLeaseValid()).toBe(true);
        el.stop();
    });

    it('isLeaseValid returns false after lease expires', async () => {
        let nowMs = 0;
        const getNowMs = () => nowMs;
        // epsilon=0, electionMin=50ms → leaseDuration=50ms
        const { el } = makeElection({
            getNowMs,
            getEpsilonMs: () => 0,
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 50, maxElectionMs: 100 },
        });
        el.start();
        await vi.advanceTimersByTimeAsync(200);
        expect(el.isLeader()).toBe(true);
        expect(el.isLeaseValid()).toBe(true);

        // Advance local clock past leaseDuration (50ms) without refreshing
        nowMs = 200; // > 50ms after leaseStart=0
        expect(el.isLeaseValid()).toBe(false);
        el.stop();
    });

    it('lease is refreshed on each heartbeat tick', async () => {
        let nowMs = 0;
        const getNowMs = () => nowMs;
        const { el } = makeElection({
            getNowMs,
            getEpsilonMs: () => 0,
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 50, maxElectionMs: 100 },
        });
        el.start();
        await vi.advanceTimersByTimeAsync(200);
        expect(el.isLeader()).toBe(true);

        // Advance beyond initial lease
        nowMs = 60;
        expect(el.isLeaseValid()).toBe(false); // expired

        // Heartbeat tick refreshes lease
        await vi.advanceTimersByTimeAsync(100); // triggers heartbeat interval
        nowMs = 60; // keep same time
        expect(el.isLeaseValid()).toBe(true); // refreshed
        el.stop();
    });

    it('isLeaseValid returns false when epsilon >= electionMin (leaseDuration=0)', async () => {
        let nowMs = 0;
        const getNowMs = () => nowMs;
        // epsilon=100ms >= electionMin=50ms → leaseDuration=max(0,50-100)=0
        const { el } = makeElection({
            getNowMs,
            getEpsilonMs: () => 100,
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 50, maxElectionMs: 100 },
        });
        el.start();
        await vi.advanceTimersByTimeAsync(200);
        expect(el.isLeader()).toBe(true);
        expect(el.isLeaseValid()).toBe(false); // epsilon too large
        el.stop();
    });

    it('lease is cleared immediately on stepDown', async () => {
        let nowMs = 0;
        const getNowMs = () => nowMs;
        const { el } = makeElection({ getNowMs, getEpsilonMs: () => 0 });
        el.start();
        await vi.advanceTimersByTimeAsync(200);
        expect(el.isLeader()).toBe(true);
        expect(el.isLeaseValid()).toBe(true);

        el._stepDown(el._currentTerm + 1); // force step down
        expect(el.isLeaseValid()).toBe(false);
        el.stop();
    });
});

// ═══════════════════════════════════════════════════
//  getStatus
// ═══════════════════════════════════════════════════

describe('getStatus', () => {
    it('returns correct structure', () => {
        const { el } = makeElection();
        const status = el.getStatus();
        expect(status).toHaveProperty('shardId', 0);
        expect(status).toHaveProperty('state', 'follower');
        expect(status).toHaveProperty('term', 0);
        expect(status).toHaveProperty('leaderNodeId', null);
        expect(status).toHaveProperty('currentQuorum');
        expect(status).toHaveProperty('connectedNodes', 1); // self
    });

    it('connectedNodes includes self + added nodes', () => {
        const { el } = makeElection();
        el.addNode('node-1');
        el.addNode('node-2');
        expect(el.getStatus().connectedNodes).toBe(3);
    });
});

// ═══════════════════════════════════════════════════
//  Pre-Vote（预投票）
// ═══════════════════════════════════════════════════

function makeCluster(ids, quorum) {
    const net = {};
    const hbBlock = new Set();
    for (const id of ids) {
        const sent = {
            onSendLeaderQuery: vi.fn((sid, queryId, target) => {
                net[target].el.receiveLeaderQuery(queryId, id);
            }),
            onSendLeaderQueryAck: vi.fn((sid, queryId, askerId, alive, leaderId, term) => {
                net[askerId].el.receiveLeaderQueryAck(queryId, alive, leaderId, term, id);
            }),
            onSendPreVoteReq: vi.fn((sid, term, cand, prio) => {
                for (const o of ids) if (o !== id) net[o].el.receivePreVoteReq(term, cand, prio);
            }),
            onSendPreVoteAck: vi.fn((sid, term, cand, granted) => {
                net[cand].el.receivePreVoteAck(term, cand, granted, id);
            }),
            onSendVoteReq: vi.fn((sid, term, cand, prio) => {
                for (const o of ids) if (o !== id) net[o].el.receiveVoteReq(term, cand, prio);
            }),
            onSendVoteAck: vi.fn((sid, term, cand, granted) => {
                net[cand].el.receiveVoteAck(term, cand, granted, id);
            }),
            onSendHeartbeat: vi.fn((sid, term, leaderId) => {
                for (const o of ids) if (o !== id && !hbBlock.has(o)) net[o].el.receiveHeartbeat(term, leaderId);
            }),
        };
        const { el } = makeElection({
            nodeId: id,
            getMinQuorum: () => quorum,
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 400 },
            ...sent,
        });
        const asked = { count: 0 };
        const origReceiveQuery = el.receiveLeaderQuery.bind(el);
        el.receiveLeaderQuery = (qid, asker) => { asked.count++; return origReceiveQuery(qid, asker); };
        net[id] = { el, sent, asked };
    }
    for (const id of ids) for (const o of ids) if (o !== id) net[id].el.addNode(o);
    return { net, hbBlock };
}

describe('Pre-Vote', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('receiver refuses when it heard the leader within the min election window, without side effects', () => {
        const { el, callbacks } = makeElection({
            onSendPreVoteAck: vi.fn(),
        });
        el.start();
        el.addNode('leader-1');
        el.addNode('cand-1');
        el.receiveHeartbeat(1, 'leader-1');
        el.receivePreVoteReq(2, 'cand-1', 0.5);
        expect(el._onSendPreVoteAck).toHaveBeenCalledWith(0, 2, 'cand-1', false);
        // 探询不扰动接收方：term 不变、leader 视图不变、没有投出正式票
        expect(el.getStatus().term).toBe(1);
        expect(el.getLeaderNodeId()).toBe('leader-1');
        expect(callbacks.onSendVoteAck).not.toHaveBeenCalled();
        el.stop();
    });

    it('receiver grants when the leader has been silent beyond the window, and the intention does not consume the formal vote', () => {
        const { el, callbacks } = makeElection({ onSendPreVoteAck: vi.fn() });
        el.start();
        el.addNode('leader-1');
        el.addNode('cand-1');
        el.receiveHeartbeat(1, 'leader-1');
        el._lastLeaderHeartbeatAt = Date.now() - 100000; // leader 久无心跳
        el.receivePreVoteReq(2, 'cand-1', 0.5);
        expect(el._onSendPreVoteAck).toHaveBeenCalledWith(0, 2, 'cand-1', true);
        // 意向票不是正式票：随后同 term 的正式 vote_req 仍应获得投票
        el.receiveVoteReq(2, 'cand-1', 0.5);
        expect(callbacks.onSendVoteAck).toHaveBeenCalledWith(0, 2, 'cand-1', true);
        el.stop();
    });

    it('the leader itself always refuses', () => {
        const { el } = makeElection({ onSendPreVoteAck: vi.fn() });
        el.start();
        el.addNode('node-1');
        el.addNode('node-2');
        el.triggerElection();
        el.receiveVoteAck(1, 'node-0', true, 'node-1');
        expect(el.isLeader()).toBe(true);
        el.receivePreVoteReq(2, 'node-1', 0.9);
        expect(el._onSendPreVoteAck).toHaveBeenCalledWith(0, 2, 'node-1', false);
        el.stop();
    });

    it('a node that cannot hear the leader cannot force a re-election while the majority still can', async () => {
        const { net, hbBlock } = makeCluster(['A', 'B', 'C'], 2);
        for (const id of ['A', 'B', 'C']) net[id].el.start();
        net.A.el.triggerElection();
        expect(net.A.el.isLeader()).toBe(true);
        const term = net.A.el.getStatus().term;

        hbBlock.add('C'); // C 与 leader 的链路断了，但与 B 互通
        await vi.advanceTimersByTimeAsync(1500);

        // C 先走存活问询：A（leader 本人）确认在世 → 问询即解决，连预投票都不必
        expect(net.C.sent.onSendLeaderQuery.mock.calls.length).toBeGreaterThanOrEqual(1);
        expect(net.C.sent.onSendPreVoteReq).not.toHaveBeenCalled();
        expect(net.C.sent.onSendVoteReq).not.toHaveBeenCalled();
        expect(net.A.el.isLeader()).toBe(true);
        for (const id of ['A', 'B', 'C']) {
            expect(net[id].el.getStatus().term).toBe(term);
            expect(net[id].el.getLeaderNodeId()).toBe('A');
        }
        for (const id of ['A', 'B', 'C']) net[id].el.stop();
    });

    it('when the leader truly dies, the survivors still elect a new one through pre-vote', async () => {
        const { net } = makeCluster(['A', 'B', 'C'], 2);
        for (const id of ['A', 'B', 'C']) net[id].el.start();
        net.A.el.triggerElection();
        expect(net.A.el.isLeader()).toBe(true);

        net.A.el.stop(); // leader 真死：心跳停止
        await vi.advanceTimersByTimeAsync(5000);

        const leaders = ['B', 'C'].filter((id) => net[id].el.isLeader());
        expect(leaders.length).toBe(1);
        const other = leaders[0] === 'B' ? 'C' : 'B';
        expect(net[other].el.getLeaderNodeId()).toBe(leaders[0]);
        net.B.el.stop();
        net.C.el.stop();
    });

    it('fresh cluster elects a leader through the pre-vote path', async () => {
        const { net } = makeCluster(['A', 'B', 'C'], 2);
        for (const id of ['A', 'B', 'C']) net[id].el.start();
        await vi.advanceTimersByTimeAsync(5000);
        const leaders = ['A', 'B', 'C'].filter((id) => net[id].el.isLeader());
        expect(leaders.length).toBe(1);
        for (const id of ['A', 'B', 'C']) {
            if (id !== leaders[0]) expect(net[id].el.getLeaderNodeId()).toBe(leaders[0]);
        }
        // 胜者确实先走了预投票再正式参选
        expect(net[leaders[0]].sent.onSendPreVoteReq.mock.calls.length).toBeGreaterThanOrEqual(1);
        for (const id of ['A', 'B', 'C']) net[id].el.stop();
    });

    it('zero pre-vote replies fall back to a direct election (mixed-version liveness)', async () => {
        const { el, callbacks } = makeElection({ getMinQuorum: () => 2 });
        el.start();
        el.addNode('node-1');
        el.addNode('node-2');
        await vi.advanceTimersByTimeAsync(500);
        // 未注入预投票回调 → 探询零回复 → 回退直接选举
        expect(callbacks.onSendVoteReq).toHaveBeenCalled();
        expect(el.getStatus().state).toBe('candidate');
        el.receiveVoteAck(el.getStatus().term, 'node-0', true, 'node-1');
        expect(el.isLeader()).toBe(true);
        el.stop();
    });

    it('an accepted heartbeat cancels an in-flight inquiry', async () => {
        const sentQuery = vi.fn();
        const { el, callbacks } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: sentQuery,
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('A');
        el.addNode('B');
        await vi.advanceTimersByTimeAsync(330); // 选举超时 → 问询开始（窗口 300ms，未到期）
        expect(sentQuery).toHaveBeenCalled();
        el.receiveHeartbeat(1, 'A'); // 链路恢复，leader 还在：问询取消
        expect(el._inquiry).toBeNull();
        // 迟到的问询应答不得再推进流程
        el.receiveLeaderQueryAck(1, false, null, 1, 'B');
        el.receiveLeaderQueryAck(1, false, null, 1, 'A');
        await vi.advanceTimersByTimeAsync(200);
        expect(callbacks.onSendVoteReq).not.toHaveBeenCalled();
        expect(el.getStatus().state).toBe('follower');
        el.stop();
    });

    afterEach(() => { vi.useRealTimers(); });
});

// ═══════════════════════════════════════════════════
//  Relayed heartbeat（传递心跳）
// ═══════════════════════════════════════════════════

describe('Relayed heartbeat', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('accepts a relayed heartbeat from a leader it is not directly connected to', async () => {
        const { el } = makeElection({ getMinQuorum: () => 2 });
        el.start();
        el.addNode('relay-peer'); // 直连的是中继者，不是 leader 本人
        el.receiveHeartbeat(3, 'far-leader', { relayed: true });
        expect(el.getLeaderNodeId()).toBe('far-leader');
        expect(el.getStatus().term).toBe(3);
        // 传递心跳同样刷新存活证据：选举计时器被重置，不会立刻探询
        await vi.advanceTimersByTimeAsync(30);
        expect(el.getStatus().state).toBe('follower');
        el.stop();
    });

    it('rejects a direct heartbeat from a non-connected leader (unchanged rule)', () => {
        const { el } = makeElection();
        el.start();
        el.addNode('someone');
        el.receiveHeartbeat(3, 'far-leader');
        expect(el.getLeaderNodeId()).toBeNull();
        el.stop();
    });

    it('ignores its own heartbeat coming back through a relay', () => {
        const { el } = makeElection();
        el.start();
        el.addNode('relay-peer');
        el.receiveHeartbeat(2, 'node-0', { relayed: true }); // node-0 是自己
        expect(el.getLeaderNodeId()).toBeNull();
        el.stop();
    });

    afterEach(() => { vi.useRealTimers(); });
});

// ═══════════════════════════════════════════════════
//  Relay grace / repair（传递存活修复窗口）
// ═══════════════════════════════════════════════════

// ═══════════════════════════════════════════════════
//  Leader inquiry（存活问询）
// ═══════════════════════════════════════════════════

function makeInquiryCluster(ids, quorum) {
    const net = {};
    const hbBlock = new Set();
    for (const id of ids) {
        const sent = {
            onSendLeaderQuery: vi.fn((sid, queryId, target) => {
                net[target].el.receiveLeaderQuery(queryId, id);
            }),
            onSendLeaderQueryAck: vi.fn((sid, queryId, askerId, alive, leaderId, term) => {
                net[askerId].el.receiveLeaderQueryAck(queryId, alive, leaderId, term, id);
            }),
            onSendPreVoteReq: vi.fn((sid, term, cand, prio) => {
                for (const o of ids) if (o !== id) net[o].el.receivePreVoteReq(term, cand, prio);
            }),
            onSendPreVoteAck: vi.fn((sid, term, cand, granted) => {
                net[cand].el.receivePreVoteAck(term, cand, granted, id);
            }),
            onSendVoteReq: vi.fn((sid, term, cand, prio) => {
                for (const o of ids) if (o !== id) net[o].el.receiveVoteReq(term, cand, prio);
            }),
            onSendVoteAck: vi.fn((sid, term, cand, granted) => {
                net[cand].el.receiveVoteAck(term, cand, granted, id);
            }),
            onSendHeartbeat: vi.fn((sid, term, leaderId) => {
                for (const o of ids) if (o !== id && !hbBlock.has(o)) net[o].el.receiveHeartbeat(term, leaderId);
            }),
        };
        const { el } = makeElection({
            nodeId: id,
            getMinQuorum: () => quorum,
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 400 },
            ...sent,
        });
        const asked = { count: 0 };
        const origReceiveQuery = el.receiveLeaderQuery.bind(el);
        el.receiveLeaderQuery = (qid, asker) => { asked.count++; return origReceiveQuery(qid, asker); };
        net[id] = { el, sent, asked };
    }
    for (const id of ids) for (const o of ids) if (o !== id) net[id].el.addNode(o);
    return { net, hbBlock };
}

describe('Leader inquiry', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('a node cut off from the leader stands down when peers confirm the leader is alive', async () => {
        const { net, hbBlock } = makeInquiryCluster(['A', 'B', 'C'], 2);
        for (const id of ['A', 'B', 'C']) net[id].el.start();
        net.A.el.triggerElection();
        expect(net.A.el.isLeader()).toBe(true);
        const term = net.A.el.getStatus().term;

        hbBlock.add('C'); // C 听不到 leader 心跳，但与 A、B 直连互通
        await vi.advanceTimersByTimeAsync(3000);

        // C 顺序问询：第一个问的就是 leader A，得到确认即停 —— B 从未被问到
        expect(net.C.sent.onSendLeaderQuery.mock.calls.length).toBeGreaterThanOrEqual(1);
        expect(net.A.asked.count).toBeGreaterThanOrEqual(1);
        expect(net.B.asked.count).toBe(0);
        expect(net.C.sent.onSendPreVoteReq).not.toHaveBeenCalled();
        expect(net.C.sent.onSendVoteReq).not.toHaveBeenCalled();
        expect(net.A.el.isLeader()).toBe(true);
        for (const id of ['A', 'B', 'C']) {
            expect(net[id].el.getStatus().term).toBe(term);
            expect(net[id].el.getLeaderNodeId()).toBe('A');
        }
        for (const id of ['A', 'B', 'C']) net[id].el.stop();
    });

    it('when no peer can confirm the leader, the node proceeds through pre-vote to a real election', async () => {
        const { net } = makeInquiryCluster(['A', 'B', 'C'], 2);
        for (const id of ['A', 'B', 'C']) net[id].el.start();
        net.A.el.triggerElection();
        expect(net.A.el.isLeader()).toBe(true);

        net.A.el.stop(); // leader 真死
        await vi.advanceTimersByTimeAsync(6000);

        const leaders = ['B', 'C'].filter((id) => net[id].el.isLeader());
        expect(leaders.length).toBe(1);
        const other = leaders[0] === 'B' ? 'C' : 'B';
        expect(net[other].el.getLeaderNodeId()).toBe(leaders[0]);
        net.B.el.stop();
        net.C.el.stop();
    });

    it('one decisive confirmation ends the inquiry (later acks are ignored)', async () => {
        const { el, callbacks } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            onSendPreVoteReq: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('L');
        el.addNode('B');
        el.receiveHeartbeat(1, 'L');
        await vi.advanceTimersByTimeAsync(330); // 超时 → 问询开始
        expect(el._onSendLeaderQuery).toHaveBeenCalled();
        el.receiveLeaderQueryAck(1, true, 'L', 1, 'B'); // 决定性确认
        expect(el._inquiry).toBeNull();
        el.receiveLeaderQueryAck(1, false, null, 1, 'L'); // 迟到应答：已结束，忽略
        await vi.advanceTimersByTimeAsync(200);
        expect(el._onSendPreVoteReq).not.toHaveBeenCalled();
        expect(callbacks.onSendVoteReq).not.toHaveBeenCalled();
        expect(el.getLeaderNodeId()).toBe('L');
        el.stop();
    });

    it('an ack about a different leader (same term) is not decisive; sequence moves on and exhausts into pre-vote', async () => {
        const { el, callbacks } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            onSendPreVoteReq: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('L');
        el.addNode('B');
        el.receiveHeartbeat(1, 'L');
        await vi.advanceTimersByTimeAsync(330); // 问询开始，先问 L
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'L');
        el.receiveLeaderQueryAck(1, true, 'OTHER', 1, 'B'); // 非当前被问者的别域确认：不决定性、不推进
        expect(el._inquiry).not.toBeNull();
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        el.receiveLeaderQueryAck(1, false, 'L', 1, 'L'); // 当前被问者 L：无法确认 → 问下一个
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(2);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'B');
        el.receiveLeaderQueryAck(1, false, 'L', 1, 'B'); // B 也无法确认 → 全部问完
        expect(el._onSendPreVoteReq).toHaveBeenCalled(); // 转入预投票
        expect(callbacks.onSendVoteReq).not.toHaveBeenCalled();
        el.stop();
    });

    it('per-ask timeout is dynamic: silent peer is skipped after its RTT-scaled timeout', async () => {
        const { el } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            getPeerRttMs: (id) => (id === 'B' ? 10 : null),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('B');
        el.addNode('C');
        el.receiveHeartbeat(1, 'B'); // 视图 leader=B（不影响问询目标顺序）
        el._currentLeaderNodeId = 'L';
        await vi.advanceTimersByTimeAsync(330); // 问询开始，先问 B（队列序）
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'B');
        // B 近期 RTT=10ms → 超时 = 1.5×10+50 = 65ms；未到期不推进，到期 B 不回 → 问 C
        await vi.advanceTimersByTimeAsync(30);
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        await vi.advanceTimersByTimeAsync(60);
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(2);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'C');
        el.receiveLeaderQueryAck(1, true, 'L', 1, 'C'); // C 确认 → 停问
        expect(el._inquiry).toBeNull();
        expect(el.getLeaderNodeId()).toBe('L');
        el.stop();
    });

    afterEach(() => { vi.useRealTimers(); });
});
