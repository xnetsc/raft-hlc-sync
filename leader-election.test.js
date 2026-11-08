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
//  问询把关的选举流程（集群）
// ═══════════════════════════════════════════════════

function makeCluster(ids, quorum) {
    const net = {};
    const hbBlock = new Set();
    for (const id of ids) {
        const sent = {
            onSendLeaderQuery: vi.fn((sid, queryId, target, targetLeaderId) => {
                net[target].el.receiveLeaderQuery(queryId, id, targetLeaderId);
            }),
            onSendLeaderQueryAck: vi.fn((sid, queryId, askerId, verdict, leaderId, term) => {
                net[askerId].el.receiveLeaderQueryAck(queryId, verdict, leaderId, term, id);
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

describe('Inquiry-gated election', () => {
    beforeEach(() => { vi.useFakeTimers(); });

    it('a node that cannot hear the leader cannot force a re-election while the majority still can', async () => {
        const { net, hbBlock } = makeCluster(['A', 'B', 'C'], 2);
        for (const id of ['A', 'B', 'C']) net[id].el.start();
        net.A.el.triggerElection();
        expect(net.A.el.isLeader()).toBe(true);
        const term = net.A.el.getStatus().term;

        hbBlock.add('C'); // C 与 leader 的链路断了，但与 B 互通
        await vi.advanceTimersByTimeAsync(1500);

        // C 先走存活问询：A（leader 本人）回明确"在线" → 问询即终结，不选举
        expect(net.C.sent.onSendLeaderQuery.mock.calls.length).toBeGreaterThanOrEqual(1);
        expect(net.C.sent.onSendVoteReq).not.toHaveBeenCalled();
        expect(net.A.el.isLeader()).toBe(true);
        for (const id of ['A', 'B', 'C']) {
            expect(net[id].el.getStatus().term).toBe(term);
            expect(net[id].el.getLeaderNodeId()).toBe('A');
        }
        for (const id of ['A', 'B', 'C']) net[id].el.stop();
    });

    it('when the leader truly dies, the survivors still elect a new one (inquiry first, then direct election)', async () => {
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

    it('fresh cluster elects a leader through inquiry-then-election', async () => {
        const { net } = makeCluster(['A', 'B', 'C'], 2);
        for (const id of ['A', 'B', 'C']) net[id].el.start();
        await vi.advanceTimersByTimeAsync(5000);
        const leaders = ['A', 'B', 'C'].filter((id) => net[id].el.isLeader());
        expect(leaders.length).toBe(1);
        for (const id of ['A', 'B', 'C']) {
            if (id !== leaders[0]) expect(net[id].el.getLeaderNodeId()).toBe(leaders[0]);
        }
        for (const id of ['A', 'B', 'C']) net[id].el.stop();
    });

    it('with no leader-query callback injected, asks time out one by one and the node still reaches a direct election', async () => {
        const { el, callbacks } = makeElection({ getMinQuorum: () => 2 });
        el.start();
        el.addNode('node-1');
        el.addNode('node-2');
        await vi.advanceTimersByTimeAsync(500);
        // 未注入问询回调 → 问发不出去，两个目标依次超时问完 → 直接选举
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
        el.receiveLeaderQueryAck(1, 'offline', null, 1, 'B');
        el.receiveLeaderQueryAck(1, 'offline', null, 1, 'A');
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
            onSendLeaderQuery: vi.fn((sid, queryId, target, targetLeaderId) => {
                net[target].el.receiveLeaderQuery(queryId, id, targetLeaderId);
            }),
            onSendLeaderQueryAck: vi.fn((sid, queryId, askerId, verdict, leaderId, term) => {
                net[askerId].el.receiveLeaderQueryAck(queryId, verdict, leaderId, term, id);
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

        // C 顺序问询：被问者共 2 人，过半门槛 = 2——A（leader 本人）与
        // B（直连心跳新鲜）都回明确"在线"，凑满过半才停问、不选举
        expect(net.C.sent.onSendLeaderQuery.mock.calls.length).toBeGreaterThanOrEqual(1);
        expect(net.A.asked.count).toBeGreaterThanOrEqual(1);
        expect(net.B.asked.count).toBeGreaterThanOrEqual(1);
        expect(net.C.sent.onSendVoteReq).not.toHaveBeenCalled();
        expect(net.A.el.isLeader()).toBe(true);
        for (const id of ['A', 'B', 'C']) {
            expect(net[id].el.getStatus().term).toBe(term);
            expect(net[id].el.getLeaderNodeId()).toBe('A');
        }
        for (const id of ['A', 'B', 'C']) net[id].el.stop();
    });

    it('when no peer can confirm the leader, the node proceeds to a direct election', async () => {
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

    it('a strict majority of explicit "online" ends the inquiry; a single one does not (later acks are ignored)', async () => {
        const { el, callbacks } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('L');
        el.addNode('B');
        el.receiveHeartbeat(1, 'L');
        await vi.advanceTimersByTimeAsync(330); // 超时 → 问询开始（被问者 L、B 共 2 人，过半 = 2）
        expect(el._onSendLeaderQuery).toHaveBeenCalled();
        el.receiveLeaderQueryAck(1, 'online', 'L', 1, 'B'); // 只有 B 一人回明确"在线"：1/2 不过半 → 问询继续
        expect(el._inquiry).not.toBeNull();
        el.receiveLeaderQueryAck(1, 'online', 'L', 1, 'L'); // L 也回明确"在线"：2/2 过半 → 终结问询
        expect(el._inquiry).toBeNull();
        el.receiveLeaderQueryAck(1, 'offline', null, 1, 'L'); // 迟到应答：已结束，忽略
        await vi.advanceTimersByTimeAsync(200);
        expect(callbacks.onSendVoteReq).not.toHaveBeenCalled();
        expect(el.getLeaderNodeId()).toBe('L');
        el.stop();
    });

    it('an "online" ack about a different leader does not count toward the asked leader; without a majority for it the node elects', async () => {
        const { el, callbacks } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('L');
        el.addNode('B');
        el.receiveHeartbeat(1, 'L');
        await vi.advanceTimersByTimeAsync(330); // 问询开始，先问 L（被问者 2 人，过半 = 2）
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'L', 'L');
        el.receiveLeaderQueryAck(1, 'online', 'OTHER', 1, 'L'); // 当前被问者 L 回的"在线"指向别域 leader：只记 OTHER 的票，不算 L 的 → 不终结，问下一个
        expect(el._inquiry).not.toBeNull();
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(2);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'B', 'L');
        el.receiveLeaderQueryAck(1, 'offline', 'L', 1, 'B'); // B 回不在线了：L 的票 0/2、OTHER 也只有 1/2 → 达不到过半 → 直接选举
        expect(callbacks.onSendVoteReq).toHaveBeenCalled();
        expect(el.getStatus().state).toBe('candidate');
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
        el.addNode('D');
        el.receiveHeartbeat(1, 'B'); // 视图 leader=B（不影响问询目标顺序）
        el._currentLeaderNodeId = 'L';
        await vi.advanceTimersByTimeAsync(330); // 问询开始，先问 B（队列序；被问者 3 人，过半 = 2）
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'B', 'L');
        // B 近期 RTT=10ms → 超时 = 1.5×10+50 = 65ms；未到期不推进，到期 B 不回 → 问 C
        await vi.advanceTimersByTimeAsync(30);
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        await vi.advanceTimersByTimeAsync(60);
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(2);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'C', 'L');
        el.receiveLeaderQueryAck(1, 'online', 'L', 1, 'C'); // C 回明确"在线"：1/3 未过半 → 继续问 D
        expect(el._inquiry).not.toBeNull();
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(3);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'D', 'L');
        el.receiveLeaderQueryAck(1, 'online', 'L', 1, 'D'); // D 也回明确"在线"：2/3 过半 → 停问
        expect(el._inquiry).toBeNull();
        expect(el.getLeaderNodeId()).toBe('L');
        el.stop();
    });

    it('leader reconnect during inquiry stops the asking immediately (no waiting for its next heartbeat)', async () => {
        const { el, callbacks } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('L');
        el.addNode('B');
        el.receiveHeartbeat(1, 'L'); // leader = L
        el.removeNode('L'); // 直连断开：leader 视图清空
        expect(el.getLeaderNodeId()).toBeNull();
        await vi.advanceTimersByTimeAsync(330); // 超时 → 问询开始，问 B
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'B', 'L');
        el.addNode('L'); // 问到一半，L 的直连恢复 → 立刻停问
        expect(el._inquiry).toBeNull();
        // 重连不算存活证据：视图不恢复，等它的心跳来确认
        expect(el.getLeaderNodeId()).toBeNull();
        await vi.advanceTimersByTimeAsync(250); // 不升级到选举
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        expect(callbacks.onSendVoteReq).not.toHaveBeenCalled();
        el.stop();
    });

    it('reconnect of a non-leader peer during inquiry does not stop the asking', async () => {
        const { el } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('L');
        el.addNode('B');
        el.addNode('X');
        el.receiveHeartbeat(1, 'L');
        await vi.advanceTimersByTimeAsync(330); // 问询开始，先问 L（被问者 3 人）
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        el.addNode('Y'); // 无关节点加入（不在本轮被问名单内）：不影响问询
        expect(el._inquiry).not.toBeNull();
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        el.receiveLeaderQueryAck(1, 'offline', 'L', 1, 'L'); // L 回不在线了 → 顺序推进到 B
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(2);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'B', 'L');
        el.stop();
    });

    it('once the inquiry is exhausted the node goes straight to a formal election; a late leader reconnect does not cancel the candidacy', async () => {
        const { el, callbacks } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            getPeerRttMs: () => 10, // 问询超时 = 1.5×10+50 = 65ms，B 不答即问完
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('L');
        el.addNode('B');
        el.receiveHeartbeat(1, 'L');
        el.removeNode('L'); // leader 直连断开
        await vi.advanceTimersByTimeAsync(330); // 问询开始，问 B
        await vi.advanceTimersByTimeAsync(100); // B 超时 → 全部问完 → 直接正式选举（无中间一轮）
        expect(callbacks.onSendVoteReq).toHaveBeenCalled();
        expect(el.getStatus().state).toBe('candidate');
        expect(el._inquiry).toBeNull();
        // 重连只停"问"，停不了已开始的"投"：旧 leader 的旧 term 心跳同样作废
        el.addNode('L');
        el.receiveHeartbeat(1, 'L');
        expect(el.getStatus().state).toBe('candidate');
        expect(el.getStatus().term).toBe(2);
        el.stop();
    });

    it('responder verdicts: four replies, each with an exact meaning, all from its own direct evidence about the asked leader', async () => {
        // 被问的就是本节点的 leader，且直连心跳新鲜 → "在线"
        const direct = makeElection({ onSendLeaderQueryAck: vi.fn() });
        direct.el.start();
        direct.el.addNode('L');
        direct.el.receiveHeartbeat(1, 'L');
        direct.el.receiveLeaderQuery(9, 'asker', 'L');
        expect(direct.el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 9, 'asker', 'online', 'L', 1);
        direct.el.stop();

        // 被问的就是自己（自己是 leader）→ "在线"
        const self = makeElection({ onSendLeaderQueryAck: vi.fn() });
        self.el.start();
        self.el.triggerElection(); // quorum=1
        await vi.advanceTimersByTimeAsync(1); // 0ms 回调落定 leader 状态
        expect(self.el.isLeader()).toBe(true);
        self.el.receiveLeaderQuery(9, 'asker', 'node-0');
        expect(self.el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 9, 'asker', 'online', 'node-0', 1);
        self.el.stop();

        // 对被问 leader 的证据是问别人问来的（问询确认）→ "在线，但是我是问的别人"
        const relayed = makeElection({ onSendLeaderQueryAck: vi.fn() });
        relayed.el.start();
        relayed.el.addNode('peer');
        relayed.el.receiveHeartbeat(1, 'L', { relayed: true });
        relayed.el.receiveLeaderQuery(9, 'asker', 'L');
        expect(relayed.el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 9, 'asker', 'online_indirect', 'L', 1);
        relayed.el.stop();

        // 被问的就是本节点的 leader，但直连与问询确认的证据都已过期 → "不在线了"
        const stale = makeElection({ onSendLeaderQueryAck: vi.fn() });
        stale.el.start();
        stale.el.addNode('L');
        stale.el.receiveHeartbeat(1, 'L');
        stale.el._lastLeaderHeartbeatAt = 0; // 证据强制过期（且不在问询过程中）
        stale.el._lastRelayedHeartbeatAt = 0;
        stale.el.receiveLeaderQuery(9, 'asker', 'L');
        expect(stale.el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 9, 'asker', 'offline', 'L', 1);
        stale.el.stop();

        // 没有 leader 视图、被问一个自己不跟随的 leader → 无从判断 → "不确定"（应答回显所问的 leader）
        const none = makeElection({ onSendLeaderQueryAck: vi.fn() });
        none.el.start();
        none.el.addNode('L');
        none.el.receiveLeaderQuery(9, 'asker', 'L');
        expect(none.el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 9, 'asker', 'uncertain', 'L', 0);
        none.el.stop();

        // 本节点的 leader 是另一个（直连心跳来自 Y）：被问 X 时不许拿 Y 的心跳给 X 背书；
        // 但 X 不是它的 leader，它对 X 的死活根本不知道 → "不确定"，不是"不在线了"
        const other = makeElection({ onSendLeaderQueryAck: vi.fn() });
        other.el.start();
        other.el.addNode('Y');
        other.el.receiveHeartbeat(1, 'Y');
        other.el.receiveLeaderQuery(9, 'asker', 'X');
        expect(other.el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 9, 'asker', 'uncertain', 'X', 1);
        other.el.stop();

        // 自己是 leader，但被问的是另一个 leader → 不是同一个 leader，不知道 → "不确定"
        const selfOther = makeElection({ onSendLeaderQueryAck: vi.fn() });
        selfOther.el.start();
        selfOther.el.triggerElection();
        await vi.advanceTimersByTimeAsync(1);
        expect(selfOther.el.isLeader()).toBe(true);
        selfOther.el.receiveLeaderQuery(9, 'asker', 'X');
        expect(selfOther.el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 9, 'asker', 'uncertain', 'X', 1);
        selfOther.el.stop();

        // 自己也正在问同一个 leader 的过程中 → "不确定"（还没有结论）
        const asking = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            onSendLeaderQueryAck: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        asking.el.start();
        asking.el.addNode('L');
        asking.el.addNode('B');
        asking.el.receiveHeartbeat(1, 'L');
        await vi.advanceTimersByTimeAsync(330); // 它自己的选举超时 → 问询进行中
        expect(asking.el._inquiry).not.toBeNull();
        asking.el.receiveLeaderQuery(9, 'asker', 'L');
        expect(asking.el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 9, 'asker', 'uncertain', 'L', 1);
        asking.el.stop();
    });

    it('"uncertain" does not count as online; once a majority of online is unreachable the node elects without asking further', async () => {
        const { el, callbacks } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('B');
        el.addNode('C');
        el.receiveHeartbeat(1, 'L', { relayed: true }); // 本节点视图 leader=L（问来的）
        await vi.advanceTimersByTimeAsync(330); // 问询开始，先问 B（被问者 2 人，过半 = 2）
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        el.receiveLeaderQueryAck(1, 'uncertain', 'L', 1, 'B'); // B 也在问的过程中 → 不算"在线"；只剩 C 一人，再问也凑不够 2 → 结论已定
        expect(el._inquiry).toBeNull();
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1); // C 不必再问
        expect(callbacks.onSendVoteReq).toHaveBeenCalled(); // → 直接发起投票
        expect(el.getStatus().state).toBe('candidate');
        el.stop();
    });

    it('"online, but I asked others" does not count as online either', async () => {
        const { el, callbacks } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('B');
        el.addNode('C');
        el.addNode('D');
        el.receiveHeartbeat(1, 'L', { relayed: true }); // 本节点视图 leader=L（问来的）
        await vi.advanceTimersByTimeAsync(330); // 问询开始，先问 B（被问者 3 人，过半 = 2）
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(1);
        el.receiveLeaderQueryAck(1, 'online_indirect', 'L', 1, 'B'); // B 回"在线，但是我是问的别人" → 不计票，问下一个
        expect(el._inquiry).not.toBeNull();
        expect(el._onSendLeaderQuery).toHaveBeenCalledTimes(2);
        expect(el._onSendLeaderQuery).toHaveBeenCalledWith(0, 1, 'C', 'L');
        el.receiveLeaderQueryAck(1, 'offline', 'L', 1, 'C'); // C 回不在线了 → 只剩 D，再问也凑不够 2 个明确"在线" → 直接选举
        expect(callbacks.onSendVoteReq).toHaveBeenCalled();
        expect(el.getStatus().state).toBe('candidate');
        el.stop();
    });

    it('a vote request received mid-inquiry stops the asking at once and the node joins the election', async () => {
        // 投出票的情况：同 term 的 vote_req 到达 → 停问并投票
        const grantedCase = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            onSendVoteAck: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        grantedCase.el.start();
        grantedCase.el.addNode('L');
        grantedCase.el.addNode('B');
        grantedCase.el.receiveHeartbeat(1, 'L');
        await vi.advanceTimersByTimeAsync(330); // 问询开始
        expect(grantedCase.el._inquiry).not.toBeNull();
        grantedCase.el.receiveVoteReq(1, 'B', 0); // 选举信息到达
        expect(grantedCase.el._inquiry).toBeNull(); // 立即停问
        expect(grantedCase.el._onSendVoteAck).toHaveBeenCalledWith(0, 1, 'B', true); // 参与选举：投票
        grantedCase.el.receiveLeaderQueryAck(1, 'offline', 'L', 1, 'L'); // 旧问询的迟到应答：作废，不推进
        await vi.advanceTimersByTimeAsync(100);
        expect(grantedCase.el._onSendLeaderQuery).toHaveBeenCalledTimes(1); // 不再继续问
        grantedCase.el.stop();

        // 没投出票的情况（本 term 已投更高优先级的他人）：同样停问，且选举计时器重新武装、不悬空
        const deniedCase = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn(),
            onSendVoteAck: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        deniedCase.el.start();
        deniedCase.el.addNode('L');
        deniedCase.el.addNode('B');
        deniedCase.el.receiveHeartbeat(1, 'L');
        deniedCase.el._votedFor.set(1, { candidateId: 'Z', priority: 99 }); // 本 term 已投 Z
        await vi.advanceTimersByTimeAsync(330); // 问询开始
        expect(deniedCase.el._inquiry).not.toBeNull();
        deniedCase.el.receiveVoteReq(1, 'B', 1); // 优先级更低 → 不投票
        expect(deniedCase.el._inquiry).toBeNull(); // 仍立即停问
        expect(deniedCase.el._onSendVoteAck).toHaveBeenCalledWith(0, 1, 'B', false);
        expect(deniedCase.el._electionTimer).not.toBeNull(); // 计时器已重新武装
        expect(deniedCase.el.getStatus().state).toBe('follower');
        deniedCase.el.stop();
    });

    it('once election info is received, the node stops answering leader queries until a new leader is established', async () => {
        const { el } = makeElection({
            getMinQuorum: () => 2,
            onSendLeaderQueryAck: vi.fn(),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        el.start();
        el.addNode('L');
        el.addNode('B');
        el.receiveHeartbeat(1, 'L'); // 视图 leader=L，直连心跳新鲜
        el.receiveLeaderQuery(9, 'asker', 'L'); // 正常时期：照常回明确"在线"
        expect(el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 9, 'asker', 'online', 'L', 1);
        el._onSendLeaderQueryAck.mockClear();
        el.receiveVoteReq(2, 'B', 0); // 收到选举信息（更高 term）→ 参与选举
        el.receiveLeaderQuery(10, 'asker', 'L'); // 被问旧 leader：不再作答
        expect(el._onSendLeaderQueryAck).not.toHaveBeenCalled();
        el.receiveHeartbeat(2, 'B'); // 新 leader 经心跳确立 → 恢复作答
        el.receiveLeaderQuery(11, 'asker', 'B');
        expect(el._onSendLeaderQueryAck).toHaveBeenCalledWith(0, 11, 'asker', 'online', 'B', 2);
        el.stop();
    });

    it('chain: a node that only heard it from asking answers "online, but I asked others", and the next asker does not stand down on it', async () => {
        // C 对 leader 在世的认知来自问询确认（relayed）；D 只连得上 C。
        const cAck = vi.fn((sid, qid, askerId, verdict, leaderId, term) => {
            d.el.receiveLeaderQueryAck(qid, verdict, leaderId, term, 'C');
        });
        const c = makeElection({
            nodeId: 'C',
            onSendLeaderQueryAck: cAck,
            // C 的选举计时器拉远，避免推进 D 的假时钟时 C 自己超时参选、冲掉 leader 视图
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 100000, maxElectionMs: 100000 },
        });
        c.el.start();
        c.el.addNode('peer');
        c.el.receiveHeartbeat(1, 'L', { relayed: true }); // C 的证据是问来的

        const d = makeElection({
            nodeId: 'D',
            getMinQuorum: () => 2,
            onSendLeaderQuery: vi.fn((sid, qid, target, targetLeaderId) => { c.el.receiveLeaderQuery(qid, 'D', targetLeaderId); }),
            _testOptions: { heartbeatIntervalMs: 100, minElectionMs: 300, maxElectionMs: 320 },
        });
        d.el.start();
        d.el.addNode('C');
        await vi.advanceTimersByTimeAsync(330); // D 超时 → 问 C
        expect(d.callbacks.onSendVoteReq).toHaveBeenCalled(); // C 回"在线，但是我是问的别人" → D 问完无人明确在线 → 直接选举
        expect(d.el.getStatus().state).toBe('candidate');
        // C 的应答确实只是中间结论
        expect(cAck).toHaveBeenCalledWith(0, 1, 'D', 'online_indirect', 'L', 1);
        d.el.stop();
        c.el.stop();
    });

    afterEach(() => { vi.useRealTimers(); });
});
