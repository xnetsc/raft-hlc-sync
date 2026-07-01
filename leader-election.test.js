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
