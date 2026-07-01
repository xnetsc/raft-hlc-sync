/**
 * Leader Election 状态机（Term-based，类 Raft 简化版）
 *
 * 纯算法实现，所有副作用通过 onXXX 回调通知外部。
 * 不依赖任何传输层或数据库。
 *
 * 状态机：
 *   follower ──(选举超时)──> candidate ──(获得多数票)──> leader
 *      ^                         |                         |
 *      └──(发现更高 term)─────────┘ <────(发现更高 term)────┘
 *
 * 核心规则：
 *   1. 每个节点在每个 term 内只投一票（先到先得，高优先级可改投）
 *   2. 获得票数 >= quorum 则成为 Leader
 *   3. Leader 每 HEARTBEAT_INTERVAL 广播心跳，Follower 收到心跳后重置选举计时器
 *   4. 选举超时随机化，避免多节点同时选举
 *   5. 收到更高 term 的消息时，立即降级为 Follower
 *
 * 网络感知优化（需外部注入回调）：
 *   - getPriority()      — 返回 [0,1) 的候选优先级，越高越容易赢得选票。
 *                          建议基于双向对时 RTT 计算（RTT 越小 → 网络位置越居中 → 优先级越高）。
 *                          不注入时回退到 Math.random()。
 *   - getNetworkRttMs()  — 返回当前与所有 peer 中最小的往返时延（ms），或 null。
 *                          引擎用此值动态调整选举超时：RTT 小时缩短超时（加速收敛），
 *                          RTT 大时延长超时（防止误判）。不注入时使用默认固定范围。
 *   - getEpsilonMs()     — 返回所有 peer 中最大的时钟误差上界 ε（ms），或 Infinity。
 *                          用于计算 Leader 租约时长：leaseDuration = electionMin - ε。
 *                          不注入时 ε 视为 0（租约时长 = electionMin）。
 *   - getNowMs()         — 返回当前本地时间（ms）。
 *                          用于租约有效性判断。不注入时使用 Date.now()。
 *
 * Leader 租约（Leader Lease）：
 *   Leader 每次发出心跳时刷新租约，租约时长 = electionMin - ε。
 *   在租约有效期内，Leader 可保证自己是唯一 Leader：
 *     因为 Follower 的选举超时 ≥ electionMin，而 Leader 记录的 leaseDuration = electionMin - ε，
 *     所以在 leaseDuration 内不可能有 Follower 发起选举。
 *   外部代码可通过 isLeaseValid() 判断当前是否处于租约期内，
 *   从而实现本地读（不走 quorum），降低读延迟。
 *
 * 外部需注册的回调接口：
 *   onSendHeartbeat(shardId, term, leaderId)
 *   onSendVoteReq(shardId, term, candidateId, priority)
 *   onSendVoteAck(shardId, term, candidateId, granted)
 *   onLeaderChanged(shardId, leaderNodeId, isLocal)
 *
 * 定时器注入（timerAPI，必须）：
 *   算法内部定时器（心跳 interval、选举超时）通过注入的 timerAPI 调用，
 *   不使用全局 setTimeout/setInterval。timerAPI 必须显式提供，缺失时构造函数抛出错误。
 */

const HEARTBEAT_INTERVAL_MS = 1000;
const MIN_ELECTION_MS = 3000;
const MAX_ELECTION_MS = 6000;

export class LeaderElection {
    /**
     * @param {object} opts
     * @param {number} opts.shardId - 本实例负责的分片 ID
     * @param {string} opts.nodeId - 本节点唯一 ID
     * @param {function(): number|Promise<number>} opts.getMinQuorum
     *   返回当前生效的最少票数（含自身）。每次 quorum 判断时实时调用。
     * @param {function(number, number, string): void} opts.onSendHeartbeat
     * @param {function(number, number, string, number): void} opts.onSendVoteReq
     * @param {function(number, number, string, boolean): void} opts.onSendVoteAck
     * @param {function(number, string|null, boolean): void} opts.onLeaderChanged
     * @param {object} opts.timerAPI - 定时器函数注入，必须提供，缺失时构造函数抛出错误
     * @param {function} opts.timerAPI.setTimeout
     * @param {function} opts.timerAPI.clearTimeout
     * @param {function} opts.timerAPI.setInterval
     * @param {function} opts.timerAPI.clearInterval
     * @param {object} [opts._testOptions] - 测试用：覆盖定时参数
     */
    constructor({ shardId, nodeId, getMinQuorum, onSendHeartbeat, onSendVoteReq, onSendVoteAck, onLeaderChanged, timerAPI, getPriority, getNetworkRttMs, getEpsilonMs, getNowMs, _testOptions = {} }) {
        this.shardId = shardId;
        this.nodeId = nodeId;
        this._getMinQuorum = getMinQuorum;
        this._heartbeatIntervalMs = _testOptions.heartbeatIntervalMs ?? HEARTBEAT_INTERVAL_MS;
        this._minElectionMs     = _testOptions.minElectionMs     ?? MIN_ELECTION_MS;
        this._maxElectionMs     = _testOptions.maxElectionMs     ?? MAX_ELECTION_MS;

        if (!timerAPI || typeof timerAPI.setTimeout !== 'function' || typeof timerAPI.clearTimeout !== 'function' ||
            typeof timerAPI.setInterval !== 'function' || typeof timerAPI.clearInterval !== 'function') {
            throw new Error(
                'LeaderElection: timerAPI is required and must provide setTimeout, clearTimeout, setInterval, clearInterval.'
            );
        }
        this._setTimeout   = timerAPI.setTimeout;
        this._clearTimeout = timerAPI.clearTimeout;
        this._setInterval  = timerAPI.setInterval;
        this._clearInterval = timerAPI.clearInterval;

        this._onSendHeartbeat = onSendHeartbeat;
        this._onSendVoteReq = onSendVoteReq;
        this._onSendVoteAck = onSendVoteAck;
        this._onLeaderChanged = onLeaderChanged;

        /**
         * 候选优先级来源（可选注入）。
         * 返回 [0,1) 的数值，越高越容易获得选票（高优先级可覆盖对方已投出的票）。
         * 建议由 SyncEngine 注入基于 RTT 的计算，不注入时回退到纯随机。
         */
        this._getPriority = typeof getPriority === 'function' ? getPriority : () => Math.random();

        /**
         * 网络 RTT 来源（可选注入）。
         * 返回当前所有 peer 中最小的往返时延（ms），或 null（无数据时）。
         * 用于动态调整选举超时范围。
         */
        this._getNetworkRttMs = typeof getNetworkRttMs === 'function' ? getNetworkRttMs : () => null;

        /**
         * 时钟误差上界来源（可选注入）。
         * 返回所有 peer 中最大的 ε（ms）；不注入时视为 0。
         * ε 越小，租约越长（更激进）；ε 越大，租约越短（更保守）。
         */
        this._getEpsilonMs = typeof getEpsilonMs === 'function' ? getEpsilonMs : () => 0;

        /**
         * 本地时钟来源（可选注入，默认 Date.now）。
         * 用于租约有效性判断，应与 SyncEngine 的 _nowMs 保持一致。
         */
        this._getNowMs = typeof getNowMs === 'function' ? getNowMs : () => Date.now();

        // ===== 租约状态 =====
        /** 上次刷新租约的本地时刻（ms） */
        this._leaseStart = 0;
        /** 当前租约时长（ms）；0 表示无有效租约 */
        this._leaseDurationMs = 0;

        this._state = 'follower';
        this._currentTerm = 0;
        this._votedFor = new Map();
        this._currentLeaderNodeId = null;
        this._votesGranted = new Set();

        this._cachedQuorum = 1;
        this._connectedNodeIds = new Set();

        this._electionTimer = null;
        this._heartbeatTimer = null;
        this._started = false;
    }

    // ========== 生命周期 ==========

    start() {
        if (this._started) return;
        this._started = true;
        this._refreshQuorumCache();
        this._resetElectionTimer();
    }

    stop() {
        this._started = false;
        this._clearElectionTimer();
        this._clearHeartbeatTimer();
    }

    /**
     * 立即触发一次选举，走标准选举流程。
     * 若当前是 Leader，先正常降级（触发 onLeaderChanged(null)），再参与竞选。
     * 若已是 Follower/Candidate，直接发起选举。
     */
    triggerElection() {
        if (!this._started) return;
        if (this._state === 'leader') this._stepDown(this._currentTerm);
        this._clearElectionTimer();
        this._startElection();
    }

    // ========== 节点集合管理 ==========

    addNode(nodeId) {
        if (nodeId === this.nodeId) return;
        this._connectedNodeIds.add(nodeId);
    }

    removeNode(nodeId) {
        if (nodeId === this.nodeId) return;
        this._connectedNodeIds.delete(nodeId);
        if (nodeId === this._currentLeaderNodeId) {
            this._setLeader(null);
        }
    }

    // ========== 消息接收 ==========

    receiveHeartbeat(term, leaderId) {
        if (!this._started) return;
        if (term > this._currentTerm) this._stepDown(term);
        if (term < this._currentTerm) return;

        if (leaderId !== this.nodeId && !this._connectedNodeIds.has(leaderId)) return;

        if (this._state !== 'leader') this._resetElectionTimer();
        if (this._currentLeaderNodeId !== leaderId) this._setLeader(leaderId);
    }

    receiveVoteReq(term, candidateId, priority) {
        if (!this._started) return;
        if (term > this._currentTerm) this._stepDown(term);

        let granted = false;
        if (term >= this._currentTerm) {
            const current = this._votedFor.get(term);
            if (!current) {
                granted = true;
            } else if (current.candidateId === candidateId) {
                granted = true;
            } else if (priority > current.priority) {
                granted = true;
            }
            if (granted) {
                this._votedFor.set(term, { candidateId, priority });
                this._resetElectionTimer();
            }
        }

        this._onSendVoteAck(this.shardId, term, candidateId, granted);
    }

    receiveVoteAck(term, candidateId, granted, voterId) {
        if (!this._started) return;
        if (candidateId !== this.nodeId || term !== this._currentTerm || this._state !== 'candidate') return;

        if (granted) {
            this._votesGranted.add(voterId);
            if (this._votesGranted.size >= this._quorum()) {
                this._becomeLeader();
            }
        }
    }

    // ========== 查询 ==========

    isLeader() { return this._state === 'leader'; }

    getLeaderNodeId() { return this._currentLeaderNodeId; }

    getStatus() {
        return {
            shardId: this.shardId,
            state: this._state,
            term: this._currentTerm,
            leaderNodeId: this._currentLeaderNodeId,
            currentQuorum: this._quorum(),
            connectedNodes: this._connectedNodeIds.size + 1,
        };
    }

    // ========== 内部状态转换 ==========

    async _refreshQuorumCache() {
        try {
            const v = await this._getMinQuorum();
            if (!isNaN(v) && v >= 1) this._cachedQuorum = v;
        } catch (_) { /* keep cached */ }
    }

    _quorum() {
        const configured = Math.max(1, this._cachedQuorum);
        const totalNodes = this._connectedNodeIds.size + 1;
        const majority = Math.floor(totalNodes / 2) + 1;
        return Math.max(configured, majority);
    }

    _stepDown(newTerm) {
        const wasLeader = this._state === 'leader';
        this._state = 'follower';
        this._currentTerm = newTerm;
        this._votesGranted.clear();
        this._clearHeartbeatTimer();
        this._leaseDurationMs = 0; // 降级时立即清空租约
        this._resetElectionTimer();
        if (wasLeader) this._setLeader(null);
        for (const t of this._votedFor.keys()) {
            if (t < newTerm) this._votedFor.delete(t);
        }
    }

    _startElection() {
        if (!this._started) return;
        // 连通性闸门：若本地可见的 peer 不足 quorum，压根不该参选。
        //
        // 原因：若此时进入 candidate 并广播 vote_req，对端 peer 可能仍然把本节点
        // 视为连通并投赞成票（它们的 _connectedNodeIds 和本节点不一致），本节点
        // 会以"我看不见的节点的票"当选；接下来 heartbeat tick 第一次触发 quorum 检查
        // 时又立刻 _stepDown，形成 follower → candidate → leader → follower 震荡。
        // 在候选阶段前就拒绝参选，让节点老老实实当 follower，直到 peer 重连。
        const connected = this._connectedNodeIds.size + 1;
        if (connected < this._quorum()) {
            this._resetElectionTimer();  // 稍后再评估，避免丢失重试机会
            return;
        }
        this._currentTerm++;
        this._state = 'candidate';
        this._currentLeaderNodeId = null;
        this._votesGranted.clear();

        this._electionPriority = this._getPriority();
        this._votedFor.set(this._currentTerm, { candidateId: this.nodeId, priority: this._electionPriority });
        this._votesGranted.add(this.nodeId);

        this._resetElectionTimer();
        this._onSendVoteReq(this.shardId, this._currentTerm, this.nodeId, this._electionPriority);

        // 延迟一个 tick 再检查初始 quorum：
        //   单节点 quorum=1 时，确保调用者先观察到 'candidate' 状态（如 triggerElection 后的断言），
        //   再变为 leader。多节点时此处通常不满足 quorum，实际当选由 receiveVoteAck 驱动，此延迟无影响。
        this._setTimeout(() => {
            if (this._started && this._state === 'candidate' && this._votesGranted.size >= this._quorum()) {
                this._becomeLeader();
            }
        }, 0);
    }

    _becomeLeader() {
        this._state = 'leader';
        this._clearElectionTimer();
        this._setLeader(this.nodeId);

        this._refreshLease(); // 当选时立即建立初始租约
        this._onSendHeartbeat(this.shardId, this._currentTerm, this.nodeId);
        this._heartbeatTimer = this._setInterval(() => {
            if (!this._started || this._state !== 'leader') return;
            const connected = this._connectedNodeIds.size + 1;
            if (connected < this._quorum()) {
                this._stepDown(this._currentTerm);
                return;
            }
            this._refreshLease(); // 每次发心跳前先刷新租约
            this._onSendHeartbeat(this.shardId, this._currentTerm, this.nodeId);
        }, this._heartbeatIntervalMs);
    }

    _setLeader(leaderNodeId) {
        const changed = this._currentLeaderNodeId !== leaderNodeId;
        this._currentLeaderNodeId = leaderNodeId;
        if (changed) {
            const isLocal = leaderNodeId === this.nodeId;
            this._onLeaderChanged(this.shardId, leaderNodeId, isLocal);
        }
    }

    // ========== 租约 ==========

    /**
     * 判断当前 Leader 租约是否有效。
     *
     * 有效条件：
     *   1. 当前是 Leader
     *   2. 距上次心跳（租约刷新）的时间 < leaseDuration
     *
     * 租约有效期内可安全做本地读（不走 quorum），因为此期间不可能有其他节点完成选举。
     *
     * @returns {boolean}
     */
    isLeaseValid() {
        if (this._state !== 'leader') return false;
        if (this._leaseDurationMs <= 0) return false;
        return this._getNowMs() < this._leaseStart + this._leaseDurationMs;
    }

    /**
     * 刷新租约（Leader 每次发出心跳前调用）。
     *
     * 租约时长计算：
     *   leaseDuration = electionMin - ε
     *
     *   electionMin  — Follower 最短选举超时（= 心跳发出后 Follower 最早可能发起选举的时间）
     *   ε            — 时钟误差上界（来自双向对时，等于所有 peer 中最大的 epsilon）
     *
     * 安全性证明（简化版）：
     *   Leader 在本地时刻 T 发出心跳。
     *   Follower 在实际时刻 T + d_net（d_net = 单程网络延迟 ≥ 0）收到，并重置选举计时器。
     *   Follower 最早在实际时刻 T + d_net + electionMin ≥ T + electionMin 发起选举。
     *   Leader 的租约在 T + (electionMin - ε) 到期，ε 作为安全边距防止计时器精度误差。
     *   因此 leaseDuration 内不可能有新 Leader 产生。
     *
     * @private
     */
    _refreshLease() {
        const epsilon = this._getEpsilonMs();
        const electionMin = this._computeElectionMin();
        this._leaseDurationMs = Math.max(0, electionMin - epsilon);
        this._leaseStart = this._getNowMs();
    }

    // ========== 定时器 ==========

    /**
     * 计算当前动态选举超时的下界（electionMin）。
     * 同时被 _resetElectionTimer 和 _refreshLease 使用，保持一致性。
     * @private
     * @returns {number} ms
     */
    _computeElectionMin() {
        const rtt = this._getNetworkRttMs();
        if (rtt !== null && rtt > 0 && isFinite(rtt)) {
            return Math.round(Math.max(this._heartbeatIntervalMs * 1.5, rtt * 20));
        }
        return this._minElectionMs;
    }

    _resetElectionTimer() {
        this._clearElectionTimer();
        if (!this._started || this._state === 'leader') return;

        // 基于网络 RTT 动态调整选举超时范围（与 _computeElectionMin 保持一致）：
        //   局域网 rtt≈1ms  → [1500, 3000]ms（较默认 [3000,6000]ms 缩短约一半）
        //   同城   rtt≈20ms → [1500, 3000]ms（心跳下限生效）
        //   跨地域 rtt≈200ms→ [4000, 8000]ms（延长，防止高延迟下误判）
        const rtt = this._getNetworkRttMs();
        let min = this._minElectionMs;
        let max = this._maxElectionMs;
        if (rtt !== null && rtt > 0 && isFinite(rtt)) {
            min = Math.round(Math.max(this._heartbeatIntervalMs * 1.5, rtt * 20));
            max = Math.round(Math.max(this._heartbeatIntervalMs * 3,   rtt * 40));
            max = Math.max(max, min + 100);
        }

        const delay = min + Math.floor(Math.random() * (max - min));
        this._electionTimer = this._setTimeout(async () => {
            if (!this._started || this._state === 'leader') return;
            await this._refreshQuorumCache();
            if (this._started && this._state !== 'leader') this._startElection();
        }, delay);
    }

    _clearElectionTimer() {
        if (this._electionTimer) { this._clearTimeout(this._electionTimer); this._electionTimer = null; }
    }

    _clearHeartbeatTimer() {
        if (this._heartbeatTimer) { this._clearInterval(this._heartbeatTimer); this._heartbeatTimer = null; }
    }
}
