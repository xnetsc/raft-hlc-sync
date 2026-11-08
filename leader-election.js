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
 * 存活问询（Leader Inquiry，防误判扰动）：
 *   选举超时后不直接参选，先逐个问直连同组节点"现任 leader 还在不在"
 *   （顺序问询，一次一问）。问时带上目标 leader 的 id——"我们共同的
 *   leader 是 X，X 还在不在"；被问方只就 X 作答，结论必须来自自己的
 *   直连证据：只有自己就是 X、或近期收到过 X 的直连心跳，才回明确
 *   "在线"；若它对 X 在世的认知也是问别人问来的，只能回中间结论
 *   "在线，但是我是问的别人"，不许把二手结论放大成明确结论；若它
 *   自己也正在问的过程中（还没有结论），回"不确定"。回复一共四种：
 *   在线 / 在线，但是我是问的别人 / 不在线了 / 不确定。收结论方只认
 *   第一种：任一节点给出明确"在线"即停问、刷新 leader 存活时间，
 *   本轮结束——单个节点连不上 leader、但同组节点仍能听到 leader 时，
 *   它涨不了 term、全网不换届；其余三种回复与超时未回一律不算数，
 *   全部问完仍无明确"在线"就直接进入正式选举，不再另设预投票一轮。
 *   问询期间直连恢复（leader 心跳到达，或原 leader 的传输连接重连）
 *   同样即时停问。
 *
 * 外部需注册的回调接口：
 *   onSendHeartbeat(shardId, term, leaderId)
 *   onSendVoteReq(shardId, term, candidateId, priority)
 *   onSendVoteAck(shardId, term, candidateId, granted)
 *   onSendLeaderQuery(shardId, queryId, targetNodeId, targetLeaderId)（可选，缺省不发问询）
 *   onSendLeaderQueryAck(shardId, queryId, askerId, verdict, leaderId, term)（可选；verdict ∈ 'online'|'online_indirect'|'offline'|'uncertain'）
 *   onLeaderChanged(shardId, leaderNodeId, isLocal)
 *
 * 定时器注入（timerAPI，必须）：
 *   算法内部定时器（心跳 interval、选举超时）通过注入的 timerAPI 调用，
 *   不使用全局 setTimeout/setInterval。timerAPI 必须显式提供，缺失时构造函数抛出错误。
 */

const HEARTBEAT_INTERVAL_MS = 1000;
const MIN_ELECTION_MS = 3000;
const MAX_ELECTION_MS = 6000;
/** 存活问询无 RTT 数据时的回退超时（ms）；有数据时按近期典型 RTT + buffer 动态计算 */
const QUERY_FALLBACK_TIMEOUT_MS = 1000;

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
    constructor({ shardId, nodeId, getMinQuorum, onSendHeartbeat, onSendVoteReq, onSendVoteAck, onSendLeaderQuery, onSendLeaderQueryAck, onLeaderChanged, timerAPI, getPriority, getNetworkRttMs, getPeerRttMs, getEpsilonMs, getNowMs, _testOptions = {} }) {
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
        // 存活问询回调可选：未注入时问询发不出去，各目标依次超时问完后直接选举
        this._onSendLeaderQuery = typeof onSendLeaderQuery === 'function' ? onSendLeaderQuery : () => {};
        this._onSendLeaderQueryAck = typeof onSendLeaderQueryAck === 'function' ? onSendLeaderQueryAck : () => {};
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
         * 单个 peer 的 RTT 来源（可选注入），参数为目标 nodeId。
         * 用于存活问询的动态应答超时；不注入时问询超时用回退值。
         */
        this._getPeerRttMs = typeof getPeerRttMs === 'function' ? getPeerRttMs : () => null;

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
        /** 最近一次已知的 leader：视图被断连清空后仍保留，供识别其直连重连 */
        this._lastKnownLeaderNodeId = null;
        this._votesGranted = new Set();

        // ===== 存活证据与问询状态 =====
        /** 上次收到现任 leader 直连心跳的本地时刻（ms） */
        this._lastLeaderHeartbeatAt = 0;
        /** 上次经存活问询确认现任 leader 在世的时刻（ms）：一次性证据 */
        this._lastRelayedHeartbeatAt = 0;
        /** 进行中的存活问询：{ queryId, queue:Array, current, timer, targetLeaderId } | null（顺序问询，一次一问） */
        this._inquiry = null;
        /** 问询编号计数（本实例内单调递增，应答按编号对号） */
        this._inquirySeq = 0;

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
        this._cancelInquiry();
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
        // 问询期间，原 leader 的直连恢复了：此刻就停问，回到等待
        // 它的心跳确认。断连时 leader 视图已被清空，若只靠下一次心跳到达
        // 才取消（receiveHeartbeat 的路径），问询队列可能在一个心跳间隔
        // 内问完并升级到选举——来不及。重连本身不算存活证据：
        // 不恢复 leader 视图、不刷新存活时间戳，只重置选举计时器；若它
        // 已不再是 leader，无心跳跟进，计时器到期后重新问询。
        if (nodeId === this._lastKnownLeaderNodeId && this._inquiry) {
            this._cancelInquiry();
            if (this._state !== 'leader') this._resetElectionTimer();
        }
    }

    removeNode(nodeId) {
        if (nodeId === this.nodeId) return;
        this._connectedNodeIds.delete(nodeId);
        if (nodeId === this._currentLeaderNodeId) {
            this._setLeader(null);
        }
    }

    // ========== 消息接收 ==========

    /**
     * 收到 leader 存活证据。直连心跳（opts.relayed 缺省）来自 leader
     * 本人；relayed 证据来自存活问询的应答——同组节点以第一手证据确认
     * leader 在世（一次性事实，无持续来源，不构成对传递的长期依赖）。
     * 两者同样更新 leader 视图与存活时间戳。relayed 证据声称 leader 是
     * 本节点时直接忽略（自己不可能向自己证明在世）。
     */
    receiveHeartbeat(term, leaderId, opts = {}) {
        if (!this._started) return;
        if (opts.relayed && leaderId === this.nodeId) return;
        if (term > this._currentTerm) this._stepDown(term);
        if (term < this._currentTerm) return;

        if (!opts.relayed && leaderId !== this.nodeId && !this._connectedNodeIds.has(leaderId)) return;

        if (this._currentLeaderNodeId !== leaderId) this._setLeader(leaderId);
        const now = this._getNowMs();
        // relayed 证据只来自存活问询的应答（一次性事实，无持续来源，
        // 不会形成对传递的永久依赖）：与直连证据一样刷新存活时间戳
        if (opts.relayed) this._lastRelayedHeartbeatAt = now;
        else this._lastLeaderHeartbeatAt = now;

        // leader 确实活着：问询失去意义，重置计时器
        this._cancelInquiry();
        if (this._state !== 'leader') this._resetElectionTimer();
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

    // ========== 存活问询（Leader Inquiry） ==========

    /**
     * 收到存活问询：问方带了目标 leader 的 id——"我们共同的 leader 是
     * X，X 还在不在"。只就 X 作答，不改变本节点任何状态；问方没点名
     * （它自己也没有 leader 视图）时，就答本节点所知的 leader。
     * 回复只有三种，且必须来自本节点自己的直连结论：
     *   'online'（在线）——本节点就是 X，或在最小选举超时内收到过
     *     X 的直连心跳：据直连心跳给的结论；
     *   'online_indirect'（在线，但是我是问的别人）——本节点对 X 在世
     *     的最新证据不是直连得来的，而是自己问别人问来的（问询确认）：
     *     只报这个中间结论，不许放大成明确"在线"；
     *   'offline'（不在线了）——按本节点的直连观察，X 不在线：没有
     *     新鲜直连证据，或本节点的 leader 根本不是 X（本节点收到的
     *     直连心跳来自另一个 leader，对 X 无话可说）；
     *   'uncertain'（不确定）——本节点自己也正在问 X 的过程中，还没有
     *     结论：既不能说它在线，也不能说它不在线。
     * 应答的 leaderId 就是所问的那个 leader（subject）。
     */
    receiveLeaderQuery(queryId, askerId, targetLeaderId) {
        if (!this._started) return;
        const subject = targetLeaderId ?? this._currentLeaderNodeId;
        let verdict = 'offline';
        if (subject !== null) {
            const now = this._getNowMs();
            if (this._state === 'leader' && subject === this.nodeId) {
                verdict = 'online'; // 问的就是我，我在
            } else if (this._currentLeaderNodeId === subject &&
                       now - this._lastLeaderHeartbeatAt < this._computeElectionMin()) {
                verdict = 'online'; // 据我直连收到的心跳，它在
            } else if (this._inquiry && this._inquiry.targetLeaderId === subject) {
                verdict = 'uncertain'; // 我自己也正在问它，还没有结论
            } else if (this._currentLeaderNodeId === subject &&
                       now - this._lastRelayedHeartbeatAt < this._computeElectionMin()) {
                verdict = 'online_indirect'; // 在线，但是我是问的别人
            }
        } else if (this._inquiry) {
            verdict = 'uncertain'; // 没点名、我也没有 leader 视图，而我正在问
        }
        this._onSendLeaderQueryAck(this.shardId, queryId, askerId, verdict, subject, this._currentTerm);
    }

    /**
     * 收到问询应答。收结论的规矩只有一条：必须收到明确的"在线"
     *（verdict='online'，且所指 leader 就是本节点所问的那个——或本
     * 节点尚无 leader 视图、或对方 term 更高），才终结问询、保持正常，
     * 按该证据刷新存活时间（注意：本节点由此得到的证据是问来的，之后
     * 被别人问到时只能报"在线，但是我是问的别人"）。'online_indirect'、
     * 'offline'、'uncertain' 一律不算明确结论：当前被问者如此回答就问下一个；
     * 迟到的明确"在线"同样作数。全部问完仍无明确"在线" → 直接正式
     * 选举。
     */
    receiveLeaderQueryAck(queryId, verdict, leaderId, term, responderId) {
        if (!this._started) return;
        const inq = this._inquiry;
        if (!inq || queryId !== inq.queryId) return;
        if (verdict === 'online' && leaderId) {
            const decisive = this._currentLeaderNodeId === null ||
                leaderId === this._currentLeaderNodeId || term > this._currentTerm;
            if (decisive) {
                this._cancelInquiry();
                this.receiveHeartbeat(term, leaderId, { relayed: true });
                if (this._state !== 'leader') this._resetElectionTimer();
                return;
            }
        }
        if (responderId === inq.current) this._askNext();
    }

    /**
     * 心跳超时后的入口：逐个问直连节点 leader 在不在。
     * 顺序问询：一次只问一个；对方答非明确"在线"、或在动态超时内没回，
     * 才问下一个；出现决定性确认立即停问。全部问完无人能确认，
     * 直接正式选举。任何时刻至多一问在途，不产生并发泛洪。
     * 问询期间直连恢复同样即时停问：leader 心跳到达（receiveHeartbeat），
     * 或原 leader 的传输连接重连（addNode）——都不必等问完。
     */
    _startInquiry() {
        if (!this._started || this._state === 'leader' || this._inquiry) return;
        const queue = [...this._connectedNodeIds];
        if (queue.length === 0) { this._startElection(); return; }
        const queryId = ++this._inquirySeq;
        // 问时带上目标 leader 的 id：当前视图里的 leader；视图已被断连
        // 清空时，就是最近一次已知的那个（问的正是它还在不在）。
        // 从来没有过 leader 时为 null，被问方改答它所知的 leader。
        const targetLeaderId = this._currentLeaderNodeId ?? this._lastKnownLeaderNodeId;
        this._inquiry = { queryId, queue, current: null, timer: null, targetLeaderId };
        this._askNext();
    }

    _askNext() {
        const inq = this._inquiry;
        if (!inq) return;
        if (inq.timer) { this._clearTimeout(inq.timer); inq.timer = null; }
        const target = inq.queue.shift();
        if (target === undefined) {
            // 全部问完，无人能确认 leader 在世 → 直接正式选举
            this._inquiry = null;
            this._startElection();
            return;
        }
        inq.current = target;
        this._onSendLeaderQuery(this.shardId, inq.queryId, target, inq.targetLeaderId);
        // 同步投递时应答可能已推进/结束了问询，此时不再设定时器
        if (this._inquiry === inq && inq.current === target) {
            inq.timer = this._setTimeout(() => this._onAskTimeout(), this._askTimeoutMs(target));
        }
    }

    /**
     * 单次问询的应答超时（动态）：以与该 peer 近期通信的典型 RTT 为基准
     * ——前几次多快，这次大概率还是多快——再加适当 buffer：
     * timeout = min(1.5 × RTT + 50ms, 最小选举超时)；极小 RTT 时由 buffer
     * 兜底出下限。无 RTT 数据时用 QUERY_FALLBACK_TIMEOUT_MS（同样封顶）。
     */
    _askTimeoutMs(targetNodeId) {
        const cap = this._computeElectionMin();
        const rtt = this._getPeerRttMs(targetNodeId);
        if (rtt !== null && rtt > 0 && isFinite(rtt)) {
            return Math.min(Math.round(rtt * 1.5) + 50, cap);
        }
        return Math.min(QUERY_FALLBACK_TIMEOUT_MS, cap);
    }

    _onAskTimeout() {
        // 被问者超时没回 → 问下一个
        this._askNext();
    }

    _cancelInquiry() {
        if (!this._inquiry) return;
        if (this._inquiry.timer) this._clearTimeout(this._inquiry.timer);
        this._inquiry = null;
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
        this._cancelInquiry();
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
        this._cancelInquiry();
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
            if (leaderNodeId !== null) this._lastKnownLeaderNodeId = leaderNodeId;
            // leader 变更：旧 leader 的存活证据作废，新 leader 重新积累
            this._lastLeaderHeartbeatAt = 0;
            this._lastRelayedHeartbeatAt = 0;
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
            if (this._started && this._state !== 'leader') this._startInquiry();
        }, delay);
    }

    _clearElectionTimer() {
        if (this._electionTimer) { this._clearTimeout(this._electionTimer); this._electionTimer = null; }
    }

    _clearHeartbeatTimer() {
        if (this._heartbeatTimer) { this._clearInterval(this._heartbeatTimer); this._heartbeatTimer = null; }
    }
}
