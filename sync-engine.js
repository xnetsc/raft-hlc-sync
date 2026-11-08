/**
 * SyncEngine — 通用分布式同步引擎（纯算法模块）
 *
 * ═══════════════════════════════════════════════════════════════
 *  设计原则
 * ═══════════════════════════════════════════════════════════════
 *
 *  本模块是一个纯算法/状态机引擎：
 *    - 不依赖任何具体的通信传输模块（WebSocket、gRPC 等）
 *    - 不依赖任何具体的数据库模块（better-sqlite3、pg 等）
 *    - 不依赖 Node.js 特有 API
 *    - 不直接调用全局 setTimeout/setInterval，所有定时器函数通过 timerAPI 注入
 *
 *  所有外部能力通过构造函数注入：
 *    1. dialect           — 预定义 SQL 方言（'sqlite' | 'postgresql' | 'mysql'），
 *                           自动填充 DatabaseAdapter 上的事务、upsert、schema、触发器方法。
 *                           不指定时需手动在 db 上实现全部方言方法，否则报错。
 *    2. DatabaseAdapter   — 数据库操作（基础查询：run/get/all/exec）
 *    3. onXXX 回调        — 向外部通知事件 / 请求外部执行动作
 *    4. timerAPI          — 定时器函数注入（必须）：setTimeout/clearTimeout/setInterval/clearInterval
 *
 *  周期性任务由引擎在 start() 时内部自动调度（通过注入的 timerAPI.setInterval）：
 *    - tickPull()       — 从所有 peer 拉取数据（间隔：pullIntervalMs，默认 10s）
 *    - tickCleanup()    — 清理过期 sync_log/墓碑（间隔：cleanupIntervalMs，默认 1h）
 *    - tickHeartbeat()  — 检查心跳超时 + 发送 ping（间隔：heartbeatIntervalMs，默认 15s）
 *
 * ═══════════════════════════════════════════════════════════════
 *  外部需实现并注入的接口
 * ═══════════════════════════════════════════════════════════════
 *
 *  ┌────────────────────────────────────────────────────────────┐
 *  │ DatabaseAdapter（同步方法，外部根据目标数据库实现）          │
 *  ├────────────────────────────────────────────────────────────┤
 *  │  db.run(sql, params)  → { changes: number }               │
 *  │  db.get(sql, params)  → Object | null                     │
 *  │  db.all(sql, params)  → Object[]                          │
 *  │  db.exec(sql)         → void                              │
 *  │                                                            │
 *  │  params 使用 ? 占位符数组。                                 │
 *  │  若目标 DB 使用 $1/$2（如 PostgreSQL），                    │
 *  │  请在 adapter 内部转译。                                    │
 *  └────────────────────────────────────────────────────────────┘
 *
 *  ┌────────────────────────────────────────────────────────────┐
 *  │ 回调接口（构造函数注入）                                     │
 *  ├────────────────────────────────────────────────────────────┤
 *  │                                                            │
 *  │  【必须】                                                   │
 *  │  onSendToPeer(peerId, message)                             │
 *  │    向指定 peer 发送字符串消息。                               │
 *  │    peerId 由外部传输层在 peerConnected() 时提供。            │
 *  │                                                            │
 *  │  onClosePeer(peerId, reason)                               │
 *  │    通知外部关闭与指定 peer 的连接。                           │
 *  │    reason: 'dedup' | 'heartbeat_timeout' | ...             │
 *  │                                                            │
 *  │  onLeaderChanged(shardId, leaderNodeId, isLocal)           │
 *  │    Leader 变更通知。isLocal 表示当前节点是否为新 Leader。     │
 *  │                                                            │
 *  │  【可选】                                                   │
 *  │  onWriteCompleted()                                        │
 *  │    远程数据同步应用完成后 / 手动事务提交后的通知。             │
 *  │    典型用途：触发 push-on-write。                            │
 *  │                                                            │
 *  │  onError(context, error)                                   │
 *  │    错误通知。context 是出错的位置字符串。                     │
 *  │                                                            │
 *  │  onExecuteProxyRequest(payload) → Promise<object>          │
 *  │    Leader 收到 Follower 转发的写请求后的执行回调。            │
 *  │    payload / response 格式由外部业务自行定义，对模块透明。    │
 *  │                                                            │
 *  │    ⚠️  多节点部署时必须注册：节点无法控制自己是否当选 Leader，  │
 *  │    若当选后未注册此回调，Follower 转发来的请求将返回错误。     │
 *  │                                                            │
 *  └────────────────────────────────────────────────────────────┘
 *
 * ═══════════════════════════════════════════════════════════════
 *  使用流程
 * ═══════════════════════════════════════════════════════════════
 *
 *  // 1. 创建引擎
 *  const engine = new SyncEngine({
 *      nodeId: 'node-1',
 *      db: myDatabaseAdapter,
 *      getNow: () => ({ value: Date.now(), unit: 'ms' }),  // 时间函数必须外部注入
 *      onSendToPeer: (peerId, msg) => wsSockets.get(peerId).send(msg),
 *      onClosePeer:  (peerId, reason) => wsSockets.get(peerId).close(),
 *      onLeaderChanged: (shardId, leaderId, isLocal) => { ... },
 *      onWriteCompleted: () => engine.notifyLocalWrite(),
 *      onError: (ctx, err) => console.error(ctx, err),
 *      heartbeatTimeoutMs: 45000,  // 所有配置由外部传入
 *  });
 *
 *  // 2. 注册业务表
 *  engine.registerTable('accounts', {
 *      keyColumns:  ['user_id'],
 *      dataColumns: ['user_id', 'created_at', '_hlc'],
 *  });
 *
 *  // 3. 初始化
 *  engine.initSchema();    // 创建 _sync_log 等基础设施表
 *  engine.initTriggers();  // 创建同步触发器（由 dialect 决定方言）
 *  engine.start();         // 启动 Leader Election
 *
 *  // 4. 外部传输层事件
 *  ws.on('open',    ()    => engine.peerConnected(peerId, { direction: 'outbound' }))
 *  ws.on('message', (raw) => engine.receiveMessage(peerId, raw))
 *  ws.on('close',   ()    => engine.peerDisconnected(peerId))
 *
 *  // 5. start() 后引擎自动调度 tickPull / tickCleanup / tickHeartbeat
 *  //    间隔通过 pullIntervalMs / cleanupIntervalMs / heartbeatIntervalMs 配置
 *
 *  // 6. 写操作：已注册表的所有增删改必须通过 engine.write() 或手动事务包裹
 *  //    ⚠️ 不要直接 db.run('INSERT ...') — 引擎需要管理事务和 2PC 同步
 *  app.post('/api/create', async (req, res) => {
 *      const result = await engine.write(async (db) => {
 *          const ts = engine.hlc.tick();
 *          db.run('INSERT INTO users ...', [ts, ...]);
 *          return { ok: true };
 *      }, userId);
 *  });
 */

import { HLC, timeValueToMs } from './hlc.js';
import { LeaderElection } from './leader-election.js';
import { applyDialect, validateDialectMethods } from './dialects.js';
import {
    logChange,
    makeHandshake,
    makeSync,
    makeAck,
    makePullRequest,
    makePing,
    makePong,
    makeLeaderHeartbeat,
    makeVoteReq,
    makeVoteAck,
    makeLeaderQuery,
    makeLeaderQueryAck,
    makeProxyReq,
    makeProxyRes,
    makePrepare,
    makePrepareAck,
    makeCommit,
    makeAbort,
    makeFullSyncRows,
    makeFullSyncTombstones,
    makeFullSyncComplete,
    makeFullSyncResumeReq,
    parseMessage,
    applyEntries,
    applyEntriesInTransaction,
    readSyncLog,
    getMaxSeq,
    getMinLogId,
    cleanSyncLog,
    cleanTombstones,
    getPeerLastSyncId,
    getPeerWatermark,
    updatePeerSyncId,
    getPeerPullOffset,
    updatePeerPullOffset,
    scanTableRowsPage,
    scanTombstonesPage,
    applyFullSyncRows,
    applyFullSyncTombstones,
    upsertFullSyncNeed,
    getAllFullSyncNeeds,
    subtractFromNeeds,
    getActiveClaims,
    insertClaim,
    deleteClaimById,
    deletePeerClaims,
    deletePeerScopeClaims,
    cleanExpiredClaims,
    computeUnclaimedFragments,
} from './sync-protocol.js';

// ========== 工具函数 ==========

function hashToShard(key, numShards) {
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
        hash = (Math.imul(hash, 31) + key.charCodeAt(i)) | 0;
    }
    return Math.abs(hash) % numShards;
}

// 按 keyColumns 顺序逐列比较两个 key 对象；遇 undefined/null 按小于处理。
function _cmpRowKey(a, b, keyColumns) {
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
// 与 _cmpRowKey 等价，但允许 a 是业务行（含额外列）
const _cmpRowValue = _cmpRowKey;

function randomUUID() {
    const hex = '0123456789abcdef';
    let uuid = '';
    for (let i = 0; i < 36; i++) {
        if (i === 8 || i === 13 || i === 18 || i === 23) uuid += '-';
        else if (i === 14) uuid += '4';
        else if (i === 19) uuid += hex[(Math.random() * 4 | 0) + 8];
        else uuid += hex[Math.random() * 16 | 0];
    }
    return uuid;
}

// ========== 时间单位转换 ==========

/** 与 hlc.js 保持一致（ms → 目标单位） */
const TIME_UNIT_TO_MS = { s: 1000, ms: 1, us: 1e-3, ns: 1e-6 };

/**
 * 将毫秒数转换为指定单位的 { value, unit } 对象
 * @param {number} ms
 * @param {'s'|'ms'|'us'|'ns'} unit
 */
function msToClock(ms, unit) {
    const factor = TIME_UNIT_TO_MS[unit];
    if (!factor) throw new Error('msToClock: unknown unit "' + unit + '"');
    return { value: ms / factor, unit };
}

// ========== SyncEngine ==========

export class SyncEngine {
    /**
     * @param {object} options
     *
     * ── 必须 ──
     * @param {string} options.nodeId - 节点唯一标识
     * @param {DatabaseAdapter} options.db - 数据库适配器
     * @param {function(string, string): void} options.onSendToPeer - 向 peer 发消息
     * @param {function(string, string): void} options.onClosePeer  - 通知关闭 peer
     * @param {function(number, string|null, boolean): void} options.onLeaderChanged - Leader 变更
     *
     * ── 可选回调 ──
     * @param {function(): void} [options.onWriteCompleted] - 同步数据写入完成通知
     * @param {function(string, Error): void} [options.onError] - 错误通知
     *
     * ── 必须回调 ──
     * @param {function(object): Promise<object>} options.onExecuteProxyRequest - 代理请求执行
     *   Leader 收到 Follower 转发的写请求后的执行回调。
     *   节点无法控制自己是否当选 Leader，因此此回调始终必须注册。
     *   payload / response 格式由外部业务自行定义，对模块透明。
     *
     * ── 方言 ──
     * @param {string} [options.dialect] - 数据库方言：'sqlite' | 'postgresql' | 'mysql'
     *   指定后自动填充 db 上缺失的方言方法（beginTransaction/commit/rollback/upsertSQL/infraSchemaSQL/triggersSQL/dropTriggersSQL）。
     *   不指定时所有方言方法必须由 db 自行实现，否则报错。
     *
     * ── 可选配置 ──
     * @param {number}   [options.numShards=16]            - 虚拟分片数
     * @param {number}   [options.syncBatchSize=500]       - 每批同步最大条目数
     * @param {number}   [options.heartbeatTimeoutMs=45000]  - 心跳超时判定阈值
     * @param {number}   [options.heartbeatIntervalMs=15000] - 心跳检测/发送间隔（tickHeartbeat 调度间隔）
     * @param {number}   [options.pullIntervalMs=10000]      - 拉取数据间隔（tickPull 调度间隔）
     * @param {number}   [options.cleanupIntervalMs=3600000] - 清理间隔（tickCleanup 调度间隔）
     * @param {number}   [options.fullSyncChunkSize=500]    - 全量重同步分页大小（每个 full_sync_rows/tombstones 消息的条目数）
     * @param {boolean}  [options.enableFullResync=true]    - 是否在握手检测到 peer 落后于本地 _sync_log 起始 id 时触发全量重同步
     *                     关闭后走老路径：peer 拉不到已被清理的历史 → 静默丢数据，仅用于对比实验
     * @param {number}   [options.fullSyncClaimTtlMs=60000] - _full_sync_claims 过期 TTL 上限（默认 60 秒）。
     *                     有 per-peer RTT 样本时实际 TTL = min(此值, max(fullSyncClaimTtlFloorMs, rtt × fullSyncClaimTtlRttFactor))；
     *                     无 RTT 样本时退化成此静态值。
     * @param {number}   [options.fullSyncClaimTtlFloorMs=5000] - 动态 TTL 下界（默认 5 秒），防止 rtt 极小时 TTL 短到和在途页竞争。
     * @param {number}   [options.fullSyncClaimTtlRttFactor=50] - 动态 TTL 的 RTT 倍数（默认 50）。典型 LAN rtt=50ms → 2.5s → 被 floor 抬到 5s；WAN rtt=200ms → 10s。
     *                     超过 TTL 仍未完成（对端卡死/丢消息）的 claim 会被清掉，下一轮 dispatcher 再分给其它 peer。
     * @param {number}   [options.proxyWaitMs=6000]         - 等待 Leader 选出的最长时间（静态下界）。
     *                     有 RTT 数据时动态放大为 max(proxyWaitMs, rtt×20 + heartbeatIntervalMs)，
     *                     确保至少覆盖一个完整选举周期。
     * @param {number}   [options.proxyTimeoutMs=10000]    - 代理请求超时（静态下界）。
     *                     有 RTT 数据时动态放大为 max(proxyTimeoutMs, rtt×6)。
     * @param {number}   [options.prepareTimeoutMs=5000]   - 2PC prepare 超时（静态下界）。
     *                     有 RTT 数据时动态放大为 max(prepareTimeoutMs, rtt×4)。
     * @param {number}   [options.followerTxnTimeoutMs=15000] - Follower 等待 commit/abort 超时
     * @param {number}   [options.maxClockSkewMs=300000]   - HLC receive 时钟偏差上限（ms）。
     *                     远端条目的 wallTime 若超出 localNow + maxClockSkewMs，跳过该条目，
     *                     防止时钟异常节点将本地 HLC 推进到不合理的未来时刻。
     *                     设为 Infinity 可关闭此检测（不推荐）。
     * @param {function(): number|Promise<number>} [options.getMinQuorum] - 获取最小法定人数
     * @param {function(string, string[], string[]): string} [options.generateUpsertSQL] - 自定义 upsert
     * @param {number}   [options.timeSyncWindowSize=8]     - 每个 peer 保留的 RTT 样本窗口大小（滑动窗口）
     *   对时请求随心跳 ping 一起发送，间隔由 heartbeatIntervalMs 控制。
     *
     * ── 时间函数注入（必须） ──
     * @param {function(): { value: number, unit: 's' | 'ms' | 'us' | 'ns' }} options.getNow
     *   获取当前时间戳的函数（必须外部注入，不使用内部 Date.now）。
     *   返回值为 { value, unit } 对象，value 为时间戳数值，unit 为单位。
     *   引擎内部所有需要获取当前时间的场景（HLC、心跳检测、peer 活跃时间等）均通过此函数调用。
     *   缺失时构造函数将抛出错误。
     *
     * ── 单调时钟注入（可选）──
     * @param {function(): { value: number, unit: 's' | 'ms' | 'us' | 'ns' }} [options.memonicNow]
     *   高分辨率单调时间源（可选）。提供时引擎内部自动实现单调时钟：
     *     - 构造时各记录一次 getNow() 和 memonicNow() 作为基准，生命周期内不变
     *     - 后续每次取时间：elapsed = memonicNow() - startMemonic，返回 startNow + elapsed
     *   这样 _nowMs() 严格单调递增，对系统时钟调整（NTP、手动、DST）免疫。
     *   典型接入方式（Node.js）：memonicNow: () => ({ value: Number(process.hrtime.bigint()), unit: 'ns' })
     *   不提供时直接调用 getNow()（向后兼容）。
     *
     * ── 定时器注入（必须） ──
     * @param {object} options.timerAPI - 定时器函数注入，必须提供，不允许使用全局定时器默认值
     *   引擎内部所有定时器（2PC 超时、proxy 超时、follower 事务超时、leader election 定时器）
     *   均通过此对象调用。缺失或不完整时构造函数将抛出错误。
     * @param {function} options.timerAPI.setTimeout
     * @param {function} options.timerAPI.clearTimeout
     * @param {function} options.timerAPI.setInterval
     * @param {function} options.timerAPI.clearInterval
     */
    constructor(options) {
        this.nodeId = options.nodeId;
        this.db = options.db;

        // ===== 时间函数注入（必须） =====
        if (typeof options.getNow !== 'function') {
            throw new Error(
                'SyncEngine: getNow is required and must be a function returning { value: number, unit: "s"|"ms"|"us"|"ns" }. ' +
                'Time functions must be externally injected, not use internal JS Date.now.'
            );
        }
        this._getNow = options.getNow;

        /**
         * 内部统一使用毫秒的便捷方法（原始本地时间，用于超时检测等）。
         *
         * 若外部注入了 memonicNow（单调时间源），则在构造时各采样一次基准值，
         * 后续每次调用计算 elapsed = memonicNow() - startMemonic，返回 startNow + elapsed，
         * 保证时间严格单调递增，不受系统时钟调整影响。
         * 未注入时直接调用 getNow()，行为与旧版完全一致。
         */
        if (typeof options.memonicNow === 'function') {
            const _startNowMs     = timeValueToMs(options.getNow());
            const _startMemonicMs = timeValueToMs(options.memonicNow());
            const _memonicNow     = options.memonicNow;
            this._nowMs = () => _startNowMs + (timeValueToMs(_memonicNow()) - _startMemonicMs);
        } else {
            this._nowMs = () => timeValueToMs(this._getNow());
        }

        /**
         * 全局时钟偏差估算（ms）。
         * 每次收到 pong 后，从所有 peer 中选 epsilon 最小（最可信）的 offset，
         * 以 20% 步长平滑收敛，避免突变导致 HLC wallTime 跳跃。
         * 初始为 0（无偏差），对时数据到达后逐步修正。
         */
        this._clockOffsetMs = 0;

        /**
         * HLC 使用经过对时偏差校正的本地时间：correctedNow = localNow + _clockOffsetMs
         * 这样 tick() / receive() 产生的时间戳更接近网络真实时间，
         * 减少因本地时钟偏慢导致的 counter 无谓递增。
         * 超时检测等场景仍使用原始 _nowMs()，不受此校正影响。
         */
        this.hlc = new HLC(options.nodeId, () => ({ value: this._nowMs() + this._clockOffsetMs, unit: 'ms' }));

        // ===== 方言处理 =====
        if (options.dialect) {
            applyDialect(this.db, options.dialect);
        }
        validateDialectMethods(this.db);

        // ===== 必须回调 =====
        this._onSendToPeer = options.onSendToPeer;
        this._onClosePeer = options.onClosePeer;
        this._onLeaderChanged = options.onLeaderChanged;

        // ===== 可选回调 =====
        this._onWriteCompleted = options.onWriteCompleted || (() => {});
        this._onError = options.onError || (() => {});

        // ===== 必须回调：onExecuteProxyRequest =====
        // 节点无法控制自己是否当选 Leader，因此此回调始终必须注册。
        if (typeof options.onExecuteProxyRequest !== 'function') {
            throw new Error(
                'SyncEngine: onExecuteProxyRequest is required. ' +
                'Any node can be elected as Leader and must be able to handle proxied write requests from Followers.'
            );
        }
        this._onExecuteProxyRequest = options.onExecuteProxyRequest;

        // ===== 可选：sync-lib 向外部订阅变更事件的回调 =====
        // 语义：这是 sync-lib 主动向外部应用"下订单"的订阅接口——sync-lib 告诉外部
        //   "请监听这张表（按此 where 过滤），有匹配变更时通过我给你的 onChanges 告诉我"。
        //   外部就像事件源（CDC / 业务层 hook / DB 日志），sync-lib 是订阅者。
        // 与 SQL 触发器并列、可并存；二者都未配置时 start() 报错。
        //
        // 外部应用只提供本回调函数；既不需要、也无法直接知道 sync-lib 关心哪些表，
        //   只能从每次回调的参数里得知"本次被订阅的表 + 过滤条件"。
        //
        // 订阅清单的来源：sync-lib 从 registerTable() 派生，每张表按其 def.where 生成一条订阅。
        //   initChangeSubscriptions() 遍历已注册的表，对每张表调用此回调一次，传入
        //   { table, where, keyColumns, dataColumns, onChanges }。
        //
        // 外部拿到 onChanges 后，在满足条件的行变更时调用：
        //   onChanges(eventTypes: ('INSERT'|'UPDATE'|'DELETE')[],
        //             eventInfos: { tableName, rowKey, rowData|null, hlc }[])
        if (options.subscribeToChanges !== undefined && typeof options.subscribeToChanges !== 'function') {
            throw new Error('SyncEngine: subscribeToChanges, if provided, must be a function');
        }
        this._subscribeToChanges = options.subscribeToChanges || null;
        this._changeLogTriggersInstalled = false;
        this._changeSubscriptionsInstalled = false;

        // ===== 外部配置 =====
        this._numShards = options.numShards ?? 16;
        this._syncBatchSize = options.syncBatchSize ?? 500;
        this._fullSyncChunkSize = options.fullSyncChunkSize ?? 500;
        this._enableFullResync = options.enableFullResync !== false;
        this._fullSyncClaimTtlMs = options.fullSyncClaimTtlMs ?? 60 * 1000;
        // claim 动态 TTL：effectiveTtl = min(fullSyncClaimTtlMs, max(fullSyncClaimTtlFloorMs, rtt × rttFactor))
        // 有 per-peer RTT 样本时用动态值，快网上静默卡死几秒就能回收；无 RTT 样本时退化成静态 fullSyncClaimTtlMs
        this._fullSyncClaimTtlFloorMs = options.fullSyncClaimTtlFloorMs ?? 5000;
        this._fullSyncClaimTtlRttFactor = options.fullSyncClaimTtlRttFactor ?? 50;
        this._heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? 45000;
        this._heartbeatIntervalMs = options.heartbeatIntervalMs ?? 15000;
        this._pullIntervalMs = options.pullIntervalMs ?? 10000;
        this._cleanupIntervalMs = options.cleanupIntervalMs ?? 3600000;
        this._proxyWaitMs = options.proxyWaitMs ?? 6000;
        this._proxyTimeoutMs = options.proxyTimeoutMs ?? 10000;
        this._prepareTimeoutMs = options.prepareTimeoutMs ?? 5000;
        this._followerTxnTimeoutMs = options.followerTxnTimeoutMs ?? 15000;
        this._maxClockSkewMs = options.maxClockSkewMs ?? 300000;
        this._generateUpsertSQL = options.generateUpsertSQL || null;

        // ===== 双向对时配置 =====
        // 对时随心跳 ping 合并发送，无需单独间隔配置
        this._timeSyncWindowSize = options.timeSyncWindowSize ?? 8;

        /** 测试专用：传透给 LeaderElection，覆盖选举/心跳定时参数（生产环境不使用） */
        this._testOptions = options._testOptions || {};

        this._getMinQuorum = typeof options.getMinQuorum === 'function'
            ? options.getMinQuorum
            : () => 1;

        // ===== 定时器注入（必须） =====
        const ta = options.timerAPI;
        if (!ta || typeof ta.setTimeout !== 'function' || typeof ta.clearTimeout !== 'function' ||
            typeof ta.setInterval !== 'function' || typeof ta.clearInterval !== 'function') {
            throw new Error(
                'SyncEngine: timerAPI is required and must provide setTimeout, clearTimeout, setInterval, clearInterval.'
            );
        }
        this._setTimeout   = ta.setTimeout;
        this._clearTimeout = ta.clearTimeout;
        this._setInterval  = ta.setInterval;
        this._clearInterval = ta.clearInterval;

        // ===== 表注册表 =====
        this._tableRegistry = new Map();

        // ===== Peer 状态 =====
        /** Map<peerId, { nodeId, direction, lastActivity }> */
        this._peers = new Map();
        /** Map<nodeId, peerId> — 逻辑 nodeId → 传输 peerId */
        this._nodeIdToPeerId = new Map();

        // ===== Leader Election =====
        this._shardElections = new Map();
        this._shardLeaders = new Map();

        // ===== 2PC Leader 侧等待池 =====
        /** Map<writeId, { resolve, reject, timeoutHandle, ackCount, requiredAcks }> */
        this._pendingPrepares = new Map();

        // ===== 2PC Follower 侧活跃事务 =====
        /** Map<writeId, { timeoutHandle, peerId }> */
        this._followerTxns = new Map();

        // ===== 代理请求等待池 =====
        /** Map<requestId, { resolve, reject, timeoutHandle }> */
        this._pendingProxies = new Map();

        // ===== 手动事务（2PC Leader 侧） =====
        this._activeManualTxn = null;

        // ===== 双向对时状态 =====
        /**
         * Map<peerId, TimeSyncState>
         * TimeSyncState: {
         *   seq: number,                    // 下一个请求序号
         *   pendingSeqs: Map<seq, t1Ms>,    // 待响应请求（seq → 发送时刻 ms）
         *   rtts: number[],                 // 滑动窗口 RTT 样本（ms）
         *   offsets: number[],              // 对应的时钟偏差样本（ms；正值表示对端时钟靠前）
         *   minRtt: number,                 // 当前窗口最小 RTT（ms）
         *   offset: number,                 // 最小 RTT 对应的偏差估算（ms）
         *   epsilon: number,                // 误差上界 = minRtt / 2（ms）
         *   lastSyncAt: number,             // 最近一次更新时刻（ms）
         * }
         */
        this._timeSyncStates = new Map();

        this._started = false;
        this._pushing = false;

        this._tickHeartbeatTimer = null;
        this._tickPullTimer = null;
        this._tickCleanupTimer = null;

        this._initShardElections();
    }

    // ╔═══════════════════════════════════════╗
    // ║           表注册                       ║
    // ╚═══════════════════════════════════════╝

    /**
     * 注册一张需要同步的业务表（必须在 start() 之前调用）
     *
     * @param {string} name - 表名
     * @param {object} def
     * @param {string[]} def.keyColumns     - 主键列名数组
     * @param {string[]} def.dataColumns    - 全部列名数组（必须包含 '_hlc'）
     * @param {string[]} [def.blobColumns]  - BLOB 列名（触发器中用 hex 序列化）
     * @param {function(object): void} [def.validator]      - 数据验证，不合法时抛异常
     * @param {function(object): object} [def.deserializer] - 反序列化（如 hex→Buffer）
     * @param {string|null} [def.where]      - 可选 SQL WHERE 片段，声明本表需要同步的行筛选条件。
     *   仅在使用外部变更订阅回调（subscribeToChanges）时才生效：initChangeSubscriptions() 会把它
     *   透传给回调，指示外部应用只上报满足此条件的行的变更。SQL 触发器不受影响（触发器覆盖全表）。
     */
    registerTable(name, def) {
        if (!def.keyColumns || def.keyColumns.length === 0) {
            throw new Error(`registerTable('${name}'): keyColumns is required and must be non-empty`);
        }
        if (!def.dataColumns || def.dataColumns.length === 0) {
            throw new Error(`registerTable('${name}'): dataColumns is required and must be non-empty`);
        }
        if (!def.dataColumns.includes('_hlc')) {
            throw new Error(`registerTable('${name}'): dataColumns must include '_hlc' column`);
        }
        if (def.where !== undefined && def.where !== null && typeof def.where !== 'string') {
            throw new Error(`registerTable('${name}'): where, if provided, must be a string or null`);
        }
        this._tableRegistry.set(name, {
            keyColumns: def.keyColumns,
            dataColumns: def.dataColumns,
            blobColumns: def.blobColumns || [],
            validator: def.validator || null,
            deserializer: def.deserializer || null,
            where: def.where || null,
        });
    }

    getTableRegistry() {
        return this._tableRegistry;
    }

    // ╔═══════════════════════════════════════╗
    // ║       Schema / Trigger 初始化          ║
    // ╚═══════════════════════════════════════╝

    /** 创建同步基础设施表（_sync_log, _sync_peers, _tombstones） */
    initSchema() {
        for (const sql of this.db.infraSchemaSQL()) {
            this.db.exec(sql);
        }
    }

    /**
     * 为所有已注册表创建同步触发器。
     *
     * 触发器分为两类：
     *   - HLC 校验触发器：强制 _hlc 非空且单调递增（数据完整性，默认安装）
     *   - 变更记录触发器：AFTER INSERT/UPDATE/DELETE 写入 _sync_log（变更检测用，默认安装）
     *
     * 若外部已用 subscribeToChanges 回调接管变更检测，可传 { changeLog: false } 只安装 HLC 校验。
     * 若 db 提供了拆分版 SQL（hlcTriggersSQL/changeLogTriggersSQL），按需单独安装；
     * 否则回退到 db.triggersSQL（此时 changeLog:false 无效，两类触发器一并安装）。
     *
     * @param {object} [options]
     * @param {boolean} [options.hlc=true]        - 是否安装 HLC 校验触发器
     * @param {boolean} [options.changeLog=true]  - 是否安装变更记录触发器
     */
    initTriggers(options = {}) {
        const installHlc = options.hlc !== false;
        const installChangeLog = options.changeLog !== false;
        const hasSplit = typeof this.db.hlcTriggersSQL === 'function'
            && typeof this.db.changeLogTriggersSQL === 'function';

        for (const [tableName, def] of this._tableRegistry) {
            if (hasSplit) {
                if (installHlc) {
                    for (const sql of this.db.hlcTriggersSQL(tableName, def)) this.db.exec(sql);
                }
                if (installChangeLog) {
                    for (const sql of this.db.changeLogTriggersSQL(tableName, def)) this.db.exec(sql);
                }
            } else {
                // 旧版 db 适配器：只能整体安装
                for (const sql of this.db.triggersSQL(tableName, def)) this.db.exec(sql);
            }
        }

        if (installChangeLog) {
            this._changeLogTriggersInstalled = true;
        }
    }

    /**
     * 删除所有已注册表的同步触发器。
     * @param {object} [options]
     * @param {boolean} [options.hlc=true]
     * @param {boolean} [options.changeLog=true]
     */
    dropTriggers(options = {}) {
        const dropHlc = options.hlc !== false;
        const dropChangeLog = options.changeLog !== false;
        const hasSplit = typeof this.db.dropHlcTriggersSQL === 'function'
            && typeof this.db.dropChangeLogTriggersSQL === 'function';

        for (const tableName of this._tableRegistry.keys()) {
            if (hasSplit) {
                if (dropHlc) for (const sql of this.db.dropHlcTriggersSQL(tableName)) this.db.exec(sql);
                if (dropChangeLog) for (const sql of this.db.dropChangeLogTriggersSQL(tableName)) this.db.exec(sql);
            } else {
                for (const sql of this.db.dropTriggersSQL(tableName)) this.db.exec(sql);
            }
        }

        if (dropChangeLog) {
            this._changeLogTriggersInstalled = false;
        }
    }

    /**
     * 遍历所有 registerTable 声明的表，对每一张表调用构造时注入的 subscribeToChanges 回调，
     * 让 sync-lib 向外部应用下发"请监听这张表（用这个 where 过滤），有变更时调用这个 onChanges"。
     *
     * 每张表回调一次 subscribeToChanges({ table, where, keyColumns, dataColumns, onChanges })；
     * 所有调用共享同一个 onChanges（都写 _sync_log）：
     *   onChanges(eventTypes: ('INSERT'|'UPDATE'|'DELETE')[],
     *             eventInfos: { tableName, rowKey, rowData|null, hlc }[])
     *
     * 必须在 initSchema() 之后、start() 之前调用（否则 _sync_log 表不存在或事件不会被推送）。
     * 构造时未提供 subscribeToChanges，或当前没有任何已注册表时，调用此方法会抛错。
     */
    initChangeSubscriptions() {
        if (!this._subscribeToChanges) {
            throw new Error(
                'SyncEngine.initChangeSubscriptions: subscribeToChanges callback was not provided in constructor options.'
            );
        }
        if (this._tableRegistry.size === 0) {
            throw new Error(
                'SyncEngine.initChangeSubscriptions: no tables registered; call registerTable() first.'
            );
        }

        const onChanges = (eventTypes, eventInfos) => this._applyExternalChanges(eventTypes, eventInfos);

        for (const [name, def] of this._tableRegistry) {
            this._subscribeToChanges({
                table: name,
                where: def.where || null,
                keyColumns: def.keyColumns.slice(),
                dataColumns: def.dataColumns.slice(),
                onChanges,
            });
        }

        this._changeSubscriptionsInstalled = true;
    }

    /**
     * 内部：将用户回调上报的变更写入 _sync_log。
     *
     * 整批事件在单个事务中提交:任一条校验失败或写入失败,整批回滚,
     * 不会在 _sync_log 中留下半截批次。
     *
     * @param {Array<'INSERT'|'UPDATE'|'DELETE'>} eventTypes
     * @param {Array<{ tableName: string, rowKey: object, rowData: object|null, hlc: string }>} eventInfos
     */
    _applyExternalChanges(eventTypes, eventInfos) {
        if (!Array.isArray(eventTypes) || !Array.isArray(eventInfos)) {
            throw new Error('onChanges: eventTypes and eventInfos must both be arrays');
        }
        if (eventTypes.length !== eventInfos.length) {
            throw new Error('onChanges: eventTypes and eventInfos must have the same length');
        }
        if (eventTypes.length === 0) return;

        this.db.beginTransaction();
        try {
            for (let i = 0; i < eventTypes.length; i++) {
                const op = eventTypes[i];
                const info = eventInfos[i];
                if (op !== 'INSERT' && op !== 'UPDATE' && op !== 'DELETE') {
                    throw new Error(`onChanges: invalid eventType "${op}" at index ${i}`);
                }
                if (!info || typeof info !== 'object') {
                    throw new Error(`onChanges: eventInfo at index ${i} must be an object`);
                }
                if (!info.tableName || !this._tableRegistry.has(info.tableName)) {
                    throw new Error(`onChanges: unknown tableName "${info.tableName}" at index ${i}`);
                }
                if (!info.rowKey || typeof info.rowKey !== 'object') {
                    throw new Error(`onChanges: rowKey at index ${i} must be an object`);
                }
                if (op !== 'DELETE' && (!info.rowData || typeof info.rowData !== 'object')) {
                    throw new Error(`onChanges: rowData at index ${i} is required for ${op}`);
                }
                if (!info.hlc || typeof info.hlc !== 'string') {
                    throw new Error(`onChanges: hlc at index ${i} must be a non-empty string`);
                }

                const rowKeyJson = JSON.stringify(info.rowKey);
                const rowDataJson = op === 'DELETE' ? null : JSON.stringify(info.rowData);
                logChange(this.db, info.tableName, op, rowKeyJson, rowDataJson, info.hlc);
            }
            this.db.commit();
        } catch (err) {
            try { this.db.rollback(); } catch (_) { /* ignore rollback errors */ }
            throw err;
        }
    }

    // ╔═══════════════════════════════════════╗
    // ║           生命周期                     ║
    // ╚═══════════════════════════════════════╝

    /** 启动引擎（启动 Leader Election 状态机 + 内部周期性任务） */
    start() {
        if (this._started) return;

        // 变更检测源必须至少配置一种：SQL 触发器（initTriggers 已安装变更记录触发器）
        // 或用户注入的 subscribeToChanges 回调（initChangeSubscriptions 已调用）。
        // 二者可并存，都缺失时无法收集本地写入的变更，同步数据就无从谈起。
        if (!this._changeLogTriggersInstalled && !this._changeSubscriptionsInstalled) {
            throw new Error(
                'SyncEngine.start: no change-detection source configured. ' +
                'Call initTriggers() to install SQL change-log triggers, ' +
                'or provide subscribeToChanges in constructor options and call initChangeSubscriptions(), ' +
                'or both.'
            );
        }

        this._started = true;
        for (const election of this._shardElections.values()) {
            election.start();
        }
        this._tickHeartbeatTimer = this._setInterval(() => this._tickHeartbeat(), this._heartbeatIntervalMs);
        this._tickPullTimer      = this._setInterval(() => this._tickPull(),      this._pullIntervalMs);
        this._tickCleanupTimer   = this._setInterval(() => this._tickCleanup(),   this._cleanupIntervalMs);
    }

    /** 停止引擎 */
    stop() {
        this._started = false;

        if (this._tickHeartbeatTimer) { this._clearInterval(this._tickHeartbeatTimer); this._tickHeartbeatTimer = null; }
        if (this._tickPullTimer)      { this._clearInterval(this._tickPullTimer);      this._tickPullTimer = null; }
        if (this._tickCleanupTimer)   { this._clearInterval(this._tickCleanupTimer);   this._tickCleanupTimer = null; }

        for (const election of this._shardElections.values()) {
            election.stop();
        }

        for (const [, pending] of this._pendingProxies) {
            this._clearTimeout(pending.timeoutHandle);
            pending.reject(new Error('SyncEngine stopped'));
        }
        this._pendingProxies.clear();

        for (const [, pending] of this._pendingPrepares) {
            this._clearTimeout(pending.timeoutHandle);
            pending.reject(new Error('SyncEngine stopped'));
        }
        this._pendingPrepares.clear();

        for (const [, txn] of this._followerTxns) {
            this._clearTimeout(txn.timeoutHandle);
            try { this.db.rollback(); } catch (_) {}
        }
        this._followerTxns.clear();

        if (this._activeManualTxn) {
            try { this.db.rollback(); } catch (_) {}
            this._activeManualTxn = null;
        }

        this._peers.clear();
        this._nodeIdToPeerId.clear();
    }

    // ╔═══════════════════════════════════════╗
    // ║         周期性任务（内部调度）          ║
    // ╚═══════════════════════════════════════╝

    /**
     * 心跳检测 + 双向对时：每次 tick 检查超时，并向所有存活 peer 发带时钟字段的 ping。
     * ping/pong 两用：既维持心跳，又完成 RTT 测量 + 时钟偏差估算。
     * 由 start() 通过 setInterval 自动调度，间隔 heartbeatIntervalMs（默认 15s）。
     */
    _tickHeartbeat() {
        const now = this._nowMs();
        for (const [peerId, peer] of this._peers) {
            if (now - peer.lastActivity > this._heartbeatTimeoutMs) {
                this._closePeer(peerId, 'heartbeat_timeout');
                continue;
            }
            // 每次 tick 都发 ping（携带对时字段），无论 peer 是否空闲
            const state = this._timeSyncStates.get(peerId);
            if (state) {
                const seq = state.seq++;
                const t1Ms = this._nowMs();
                state.pendingSeqs.set(seq, t1Ms);
                // 清理超过 2× heartbeatTimeoutMs 未响应的挂起请求，防止 Map 无限增长
                const staleThreshold = t1Ms - this._heartbeatTimeoutMs * 2;
                for (const [s, t1] of state.pendingSeqs) {
                    if (t1 < staleThreshold) state.pendingSeqs.delete(s);
                }
                this._sendToPeer(peerId, makePing(seq, t1Ms));
            } else {
                this._sendToPeer(peerId, makePing());
            }
        }
    }

    /**
     * 定时拉取：从所有 peer 拉取数据变化
     * 由 start() 通过 setInterval 自动调度，间隔 pullIntervalMs（默认 10s）
     */
    _tickPull() {
        for (const peerId of this._getActivePeerIds()) {
            const peer = this._peers.get(peerId);
            if (!peer?.nodeId) continue;
            try {
                // pull 偏移是本节点对该 peer 日志的消费进度（对方序号空间），
                // 只由实际收到条目的路径推进（_advancePullOffset）；与
                // _sync_peers 的 push 进度（本节点序号空间）严格分开。
                const pullOffset = getPeerPullOffset(this.db, peer.nodeId);
                if (pullOffset === null) {
                    // 无拉取记录（新对端，或记录已被清理）：不从 0 拉取，
                    // 改走与握手自检相同的最大序号比对——对端比我新且为
                    // outbound 连接时登记全量同步 need；对端不比我新则无可
                    // 拉取；对端最大序号未知（尚未收到其握手）时跳过本轮。
                    if (peer.direction === 'outbound' && this._enableFullResync &&
                        typeof peer.remoteMaxSeq === 'number' &&
                        peer.remoteMaxSeq > getMaxSeq(this.db)) {
                        this._registerFullSyncNeeds(peer.remoteMaxSeq);
                        try { this._dispatchPendingNeeds(); } catch (_) { /* non-fatal */ }
                    }
                    continue;
                }
                this._sendToPeer(peerId, makePullRequest(pullOffset));
            } catch (err) {
                this._onError('pull', err);
            }
        }
    }

    /**
     * 定时清理：清理过期的 sync_log 和墓碑
     * 由 start() 通过 setInterval 自动调度，间隔 cleanupIntervalMs（默认 1h）
     */
    _tickCleanup() {
        try {
            cleanSyncLog(this.db);
            cleanTombstones(this.db, this._getNow, 7);
            cleanExpiredClaims(this.db, this._getNow);
            this._dispatchPendingNeeds();
        } catch (err) {
            this._onError('cleanup', err);
        }
    }

    // ╔═══════════════════════════════════════╗
    // ║   Peer 管理（外部传输层调用）           ║
    // ╚═══════════════════════════════════════╝

    /**
     * 外部传输层建立新连接后调用
     * @param {string} peerId - 传输层的连接标识（外部自定义）
     * @param {object} [opts]
     * @param {'outbound'|'inbound'} [opts.direction='outbound']
     */
    peerConnected(peerId, opts) {
        const direction = opts?.direction || 'outbound';
        this._peers.set(peerId, {
            nodeId: null,
            direction,
            lastActivity: this._nowMs(),
            // 对端在握手时报告的最大序号 / 最小日志 id（收到握手后填入）。
            // 水位行缺失时，定时拉/推以 remoteMaxSeq 为比对基准，不回退到 0。
            remoteMaxSeq: null,
            remoteMinLogId: null,
        });
        this._timeSyncStates.set(peerId, {
            seq: 0,
            pendingSeqs: new Map(),
            rtts: [],
            offsets: [],
            minRtt: Infinity,
            offset: 0,
            epsilon: Infinity,
            lastSyncAt: 0,
        });
        // 发送握手（附带本地 _sync_log 的 min/max id，供对端检测 gap）
        const currentMaxSeq = getMaxSeq(this.db);
        const currentMinLogId = getMinLogId(this.db);
        this._sendToPeer(peerId, makeHandshake(this.nodeId, currentMaxSeq, currentMinLogId));
    }

    /**
     * 外部传输层连接断开后调用
     * @param {string} peerId
     */
    peerDisconnected(peerId) {
        const peer = this._peers.get(peerId);
        if (!peer) return;

        if (peer.nodeId) {
            const mapped = this._nodeIdToPeerId.get(peer.nodeId);
            if (mapped === peerId) {
                this._nodeIdToPeerId.delete(peer.nodeId);
            }
            for (const election of this._shardElections.values()) {
                election.removeNode(peer.nodeId);
            }
            // 释放该 peer 所有在途 claim —— 下一次 dispatcher 会分配给其他 peers。
            try { deletePeerClaims(this.db, peer.nodeId); } catch (_) { /* non-fatal */ }
        }
        this._abortFollowerTxnsForPeer(peerId);
        this._timeSyncStates.delete(peerId);
        this._peers.delete(peerId);
        try { this._dispatchPendingNeeds(); } catch (_) { /* non-fatal */ }
    }

    /**
     * 外部传输层收到消息后调用
     * @param {string} peerId
     * @param {string} raw - 原始消息字符串
     */
    receiveMessage(peerId, raw) {
        const peer = this._peers.get(peerId);
        if (!peer) return;

        peer.lastActivity = this._nowMs();
        const msg = parseMessage(raw);
        if (!msg) return;

        switch (msg.type) {
            case 'handshake':    this._handleHandshake(peerId, msg); break;
            case 'sync':         this._handleSync(peerId, msg); break;
            case 'ack':          this._handleAck(peerId, msg); break;
            case 'pull_req':     this._handlePullRequest(peerId, msg); break;
            case 'ping':  this._handlePing(peerId, msg); break;
            case 'pong':  this._handlePong(peerId, msg); break;
            case 'leader_heartbeat': {
                const el = this._shardElections.get(msg.shardId);
                if (el) el.receiveHeartbeat(msg.term, msg.leaderId);
                break;
            }
            case 'leader_query': {
                const el = this._shardElections.get(msg.shardId);
                if (el) el.receiveLeaderQuery(msg.queryId, msg.askerId, msg.targetLeaderId ?? null);
                break;
            }
            case 'leader_query_ack': {
                const el = this._shardElections.get(msg.shardId);
                if (el) el.receiveLeaderQueryAck(msg.queryId, msg.verdict, msg.leaderId, msg.term, msg.responderId);
                break;
            }
            case 'vote_req': {
                const el = this._shardElections.get(msg.shardId);
                if (el) el.receiveVoteReq(msg.term, msg.candidateId, msg.priority ?? 0);
                break;
            }
            case 'vote_ack': {
                const el = this._shardElections.get(msg.shardId);
                if (el) el.receiveVoteAck(msg.term, msg.candidateId, msg.granted, msg.voterId);
                break;
            }
            case 'proxy_req':    this._handleProxyReq(peerId, msg); break;
            case 'proxy_res':    this._handleProxyRes(msg); break;
            case 'prepare':      this._handlePrepare(peerId, msg); break;
            case 'prepare_ack':  this._handlePrepareAck(msg); break;
            case 'commit':       this._handleCommit(msg); break;
            case 'abort':        this._handleAbort(msg); break;
            case 'full_sync_rows':       this._handleFullSyncRows(peerId, msg); break;
            case 'full_sync_tombstones': this._handleFullSyncTombstones(peerId, msg); break;
            case 'full_sync_complete':   this._handleFullSyncComplete(peerId, msg); break;
            case 'full_sync_resume_req': this._handleFullSyncResumeReq(peerId, msg); break;
        }
    }

    _handleFullSyncRows(peerId, msg) {
        const def = this._tableRegistry.get(msg.table);
        if (!def) return;
        const opts = { ...this._applyEntriesOpts(), inTransaction: true };
        const pageStart = msg.pageStart || null;
        const pageEnd = msg.pageEnd || null;
        this.db.beginTransaction();
        try {
            if (msg.rows && msg.rows.length > 0) {
                applyFullSyncRows(this.db, msg.table, def, msg.rows, this.hlc, opts);
            }
            // 不论收到几行，都按发送方声明的 (pageStart, pageEnd] 从 needs 里挖掉这段。
            // 空页 (rows=[], pageEnd=null) 表示"此 range 到尾/无新数据"，仍挖掉。
            subtractFromNeeds(this.db, 'rows', msg.table, pageStart, pageEnd, def.keyColumns, this._getNow);
            this.db.commit();
        } catch (err) {
            try { this.db.rollback(); } catch (_) { /* ignore */ }
            this._onError('apply_full_sync_rows', err);
        }
        // 挖光某 need 后可能释放新碎片给其它 peers；此外 pageEnd 之后无 claim 重叠时也应补发。
        try { this._dispatchPendingNeeds(); } catch (_) { /* non-fatal */ }
    }

    _handleFullSyncTombstones(peerId, msg) {
        const opts = { ...this._applyEntriesOpts(), inTransaction: true };
        const pageStart = msg.pageStart || null;
        const pageEnd = msg.pageEnd || null;
        const tombKeyColumns = ['table_name', 'row_key'];
        this.db.beginTransaction();
        try {
            if (msg.tombstones && msg.tombstones.length > 0) {
                applyFullSyncTombstones(this.db, msg.tombstones, this.hlc, this._tableRegistry, opts);
            }
            subtractFromNeeds(this.db, 'tombstones', null, pageStart, pageEnd, tombKeyColumns, this._getNow);
            this.db.commit();
        } catch (err) {
            try { this.db.rollback(); } catch (_) { /* ignore */ }
            this._onError('apply_full_sync_tombstones', err);
        }
        try { this._dispatchPendingNeeds(); } catch (_) { /* non-fatal */ }
    }

    _handleFullSyncResumeReq(peerId, msg) {
        try {
            if (!this._enableFullResync) return;
            const peer = this._peers.get(peerId);
            if (!peer?.nodeId) return;
            if (!msg.request || typeof msg.request !== 'object') return;
            this._sendFullSync(peerId, peer.nodeId, msg.request);
        } catch (err) {
            this._onError('full_sync_resume_req', err);
        }
    }

    /**
     * 每次：peerConnect/handshake、page 收到、complete、peerDisconnect、tickCleanup 都调度一次。
     *
     * 核心：
     *   1. 遍历 needs
     *   2. 每条 need 计算未被 active claim 覆盖的"空隙碎片"
     *   3. 每个空隙碎片分给一个"尚未对此 scope 持 claim"的 outbound peer，插 claim + 发 resume_req
     *   4. 不同 scope 的碎片可分给同一个 peer（不同 scope 互不干扰）；同一 scope 的多个碎片可分给不同 peers（并行）
     */
    _dispatchPendingNeeds() {
        if (!this._enableFullResync) return;

        const needs = getAllFullSyncNeeds(this.db);
        if (needs.length === 0) return;

        const activeClaims = getActiveClaims(this.db, this._getNow);

        // 可用 outbound peers
        const outboundPeers = [];
        for (const [peerId, peer] of this._peers) {
            if (peer.direction === 'outbound' && peer.nodeId) {
                outboundPeers.push({ peerId, nodeId: peer.nodeId });
            }
        }
        if (outboundPeers.length === 0) return;

        // 按 need 逐条处理：同 scope 的 active claim 归一组，计算空隙
        // 按 scope 分组 claims
        const claimsByScope = new Map();
        for (const c of activeClaims) {
            const key = c.phase + '|' + (c.tableName ?? '');
            if (!claimsByScope.has(key)) claimsByScope.set(key, []);
            claimsByScope.get(key).push(c);
        }

        // 按 scope 分组 needs（同 scope 的多条 need 行代表该 scope 的不相交空洞碎片）
        const needsByScope = new Map();
        for (const need of needs) {
            const scopeKey = need.phase + '|' + (need.tableName ?? '');
            if (!needsByScope.has(scopeKey)) needsByScope.set(scopeKey, []);
            needsByScope.get(scopeKey).push(need);
        }

        for (const [scopeKey, scopeNeeds] of needsByScope) {
            const sample = scopeNeeds[0];
            const scopeClaims = claimsByScope.get(scopeKey) || [];
            const keyColumns = sample.phase === 'tombstones'
                ? ['table_name', 'row_key']
                : (this._tableRegistry.get(sample.tableName)?.keyColumns || []);
            if (keyColumns.length === 0 && sample.phase === 'rows') continue;  // 表未注册，跳过

            // 汇总该 scope 所有 need 的未被 claim 的碎片
            const allFragments = [];
            for (const need of scopeNeeds) {
                const frags = computeUnclaimedFragments(
                    need.rangeStart, need.rangeEnd, scopeClaims, keyColumns
                );
                for (const f of frags) allFragments.push({ frag: f, need });
            }
            if (allFragments.length === 0) continue;

            // 该 scope 下已持 claim 的 peers（会随本轮分配动态扩充，防止重复派发给同一 peer）
            const busyPeers = new Set(scopeClaims.map(c => c.peerNodeId));
            // 本轮内因发送失败而被判"不可用"的 peer —— 不再给它们派任何碎片
            const deadPeers = new Set();

            for (const { frag, need } of allFragments) {
                // 为同一碎片最多试一轮所有空闲 peer；每次发送失败就 rollback claim 并换下一个
                let assigned = false;
                while (!assigned) {
                    const freePeer = outboundPeers.find(p => !busyPeers.has(p.nodeId) && !deadPeers.has(p.nodeId));
                    if (!freePeer) break;  // 没 free peer 了, 本碎片放弃
                    const claimId = insertClaim(
                        this.db,
                        need.phase, need.tableName,
                        frag.rangeStart, frag.rangeEnd,
                        freePeer.nodeId,
                        this._computeClaimTtlMs(freePeer.peerId), this._getNow
                    );
                    try {
                        // 调用原始回调（绕过 _sendToPeer 的 swallow-then-log），以便失败时立即回滚 claim
                        this._onSendToPeer(freePeer.peerId, makeFullSyncResumeReq({
                            phase: need.phase,
                            tableName: need.tableName,
                            rangeStart: frag.rangeStart,
                            rangeEnd: frag.rangeEnd,
                            watermarkSeq: need.watermarkSeq,
                        }));
                        busyPeers.add(freePeer.nodeId);
                        assigned = true;
                    } catch (err) {
                        // 发送失败：rollback 刚写入的 claim，把 peer 标 dead，下轮尝试另一个
                        try { deleteClaimById(this.db, claimId); } catch (_) { /* non-fatal */ }
                        deadPeers.add(freePeer.nodeId);
                        this._onError('dispatch_send', err);
                    }
                }
                if (!assigned) break;  // 本 scope 里所有 peer 都不可用 → 剩余碎片也放弃
            }
        }
    }

    _handleFullSyncComplete(peerId, msg) {
        try {
            const peer = this._peers.get(peerId);
            if (peer?.nodeId && msg.request && typeof msg.request === 'object') {
                // 发送方完成了特定 (phase, table) 的区间推送 → 删该 peer 对应 scope 的 claim。
                deletePeerScopeClaims(this.db, peer.nodeId, msg.request.phase, msg.request.tableName || null);
            } else if (peer?.nodeId) {
                // 兼容无 request 的 complete：删该 peer 所有 claims。
                deletePeerClaims(this.db, peer.nodeId);
            }
            this._dispatchPendingNeeds();
        } catch (err) {
            this._onError('full_sync_complete', err);
        }
    }

    // ╔═══════════════════════════════════════╗
    // ║    本地写通知 / Push-on-Write          ║
    // ╚═══════════════════════════════════════╝

    /** 本地发生写操作后调用，向所有 peer 推送变更 */
    notifyLocalWrite() {
        if (this._pushing) return;
        this._pushing = true;
        try {
            this._pushToAllPeers();
        } finally {
            this._pushing = false;
        }
    }

    // ╔═══════════════════════════════════════╗
    // ║    2PC 写操作高阶 API                  ║
    // ╚═══════════════════════════════════════╝

    /**
     * 在 2PC 事务中执行写操作（高阶 API，自动处理事务开启/提交/回滚）
     *
     * 适用于多节点部署下需要强一致性的写操作。
     * 引擎自动完成：BEGIN → 执行 fn → 广播 prepare → 等待 quorum ack → COMMIT → 广播 commit
     *
     * @param {function(db: DatabaseAdapter): any} fn - 写操作函数，在事务中执行
     *   fn 接收 db 适配器，可直接调用 db.run/get/all 执行业务写操作。
     *   fn 的返回值会作为 write() 的返回值透传。
     * @param {string} [shardKey] - 分片键（如 userId），用于确定 term 和 shardId
     * @returns {Promise<any>} fn 的返回值
     *
     * @example
     * const result = await engine.write(async (db) => {
     *     const ts = engine.hlc.tick();
     *     db.run('INSERT INTO users (user_id, name, _hlc) VALUES (?, ?, ?)', ['u1', 'Alice', ts]);
     *     return { userId: 'u1' };
     * }, userId);
     */
    async write(fn, shardKey) {
        const writeId = randomUUID();
        const shardId = shardKey ? this.getShardId(shardKey) : 0;
        const term = this._shardElections.get(shardId)?._currentTerm || 0;

        this.beginManualTransaction(writeId);
        let result;
        let prepareSent = false;
        let committed = false;
        try {
            result = await fn(this.db);

            const entries = this.getManualTransactionEntries(writeId);
            if (entries.length > 0 && this._getActivePeerIds().length > 0) {
                prepareSent = true;
                await this.waitForPrepareAck(writeId, entries, term, shardId);

                // 验证 term 未变（防止 prepare 等待期间发生选举导致脑裂）
                const termAfter = this._shardElections.get(shardId)?._currentTerm || 0;
                if (termAfter !== term) {
                    throw new Error('2PC prepare rejected: term changed during prepare (was ' + term + ', now ' + termAfter + ')');
                }
            }

            this.commitManualTransaction(writeId);
            committed = true;

            if (entries && entries.length > 0) {
                try { this.broadcastCommit(writeId); } catch (_) {}
            }
        } catch (err) {
            if (!committed) {
                this.rollbackManualTransaction(writeId);
            }
            if (prepareSent && !committed) {
                try { this.broadcastAbort(writeId, err.message); } catch (_) {}
            }
            throw err;
        }
        return result;
    }

    // ╔═══════════════════════════════════════╗
    // ║    手动事务管理（2PC Leader 侧）        ║
    // ╚═══════════════════════════════════════╝

    beginManualTransaction(writeId) {
        if (this._activeManualTxn) {
            throw new Error('Already has active manual transaction (writeId=' + this._activeManualTxn.writeId + ')');
        }
        const prevMaxSeq = getMaxSeq(this.db);
        this.db.beginTransaction();
        this._activeManualTxn = { writeId, prevMaxSeq };
    }

    getManualTransactionEntries(writeId) {
        if (!this._activeManualTxn || this._activeManualTxn.writeId !== writeId) {
            throw new Error('No active manual transaction for writeId=' + writeId);
        }
        return readSyncLog(this.db, this._activeManualTxn.prevMaxSeq, 10000);
    }

    commitManualTransaction(writeId) {
        if (!this._activeManualTxn || this._activeManualTxn.writeId !== writeId) {
            throw new Error('No active manual transaction for writeId=' + writeId);
        }
        this.db.commit();
        this._activeManualTxn = null;
        this._onWriteCompleted();
    }

    rollbackManualTransaction(writeId) {
        if (!this._activeManualTxn) return;
        if (writeId && this._activeManualTxn.writeId !== writeId) {
            throw new Error('writeId mismatch in rollback');
        }
        try { this.db.rollback(); } catch (_) {}
        this._activeManualTxn = null;
    }

    hasActiveManualTransaction() {
        return this._activeManualTxn !== null;
    }

    // ╔═══════════════════════════════════════╗
    // ║     2PC 协调（Leader 侧对外 API）      ║
    // ╚═══════════════════════════════════════╝

    /** 当前是否有已连接的 peer */
    hasPeers() {
        return this._getActivePeerIds().length > 0;
    }

    /**
     * Leader 侧：广播 prepare，等待 quorum 确认
     * @param {string} writeId
     * @param {Array<object>} entries
     * @param {number} term
     * @param {number} [shardId=0]
     * @returns {Promise<void>}
     */
    waitForPrepareAck(writeId, entries, term, shardId = 0) {
        const activePeerIds = this._getActivePeerIds();
        if (activePeerIds.length === 0) {
            return Promise.reject(new Error('2PC: no connected peers, refusing write'));
        }

        const minQuorum = this._getMinQuorumSync();
        const requiredAcks = Math.max(1, minQuorum - 1);

        return new Promise((resolve, reject) => {
            const effectiveTimeout = this._getEffectivePrepareTimeoutMs();
            const timeoutHandle = this._setTimeout(() => {
                this._pendingPrepares.delete(writeId);
                this._broadcastToAll(makeAbort(writeId, 'timeout'));
                reject(new Error('2PC prepare timeout for writeId=' + writeId));
            }, effectiveTimeout);

            this._pendingPrepares.set(writeId, {
                resolve, reject, timeoutHandle,
                ackCount: 0, requiredAcks,
            });

            const prepareMsg = makePrepare(writeId, entries, term, shardId);
            for (const pid of activePeerIds) {
                this._sendToPeer(pid, prepareMsg);
            }
        });
    }

    /** Leader 侧：广播 commit */
    broadcastCommit(writeId) {
        this._broadcastToAll(makeCommit(writeId));
    }

    /** Leader 侧：广播 abort */
    broadcastAbort(writeId, reason) {
        this._broadcastToAll(makeAbort(writeId, reason || 'leader_abort'));
    }

    // ╔═══════════════════════════════════════╗
    // ║         写请求代理                     ║
    // ╚═══════════════════════════════════════╝

    /**
     * Follower 将写请求代理给对应分片的 Leader
     * @param {string} shardKey - 分片键（如 userId）
     * @param {object} payload  - 请求载荷（对模块透明）
     * @returns {Promise<object>} Leader 的响应载荷
     */
    async proxyRequest(shardKey, payload) {
        const shardId = hashToShard(shardKey, this._numShards);
        const leaderNodeId = await this._waitForShardLeader(shardId, this._getEffectiveProxyWaitMs());
        if (!leaderNodeId) {
            throw new Error('shard ' + shardId + ': no leader');
        }

        if (leaderNodeId === this.nodeId) {
            if (!this._onExecuteProxyRequest) {
                throw new Error(
                    'onExecuteProxyRequest is not registered on this node, but it is the Leader. ' +
                    'In a multi-node deployment, all nodes must register onExecuteProxyRequest.'
                );
            }
            return this._onExecuteProxyRequest(payload);
        }

        const leaderPeerId = this._nodeIdToPeerId.get(leaderNodeId);
        if (!leaderPeerId || !this._peers.has(leaderPeerId)) {
            throw new Error('shard ' + shardId + ': leader peer not available');
        }

        const requestId = randomUUID();
        return new Promise((resolve, reject) => {
            const timeoutHandle = this._setTimeout(() => {
                this._pendingProxies.delete(requestId);
                reject(new Error('proxy request timed out'));
            }, this._getEffectiveProxyTimeoutMs());

            this._pendingProxies.set(requestId, { resolve, reject, timeoutHandle });
            this._sendToPeer(leaderPeerId, makeProxyReq(requestId, payload));
        });
    }

    // ╔═══════════════════════════════════════╗
    // ║       手动变更记录（替代触发器）        ║
    // ╚═══════════════════════════════════════╝

    /**
     * 手动记录数据变更到 _sync_log
     * 适用于不使用 SQLite 触发器的场景（如 PostgreSQL、MySQL）。
     *
     * @param {string} tableName
     * @param {'INSERT'|'UPDATE'|'DELETE'} operation
     * @param {object} rowKey  - 主键对象，如 { user_id: 'u1' }
     * @param {object|null} rowData - 完整行数据（DELETE 时为 null）
     * @returns {string} 本次操作的 HLC 时间戳
     */
    logChange(tableName, operation, rowKey, rowData) {
        const hlcTs = this.hlc.tick();
        this.db.run(
            'INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc) VALUES (?, ?, ?, ?, ?)',
            [tableName, operation, JSON.stringify(rowKey), rowData ? JSON.stringify(rowData) : null, hlcTs]
        );
        return hlcTs;
    }

    // ╔═══════════════════════════════════════╗
    // ║       分片 / Leader 查询               ║
    // ╚═══════════════════════════════════════╝

    getShardId(key) { return hashToShard(key, this._numShards); }
    isLeaderForShard(shardId) { return this._shardElections.get(shardId)?.isLeader() ?? false; }
    isLeaderForKey(key) { return this.isLeaderForShard(this.getShardId(key)); }
    getLeaderForShard(shardId) { return this._shardLeaders.get(shardId) ?? null; }
    getLeaderForKey(key) { return this.getLeaderForShard(this.getShardId(key)); }

    /**
     * 立即触发所有分片重新选举，走标准选举流程（含正常的 onLeaderChanged 通知）。
     * 通常在外部配置参数（如 getMinQuorum 返回值）变更后调用。
     */
    triggerElection() {
        for (const election of this._shardElections.values()) {
            election.triggerElection();
        }
    }

    /**
     * 判断当前节点在指定分片上的 Leader 租约是否有效。
     *
     * 租约有效 = 当前是 Leader，且距上次心跳的时间 < (electionMin - ε)。
     * 租约有效期内，可安全执行本地读（无需 quorum），因为此期间不可能产生新 Leader。
     *
     * 依赖条件：
     *   - 节点必须是 Leader（否则直接返回 false）
     *   - 必须有至少一个 peer 完成过双向对时（否则 ε=Infinity，leaseDuration=0）
     *
     * @param {number} [shardId=0]
     * @returns {boolean}
     *
     * @example
     * // 本地读（不走 quorum）
     * if (engine.isLeaseValid()) {
     *     return db.get('SELECT * FROM users WHERE id = ?', [userId]);
     * }
     * // 需要强一致性的读走 quorum（或等待选举完成）
     */
    isLeaseValid(shardId = 0) {
        return this._shardElections.get(shardId)?.isLeaseValid() ?? false;
    }

    /**
     * 判断指定 key 所在分片的 Leader 租约是否有效。
     * @param {string} key - 业务 key（与 write/proxyRequest 的 shardKey 一致）
     * @returns {boolean}
     */
    isLeaseValidForKey(key) {
        return this.isLeaseValid(this.getShardId(key));
    }

    getStatus() {
        return {
            started: this._started,
            nodeId: this.nodeId,
            numShards: this._numShards,
            shards: Array.from(this._shardElections.values()).map(e => e.getStatus()),
            peers: Array.from(this._peers.entries()).map(([pid, info]) => ({
                peerId: pid, nodeId: info.nodeId, direction: info.direction,
            })),
            nodeIdMap: Array.from(this._nodeIdToPeerId.entries()).map(([nid, pid]) => ({
                nodeId: nid, peerId: pid,
            })),
            currentMaxSeq: getMaxSeq(this.db),
        };
    }

    /**
     * 获取与 peer 的双向对时结果
     *
     * 基于 NTP-style RTT 测量：
     *   offset  = ((t2-t1) + (t3-t4)) / 2   （正值表示对端时钟靠前）
     *   epsilon = minRTT / 2                  （时钟误差上界）
     *
     * @param {string} [peerId]
     *   指定 peer 的传输层标识（由外部传输层在 peerConnected() 时提供）。
     *   不传时返回所有 peer 的对时结果 Map<peerId, SyncResult>。
     * @param {'s'|'ms'|'us'|'ns'} [unit='ms']
     *   返回时间值的单位（rtt / offset / epsilon 均使用此单位）。
     *
     * @returns {SyncResult | Map<string, SyncResult> | null}
     *   SyncResult: {
     *     rtt:       { value: number, unit: string },  // 最小往返时延
     *     offset:    { value: number, unit: string },  // 时钟偏差估算
     *     epsilon:   { value: number, unit: string },  // 误差上界（= minRtt/2）
     *     samples:   number,                           // 当前窗口有效样本数
     *     lastSyncAt: number,                          // 最近一次成功测量的时刻（ms）
     *   }
     *   - 若指定了 peerId 但尚无测量结果，返回 null
     *   - 不传 peerId 时返回 Map（只包含已有样本的 peer）
     */
    getClockSync(peerId, unit = 'ms') {
        const toUnit = (ms) => msToClock(ms, unit);

        if (peerId !== undefined) {
            const state = this._timeSyncStates.get(peerId);
            if (!state || state.rtts.length === 0) return null;
            return {
                rtt:        toUnit(state.minRtt),
                offset:     toUnit(state.offset),
                epsilon:    toUnit(state.epsilon),
                samples:    state.rtts.length,
                lastSyncAt: state.lastSyncAt,
            };
        }

        // 返回所有已有样本的 peer
        const result = new Map();
        for (const [pid, state] of this._timeSyncStates) {
            if (state.rtts.length === 0) continue;
            result.set(pid, {
                rtt:        toUnit(state.minRtt),
                offset:     toUnit(state.offset),
                epsilon:    toUnit(state.epsilon),
                samples:    state.rtts.length,
                lastSyncAt: state.lastSyncAt,
            });
        }
        return result;
    }

    // ╔═══════════════════════════════════════╗
    // ║    ── 以下为内部实现 ──                 ║
    // ╚═══════════════════════════════════════╝

    _initShardElections() {
        for (let s = 0; s < this._numShards; s++) {
            const shardId = s;
            const election = new LeaderElection({
                shardId,
                nodeId: this.nodeId,
                getMinQuorum: this._getMinQuorum,
                timerAPI: {
                    setTimeout:   this._setTimeout,
                    clearTimeout: this._clearTimeout,
                    setInterval:   this._setInterval,
                    clearInterval: this._clearInterval,
                },
                /**
                 * 基于双向对时 RTT 计算候选优先级：
                 *   所有 peer 的平均 minRtt 越小 → 本节点网络位置越居中 → 优先级越高
                 *   映射到 (0,1)：priority = 1/(1 + avgRtt/100) + rand*0.01
                 *   无对时数据时回退到纯随机。
                 */
                getPriority: () => this._getElectionPriority(),
                /**
                 * 返回当前所有 peer 中最小的往返时延（ms），用于动态调整选举超时。
                 * 无数据时返回 null（LeaderElection 使用默认固定范围）。
                 */
                getNetworkRttMs: () => this._getBestNetworkRttMs(),
                /**
                 * 单个 peer 的近期典型 RTT（对时窗口内样本均值），供存活问询
                 * 动态定超时：前几次通信多快，这次大概率还是多快。
                 */
                getPeerRttMs: (nodeId) => {
                    const pid = this._nodeIdToPeerId.get(nodeId);
                    const st = pid ? this._timeSyncStates.get(pid) : null;
                    if (!st || st.rtts.length === 0) return null;
                    return st.rtts.reduce((a, b) => a + b, 0) / st.rtts.length;
                },
                /**
                 * 返回所有 peer 中最大的时钟误差上界 ε（ms），用于计算 Leader 租约时长。
                 * 无对时数据时返回 Infinity（租约不可用）。
                 */
                getEpsilonMs: () => this._getBestEpsilonMs(),
                /** 本地时钟（原始，不加偏差校正），用于租约时间比较 */
                getNowMs: () => this._nowMs(),
                _testOptions: this._testOptions,
                onSendHeartbeat: (sid, term, leaderId) => {
                    this._broadcastToAll(makeLeaderHeartbeat(sid, term, leaderId));
                },
                onSendVoteReq: (sid, term, candidateId, priority) => {
                    this._broadcastToAll(makeVoteReq(sid, term, candidateId, priority));
                },
                onSendVoteAck: (sid, term, candidateId, granted) => {
                    const peerId = this._nodeIdToPeerId.get(candidateId);
                    if (peerId) {
                        this._sendToPeer(peerId, makeVoteAck(sid, term, candidateId, granted, this.nodeId));
                    }
                },
                onSendLeaderQuery: (sid, queryId, targetNodeId, targetLeaderId) => {
                    const peerId = this._nodeIdToPeerId.get(targetNodeId);
                    if (peerId) {
                        this._sendToPeer(peerId, makeLeaderQuery(sid, queryId, this.nodeId, targetLeaderId));
                    }
                },
                onSendLeaderQueryAck: (sid, queryId, askerId, verdict, leaderId, term) => {
                    const peerId = this._nodeIdToPeerId.get(askerId);
                    if (peerId) {
                        this._sendToPeer(peerId, makeLeaderQueryAck(sid, queryId, verdict, leaderId, term, this.nodeId));
                    }
                },
                onLeaderChanged: (sid, leaderNodeId, isLocal) => {
                    this._shardLeaders.set(sid, leaderNodeId);
                    this._onLeaderChanged(sid, leaderNodeId, isLocal);
                },
            });
            this._shardElections.set(shardId, election);
            this._shardLeaders.set(shardId, null);
        }
    }

    // ---- 消息处理 ----

    /**
     * 登记覆盖全部业务表 + tombstones 的全量同步 need（scope=(-∞,+∞]，
     * watermark=peerLastSeq）。握手自检与 _tickPull 缺水位比对共用。
     * @param {number} peerLastSeq - 对端报告的最大序号
     */
    _registerFullSyncNeeds(peerLastSeq) {
        upsertFullSyncNeed(this.db, 'tombstones', null, peerLastSeq, this._getNow);
        for (const [tableName] of this._tableRegistry.entries()) {
            upsertFullSyncNeed(this.db, 'rows', tableName, peerLastSeq, this._getNow);
        }
    }

    _handleHandshake(peerId, msg) {
        const remoteNodeId = msg.nodeId;
        const peer = this._peers.get(peerId);
        if (!peer) return;
        peer.nodeId = remoteNodeId;
        // 保留对端报告的最大序号 / 最小日志 id：水位行缺失时，定时拉/推
        // 以此为比对基准（见 _tickPull / _pushToAllPeers），不回退到 0。
        if (typeof msg.lastSeq === 'number') peer.remoteMaxSeq = msg.lastSeq;
        if (typeof msg.minLogId === 'number') peer.remoteMinLogId = msg.minLogId;

        // 去重
        const existingPeerId = this._nodeIdToPeerId.get(remoteNodeId);
        if (existingPeerId && existingPeerId !== peerId) {
            const existingPeer = this._peers.get(existingPeerId);
            if (existingPeer) {
                if (peer.direction === 'outbound' && existingPeer.direction === 'inbound') {
                    if (this.nodeId < remoteNodeId) {
                        this._closePeer(existingPeerId, 'dedup');
                    } else {
                        this._closePeer(peerId, 'dedup');
                        return;
                    }
                } else if (peer.direction === 'inbound' && existingPeer.direction === 'outbound') {
                    if (this.nodeId < remoteNodeId) {
                        this._closePeer(peerId, 'dedup');
                        return;
                    } else {
                        this._closePeer(existingPeerId, 'dedup');
                    }
                } else {
                    this._closePeer(peerId, 'dedup');
                    return;
                }
            }
        }

        this._nodeIdToPeerId.set(remoteNodeId, peerId);
        for (const election of this._shardElections.values()) {
            election.addNode(remoteNodeId);
        }

        // ── 接收方（本节点）自检：是否要向对端拉全量？──
        //
        // 触发条件（仅 outbound 连接）：
        //   peer.lastSeq > localMaxSeq → 对端比我新，我要从它拉。
        //   （对端水位 ≤ 本地水位时什么都不做；增量推送由下方常规 sync 路径处理。）
        //
        // 合并逻辑：
        //   对每张已注册业务表 + tombstones，upsert 一条 scope=(-∞,+∞] 的 need，
        //   watermark = max(现有 watermark, peer.lastSeq)。
        //   多个 outbound peer 先后连上时，多次调用会抬升 watermark 或 no-op，
        //   但永远不会产生多条并列 need（所有 peer 覆盖同一 key 空间）。
        try {
            const outbound = peer.direction === 'outbound';
            if (this._enableFullResync && outbound) {
                const peerLastSeq = msg.lastSeq || 0;
                const localMaxSeq = getMaxSeq(this.db);
                if (peerLastSeq > localMaxSeq) {
                    this._registerFullSyncNeeds(peerLastSeq);
                }
            }
        } catch (err) {
            this._onError('handshake_need_register', err);
        }

        // 不论是否新登记了 need，都调度一次：
        //   - 有 pending need → 把 claim 分给当前连上的 peer（含本次新连的）
        //   - 无 pending need → no-op
        try { this._dispatchPendingNeeds(); } catch (_) { /* non-fatal */ }

        // 常规增量推送：对端水位落后于我时发最新 _sync_log 给它。
        try {
            const peerLastSeq = msg.lastSeq || 0;
            const entries = readSyncLog(this.db, peerLastSeq, this._syncBatchSize);
            if (entries.length > 0) {
                this._sendToPeer(peerId, makeSync(entries));
                updatePeerSyncId(this.db, remoteNodeId, entries[entries.length - 1].seq, this._getNow);
            }
        } catch (err) {
            this._onError('handshake_push', err);
        }
    }

    /**
     * 响应 full_sync_resume_req：按请求指定的 (phase, table, rangeStart, rangeEnd] 区间流式推送。
     * 发送方完全无状态：请求怎么说就怎么推，接收端据每页 (pageStart, pageEnd] 从 needs 里挖。
     *
     * @param {string} peerId
     * @param {string} remoteNodeId
     * @param {{phase, tableName, rangeStart, rangeEnd, watermarkSeq}} request
     */
    _sendFullSync(peerId, remoteNodeId, request) {
        try {
            const chunkSize = this._fullSyncChunkSize;
            const watermarkSeq = request.watermarkSeq ?? getMaxSeq(this.db);
            const rangeStart = request.rangeStart || null;
            const rangeEnd = request.rangeEnd || null;

            // 一次请求 = 一页：扫 ≤ chunkSize 条，发一条数据页 + 一条 complete。
            // 超出 chunkSize 的部分留给接收端 subtract 后由 dispatcher 再 claim。
            if (request.phase === 'rows') {
                const def = this._tableRegistry.get(request.tableName);
                if (!def) {
                    // 表不存在：回整段空页，让接收端挖掉避免死循环
                    this._sendToPeer(peerId, makeFullSyncRows(
                        request.tableName, [], rangeStart, rangeEnd, watermarkSeq
                    ));
                } else {
                    const { rows, nextLastKey } = scanTableRowsPage(
                        this.db, request.tableName, def, rangeStart, chunkSize
                    );
                    // 本页上界：nextLastKey（未扫完）或 rangeEnd（已到请求区间尾/表尾）
                    let pageEnd;
                    let clippedRows = rows;
                    if (nextLastKey && rangeEnd !== null && _cmpRowKey(nextLastKey, rangeEnd, def.keyColumns) > 0) {
                        pageEnd = rangeEnd;
                        clippedRows = rows.filter(r => _cmpRowValue(r, rangeEnd, def.keyColumns) <= 0);
                    } else {
                        pageEnd = nextLastKey || rangeEnd;  // null 代表到表尾/+∞
                    }
                    this._sendToPeer(peerId, makeFullSyncRows(
                        request.tableName, clippedRows, rangeStart, pageEnd, watermarkSeq
                    ));
                }
            } else if (request.phase === 'tombstones') {
                const keyCols = ['table_name', 'row_key'];
                const { tombstones, nextLastKey } = scanTombstonesPage(this.db, rangeStart, chunkSize);
                let pageEnd;
                let clippedTombs = tombstones;
                if (nextLastKey && rangeEnd !== null && _cmpRowKey(nextLastKey, rangeEnd, keyCols) > 0) {
                    pageEnd = rangeEnd;
                    clippedTombs = tombstones.filter(t => _cmpRowValue(t, rangeEnd, keyCols) <= 0);
                } else {
                    pageEnd = nextLastKey || rangeEnd;
                }
                this._sendToPeer(peerId, makeFullSyncTombstones(
                    clippedTombs, rangeStart, pageEnd, watermarkSeq
                ));
            }

            // complete 携带 request，接收方按 scope 删 claim
            this._sendToPeer(peerId, JSON.stringify({
                type: 'full_sync_complete',
                watermarkSeq,
                request: {
                    phase: request.phase,
                    tableName: request.tableName || null,
                    rangeStart: request.rangeStart || null,
                    rangeEnd: request.rangeEnd || null,
                },
            }));
            updatePeerSyncId(this.db, remoteNodeId, watermarkSeq, this._getNow);
        } catch (err) {
            this._onError('full_sync_send', err);
        }
    }

    /**
     * 推进本节点对某 peer 日志的消费偏移（pull offset，对方序号空间）。
     * 只允许连续推进：批次首序号必须接得上当前偏移（或从 1 开始），
     * 否则说明中间还有没收到的条目——本批数据照常应用，但偏移不越过
     * 缺口，缺口留给后续 pull 按偏移补齐。无记录且批次不从 1 开始时
     * 同样不建偏移（走全量同步/推送路径兜底）。
     */
    _advancePullOffset(peerNodeId, firstSeq, lastSeq) {
        if (!peerNodeId || typeof firstSeq !== 'number' || typeof lastSeq !== 'number') return;
        try {
            const cur = getPeerPullOffset(this.db, peerNodeId);
            if (cur === null) {
                if (firstSeq <= 1) updatePeerPullOffset(this.db, peerNodeId, lastSeq, this._getNow);
                return;
            }
            if (firstSeq <= cur + 1 && lastSeq > cur) {
                updatePeerPullOffset(this.db, peerNodeId, lastSeq, this._getNow);
            }
        } catch (err) {
            this._onError('pull_offset', err);
        }
    }

    _handleSync(peerId, msg) {
        if (!msg.entries || msg.entries.length === 0) return;
        const peer = this._peers.get(peerId);

        try {
            const result = applyEntries(this.db, msg.entries, this.hlc, this._tableRegistry, this._applyEntriesOpts());

            const maxSeq = msg.entries[msg.entries.length - 1].seq;

            // 收到并处理完这批条目 → 推进对该 peer 的 pull 偏移。
            // 注意：不得在此更新 _sync_peers 的 push 进度——收到对方
            // 数据不等于对方已消费本节点日志，虚增会让 push 跳过缺口。
            if (peer?.nodeId) {
                this._advancePullOffset(peer.nodeId, msg.entries[0].seq, maxSeq);
            }

            // ack 报告本节点对发送方日志的连续消费偏移（而非本批最大
            // 序号）：发送方据此推进 push 水位，未被连续消费的条目会
            // 被重发，不会因乱序/缺口批次的 ack 被跳过。
            const consumed = peer?.nodeId ? (getPeerPullOffset(this.db, peer.nodeId) ?? 0) : maxSeq;
            this._sendToPeer(peerId, makeAck(consumed));

            if (result.applied > 0) {
                this._onWriteCompleted();
            }
        } catch (err) {
            this._onError('apply_sync', err);
        }
    }

    _handleAck(peerId, msg) {
        const peer = this._peers.get(peerId);
        if (peer?.nodeId && msg.lastSeq) {
            try { updatePeerSyncId(this.db, peer.nodeId, msg.lastSeq, this._getNow); }
            catch (err) { this._onError('handle_ack', err); }
        }
    }

    _handlePullRequest(peerId, msg) {
        try {
            const entries = readSyncLog(this.db, msg.sinceSeq || 0, this._syncBatchSize);
            if (entries.length > 0) this._sendToPeer(peerId, makeSync(entries));
        } catch (err) {
            this._onError('handle_pull', err);
        }
    }

    // ---- 心跳 + 双向对时（ping/pong 两用） ----

    /**
     * 收到 ping：
     *  - 若携带 seq/t1，记录 t2 后立即回 pong（含 t2/t3），实现双向对时
     *  - 否则仅回纯 pong（向后兼容旧 peer）
     */
    _handlePing(peerId, msg) {
        if (typeof msg.seq === 'number' && typeof msg.t1 === 'number') {
            const t2Ms = this._nowMs();
            const t3Ms = this._nowMs();
            this._sendToPeer(peerId, makePong(msg.seq, msg.t1, t2Ms, t3Ms));
        } else {
            this._sendToPeer(peerId, makePong());
        }
    }

    /**
     * 收到 pong：
     *  - 若携带对时字段（seq/t1/t2/t3），计算 RTT 和时钟偏差，更新滑动窗口
     *  - 否则忽略（纯心跳 pong）
     *
     * 公式（NTP style）：
     *   rtt    = (t4 - t1) - (t3 - t2)         // 网络往返时延，排除对端处理耗时
     *   offset = ((t2 - t1) + (t3 - t4)) / 2   // 时钟偏差（正值=对端时钟靠前）
     *   epsilon = minRtt / 2                     // 误差上界
     */
    _handlePong(peerId, msg) {
        if (typeof msg.seq !== 'number' || typeof msg.t1 !== 'number' ||
            typeof msg.t2 !== 'number' || typeof msg.t3 !== 'number') return;

        const t4Ms = this._nowMs();
        const state = this._timeSyncStates.get(peerId);
        if (!state) return;

        if (!state.pendingSeqs.has(msg.seq)) return;
        const localT1 = state.pendingSeqs.get(msg.seq);
        state.pendingSeqs.delete(msg.seq);

        const rtt = (t4Ms - localT1) - (msg.t3 - msg.t2);
        if (rtt < 0) return; // 测量异常，丢弃

        const offset = ((msg.t2 - localT1) + (msg.t3 - t4Ms)) / 2;

        state.rtts.push(rtt);
        state.offsets.push(offset);
        if (state.rtts.length > this._timeSyncWindowSize) {
            state.rtts.shift();
            state.offsets.shift();
        }

        // 取最小 RTT 对应的偏差（该样本误差最小）
        let minRtt = Infinity;
        let bestOffset = 0;
        for (let i = 0; i < state.rtts.length; i++) {
            if (state.rtts[i] < minRtt) {
                minRtt = state.rtts[i];
                bestOffset = state.offsets[i];
            }
        }

        state.minRtt = minRtt;
        state.offset = bestOffset;
        state.epsilon = minRtt / 2;
        state.lastSyncAt = t4Ms;

        // 每次成功测量后更新全局时钟偏差，为 HLC 提供更准确的本地时间
        this._updateClockOffset();
    }

    /**
     * 从所有 peer 中选取 epsilon 最小（最可信）的 offset，
     * 以 20% 步长平滑收敛到 _clockOffsetMs，避免突变导致 HLC wallTime 跳跃。
     *
     * 为什么选 epsilon 最小的 peer：
     *   epsilon = minRtt/2 是误差上界，epsilon 越小说明该样本的测量误差越小，
     *   对应的 offset 估算越可信。
     */
    _updateClockOffset() {
        let bestEpsilon = Infinity;
        let bestOffset = null;
        for (const state of this._timeSyncStates.values()) {
            if (state.rtts.length > 0 && isFinite(state.epsilon) && state.epsilon < bestEpsilon) {
                bestEpsilon = state.epsilon;
                bestOffset = state.offset;
            }
        }
        if (bestOffset === null) return;
        // 平滑收敛（步长 20%），防止 offset 突变
        this._clockOffsetMs = this._clockOffsetMs * 0.8 + bestOffset * 0.2;
    }

    /**
     * 返回所有 peer 中最小的往返时延（ms），供选举超时动态计算使用。
     * 无对时数据时返回 null。
     */
    _getBestNetworkRttMs() {
        let best = null;
        for (const state of this._timeSyncStates.values()) {
            if (state.rtts.length > 0 && isFinite(state.minRtt)) {
                if (best === null || state.minRtt < best) best = state.minRtt;
            }
        }
        return best;
    }

    /**
     * 计算给定 peer 下的 full-sync claim 有效 TTL（ms）。
     *   无 RTT 样本 → 静态 _fullSyncClaimTtlMs（保底最保守）
     *   有 RTT 样本 → min(_fullSyncClaimTtlMs, max(_fullSyncClaimTtlFloorMs, rtt × rttFactor))
     * 动机：快网上 peer 静默卡死时无需等 10 分钟兜底；慢链路自动放宽到配置上限。
     */
    _computeClaimTtlMs(peerId) {
        const state = this._timeSyncStates.get(peerId);
        if (!state || state.rtts.length === 0 || !isFinite(state.minRtt)) {
            return this._fullSyncClaimTtlMs;
        }
        const dynamic = Math.max(this._fullSyncClaimTtlFloorMs, state.minRtt * this._fullSyncClaimTtlRttFactor);
        return Math.min(this._fullSyncClaimTtlMs, dynamic);
    }

    /**
     * 返回所有 peer 中最大的时钟误差上界 ε（ms），供 Leader 租约计算使用。
     *
     * 使用最大值（最保守估计）的原因：
     *   租约需要对所有 Follower 都安全，若某 Follower 的 ε 最大，
     *   则其时钟误差最大，最有可能在租约期内意外发起选举。
     *   取最大 ε 确保对所有 Follower 的安全边距都足够。
     *
     * 无对时数据时返回 Infinity（LeaderElection 会得到 leaseDuration=0，租约不可用）。
     */
    _getBestEpsilonMs() {
        let maxEpsilon = null;
        for (const state of this._timeSyncStates.values()) {
            if (state.rtts.length > 0 && isFinite(state.epsilon)) {
                if (maxEpsilon === null || state.epsilon > maxEpsilon) maxEpsilon = state.epsilon;
            }
        }
        return maxEpsilon === null ? Infinity : maxEpsilon;
    }

    /**
     * 计算本节点的选举候选优先级（基于双向对时 RTT）。
     *
     * 用所有 peer 的平均 minRtt 估算本节点的网络中心性：
     *   avgRtt=0   → priority≈1.0（极低延迟，网络位置极佳）
     *   avgRtt=50  → priority≈0.67
     *   avgRtt=100 → priority≈0.50
     *   avgRtt=200 → priority≈0.33
     *
     * 加 rand×0.01 微扰防止多节点完全相同优先级时陷入平票。
     * 无对时数据（刚启动 / 单节点）时回退到纯随机，与旧行为一致。
     */
    _getElectionPriority() {
        let totalRtt = 0;
        let count = 0;
        for (const state of this._timeSyncStates.values()) {
            if (state.rtts.length > 0 && isFinite(state.minRtt)) {
                totalRtt += state.minRtt;
                count++;
            }
        }
        if (count === 0) return Math.random();
        const avgRtt = totalRtt / count;
        // 映射到 (0,1)：avgRtt 越小 priority 越接近 1
        const base = 1 / (1 + avgRtt / 100);
        return Math.min(0.9999, base + Math.random() * 0.01);
    }

    // ---- 动态超时计算（基于对时 RTT）----

    /**
     * 2PC prepare 超时（动态）：
     *   有 RTT 数据时 = max(配置下界, minRtt × 4)
     *   rtt×4 覆盖一个 prepare 往返（2×RTT）加上 Follower 写盘抖动（2×RTT 余量）。
     *   无数据时使用配置下界，行为与旧版完全一致。
     * @returns {number} ms
     */
    _getEffectivePrepareTimeoutMs() {
        const rtt = this._getBestNetworkRttMs();
        if (rtt !== null && rtt > 0 && isFinite(rtt)) {
            return Math.max(this._prepareTimeoutMs, Math.ceil(rtt * 4));
        }
        return this._prepareTimeoutMs;
    }

    /**
     * proxy 请求超时（动态）：
     *   有 RTT 数据时 = max(配置下界, minRtt × 6)
     *   rtt×6 覆盖一个 proxy 往返（2×RTT）加上 Leader 本地执行时间（4×RTT 余量）。
     *   无数据时使用配置下界。
     * @returns {number} ms
     */
    _getEffectiveProxyTimeoutMs() {
        const rtt = this._getBestNetworkRttMs();
        if (rtt !== null && rtt > 0 && isFinite(rtt)) {
            return Math.max(this._proxyTimeoutMs, Math.ceil(rtt * 6));
        }
        return this._proxyTimeoutMs;
    }

    /**
     * 等待 Leader 选出的超时（动态）：
     *   有 RTT 数据时 = max(配置下界, rtt×20 + heartbeatIntervalMs)
     *   rtt×20 = 动态 electionMin（与 LeaderElection._computeElectionMin 公式一致），
     *   再加一个心跳间隔作为选举完成后心跳广播的传播余量。
     *   无数据时使用配置下界。
     * @returns {number} ms
     */
    _getEffectiveProxyWaitMs() {
        const rtt = this._getBestNetworkRttMs();
        if (rtt !== null && rtt > 0 && isFinite(rtt)) {
            const electionMin = Math.round(Math.max(
                this._heartbeatIntervalMs * 1.5,
                rtt * 20
            ));
            return Math.max(this._proxyWaitMs, electionMin + this._heartbeatIntervalMs);
        }
        return this._proxyWaitMs;
    }

    /**
     * 构建传给 applyEntries / applyEntriesInTransaction 的选项对象。
     * 统一携带 HLC 时钟偏差上限参数，避免多处重复。
     * @private
     * @returns {object|undefined}
     */
    _applyEntriesOpts() {
        const base = this._generateUpsertSQL ? { generateUpsertSQL: this._generateUpsertSQL } : {};
        return Object.assign(base, {
            maxClockSkewMs: this._maxClockSkewMs,
            localNowMs: this._nowMs(),
        });
    }

    // ---- 代理请求 ----

    async _handleProxyReq(peerId, msg) {
        const { requestId, payload } = msg;
        try {
            if (!this._onExecuteProxyRequest) {
                // onExecuteProxyRequest 未注册：当前节点是 Leader 但无法处理代理请求。
                // 这通常意味着多节点部署时忘记注册此回调。
                const err = new Error(
                    'onExecuteProxyRequest is not registered on this node, but it was elected as Leader. ' +
                    'In a multi-node deployment, all nodes must register onExecuteProxyRequest ' +
                    'because any node can become Leader.'
                );
                this._onError('proxy_exec', err);
                this._sendToPeer(peerId, makeProxyRes(requestId, { error: err.message }));
                return;
            }
            const result = await this._onExecuteProxyRequest(payload);
            this._sendToPeer(peerId, makeProxyRes(requestId, result));
        } catch (err) {
            this._onError('proxy_exec', err);
            this._sendToPeer(peerId, makeProxyRes(requestId, { error: err.message }));
        }
    }

    _handleProxyRes(msg) {
        const pending = this._pendingProxies.get(msg.requestId);
        if (!pending) return;
        this._clearTimeout(pending.timeoutHandle);
        this._pendingProxies.delete(msg.requestId);
        pending.resolve(msg.payload);
    }

    // ---- 2PC Follower ----

    _handlePrepare(peerId, msg) {
        const { writeId, entries, term } = msg;
        if (!writeId || !entries) {
            this._sendToPeer(peerId, makePrepareAck(writeId, false, 'invalid message'));
            return;
        }
        if (this._followerTxns.has(writeId)) {
            this._sendToPeer(peerId, makePrepareAck(writeId, true, null));
            return;
        }
        if (this._followerTxns.size > 0) {
            this._sendToPeer(peerId, makePrepareAck(writeId, false, 'busy'));
            return;
        }
        const prepareShardId = msg.shardId ?? 0;
        const currentTerm = this._shardElections.get(prepareShardId)?._currentTerm || 0;
        if (typeof term === 'number' && term < currentTerm) {
            this._sendToPeer(peerId, makePrepareAck(writeId, false, 'stale term'));
            return;
        }

        try {
            this.db.beginTransaction();
            applyEntriesInTransaction(this.db, entries, this.hlc, this._tableRegistry, this._applyEntriesOpts());

            const timeoutHandle = this._setTimeout(() => {
                this._followerTxns.delete(writeId);
                try { this.db.rollback(); } catch (_) {}
            }, this._followerTxnTimeoutMs);

            this._followerTxns.set(writeId, {
                timeoutHandle, peerId,
                // 暂存条目在 leader 日志中的序号区间，commit 落地后据此
                // 推进对 leader 的 pull 偏移（prepare 阶段不推进：可能 abort）
                firstSeq: entries[0]?.seq,
                lastSeq: entries[entries.length - 1]?.seq,
            });
            this._sendToPeer(peerId, makePrepareAck(writeId, true, null));
        } catch (err) {
            try { this.db.rollback(); } catch (_) {}
            this._sendToPeer(peerId, makePrepareAck(writeId, false, err.message));
        }
    }

    _handlePrepareAck(msg) {
        const pending = this._pendingPrepares.get(msg.writeId);
        if (!pending) return;
        if (!msg.granted) {
            this._clearTimeout(pending.timeoutHandle);
            this._pendingPrepares.delete(msg.writeId);
            this._broadcastToAll(makeAbort(msg.writeId, 'follower_rejected: ' + (msg.reason || 'unknown')));
            pending.reject(new Error('2PC prepare rejected: ' + (msg.reason || 'unknown')));
            return;
        }
        pending.ackCount++;
        if (pending.ackCount >= pending.requiredAcks) {
            this._clearTimeout(pending.timeoutHandle);
            this._pendingPrepares.delete(msg.writeId);
            pending.resolve();
        }
    }

    _handleCommit(msg) {
        const txn = this._followerTxns.get(msg.writeId);
        if (!txn) return;
        this._clearTimeout(txn.timeoutHandle);
        this._followerTxns.delete(msg.writeId);
        try {
            this.db.commit();
            // 暂存条目已正式落地 → 计入对 leader 日志的消费偏移
            const leaderNodeId = txn.peerId ? this._peers.get(txn.peerId)?.nodeId : null;
            this._advancePullOffset(leaderNodeId, txn.firstSeq, txn.lastSeq);
            this._onWriteCompleted();
        } catch (err) {
            this._onError('2pc_commit', err);
            try { this.db.rollback(); } catch (_) {}
        }
    }

    _handleAbort(msg) {
        const txn = this._followerTxns.get(msg.writeId);
        if (!txn) return;
        this._clearTimeout(txn.timeoutHandle);
        this._followerTxns.delete(msg.writeId);
        try { this.db.rollback(); } catch (_) {}
    }

    // ---- Push / Pull ----

    _pushToAllPeers() {
        for (const peerId of this._getActivePeerIds()) {
            const peer = this._peers.get(peerId);
            if (!peer?.nodeId) continue;
            try {
                let lastSeq = getPeerWatermark(this.db, peer.nodeId);
                if (lastSeq === null) {
                    // 无水位记录：不从 0 重放，改以对端握手时报告的最大序号
                    // 为基准——只推它报告位置之后的增量；基准未知（尚未收到
                    // 其握手）时跳过本轮，等握手路径处理。
                    if (typeof peer.remoteMaxSeq !== 'number') continue;
                    lastSeq = peer.remoteMaxSeq;
                }
                const entries = readSyncLog(this.db, lastSeq, this._syncBatchSize);
                if (entries.length === 0) continue;
                this._sendToPeer(peerId, makeSync(entries));
                // 发送后不推进 push 水位：水位只由接收方的 ack 推进
                // （ack 报告其连续消费偏移）。发送即推进会让接收方未能
                // 落地的批次（单连接事务冲突等）永远不再重发，形成缺口。
            } catch (err) {
                this._onError('push', err);
            }
        }
    }

    // ---- 辅助 ----

    _sendToPeer(peerId, message) {
        try { this._onSendToPeer(peerId, message); }
        catch (err) { this._onError('send', err); }
    }

    _closePeer(peerId, reason) {
        this.peerDisconnected(peerId);
        try { this._onClosePeer(peerId, reason); }
        catch (err) { this._onError('close_peer', err); }
    }

    _broadcastToAll(message) {
        for (const peerId of this._getActivePeerIds()) {
            this._sendToPeer(peerId, message);
        }
    }

    _getActivePeerIds() {
        const result = [];
        const seenNodeIds = new Set();
        for (const [peerId, peer] of this._peers) {
            if (peer.nodeId) {
                if (seenNodeIds.has(peer.nodeId)) continue;
                seenNodeIds.add(peer.nodeId);
            }
            result.push(peerId);
        }
        return result;
    }

    _getMinQuorumSync() {
        const election = this._shardElections.get(0);
        if (election) return election._quorum();
        return Math.floor(this._getActivePeerIds().length / 2) + 1 + 1;
    }

    async _waitForShardLeader(shardId, maxWaitMs) {
        const deadline = this._nowMs() + maxWaitMs;
        while (this._nowMs() < deadline) {
            const leaderId = this._shardLeaders.get(shardId);
            if (leaderId) return leaderId;
            await new Promise(r => this._setTimeout(r, 100));
        }
        return this._shardLeaders.get(shardId) ?? null;
    }

    _abortFollowerTxnsForPeer(peerId) {
        for (const [writeId, txn] of this._followerTxns) {
            if (txn.peerId === peerId) {
                this._clearTimeout(txn.timeoutHandle);
                this._followerTxns.delete(writeId);
                try { this.db.rollback(); } catch (_) {}
            }
        }
    }
}
