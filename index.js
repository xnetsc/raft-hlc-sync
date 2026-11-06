/**
 * sync-lib — 通用分布式数据同步 & 2PC 算法模块
 *
 * 纯算法实现，不依赖任何具体的传输层（WebSocket/gRPC/...）和数据库（SQLite/PG/MySQL/...）。
 * 所有外部能力通过接口注入。
 *
 * ═══════════════════════════════════════════════════════════════════
 *  快速开始
 * ═══════════════════════════════════════════════════════════════════
 *
 *  import { SyncEngine } from './sync-lib/index.js';
 *
 *  // 1. 外部实现 DatabaseAdapter 基础查询接口
 *  const db = {
 *      run(sql, params) { ... },  // → { changes: number }
 *      get(sql, params) { ... },  // → Object | null
 *      all(sql, params) { ... },  // → Object[]
 *      exec(sql)        { ... },  // → void
 *  };
 *
 *  // 2. 创建引擎，注入所有外部能力
 *  const engine = new SyncEngine({
 *      nodeId: 'node-1',
 *      db,
 *
 *      // ── 方言（推荐）──
 *      // 指定内置方言后，db 上缺失的 SQL 方法由 dialects.js 自动填充：
 *      //   beginTransaction / commit / rollback
 *      //   upsertSQL / infraSchemaSQL / triggersSQL / dropTriggersSQL
 *      // 支持：'sqlite' | 'postgresql' | 'postgres' | 'pg' | 'mysql'
 *      dialect: 'sqlite',
 *
 *      // ── 方言方法优先级 ──
 *      // db 上已有的方法 > dialect 自动填充 > 报错（未指定 dialect 且方法缺失时）
 *      // 即：用户在 db 上手动注入的方法始终优先于内置方言实现。
 *      // 例如：db.upsertSQL = (table, cols, keys) => '...'  // 覆盖内置 SQLite upsert
 *
 *      // ── 必须回调 ──
 *      onSendToPeer:   (peerId, msg) => transport.send(peerId, msg),
 *      onClosePeer:    (peerId, reason) => transport.close(peerId),
 *      onLeaderChanged: (shardId, leaderId, isLocal) => { ... },
 *      onExecuteProxyRequest: async (payload) => handleRequest(payload),
 *        // ⚠️ 必须注册：任何节点都可能当选 Leader，必须能处理 Follower 转发的写请求
 *
 *      // ── 可选回调 ──
 *      onWriteCompleted:       () => engine.notifyLocalWrite(),
 *      onError:                (ctx, err) => logger.error(ctx, err),
 *
 *      // ── 可选：外部变更检测回调（与 SQL 触发器并列，可并存）──
 *      // 设计：sync-lib 驱动订阅，外部被动响应。外部只提供这一个回调，既不需要、也无法
 *      // 直接知道 sync-lib 关心哪些表——它只能从回调参数里得知"本次被要求监听的表 + where"。
 *      // initChangeSubscriptions() 会遍历所有 registerTable() 声明的表，对每张表调用此回调一次：
 *      //   subscribeToChanges({ table, where, keyColumns, dataColumns, onChanges })
 *      // 其中 where 来源于 registerTable(name, { where }) 的配置。
 *      // 外部在满足条件的行变更时通过 onChanges 回传：
 *      //   onChanges(eventTypes: ('INSERT'|'UPDATE'|'DELETE')[],
 *      //             eventInfos: { tableName, rowKey, rowData|null, hlc }[])
 *      // 事件会被写入 _sync_log，交由已有的同步流程分发。
 *      // SQL 变更记录触发器和本回调二者都未配置时 start() 报错。
 *      subscribeToChanges: ({ table, where, onChanges }) => { ... },
 *
 *      // ── 可选配置（全部有默认值）──
 *      numShards:          16,
 *      syncBatchSize:      500,
 *      fullSyncChunkSize:  500,     // 全量重同步每页条目数（用于大表分页传输）
 *      enableFullResync:   true,    // 握手检测到 peer 落后于 _sync_log 起始 id 时是否触发全量重同步
 *      fullSyncProgressTtlMs: 86400000, // _full_sync_progress 孤儿记录 TTL（24h，对端永久失联的兜底清理）
 *      heartbeatTimeoutMs: 45000,
 *      heartbeatIntervalMs:15000,   // tickHeartbeat 调度间隔（同时也是双向对时间隔）
 *      pullIntervalMs:     10000,   // tickPull 调度间隔
 *      cleanupIntervalMs:  3600000, // tickCleanup 调度间隔
 *      timeSyncWindowSize: 8,       // 每个 peer 的 RTT 滑动窗口大小（默认 8）
 *      proxyWaitMs:        6000,
 *      proxyTimeoutMs:     10000,
 *      prepareTimeoutMs:   5000,
 *      followerTxnTimeoutMs: 15000,
 *      getMinQuorum:       () => 2,
 *      generateUpsertSQL:  (table, cols, keys) => '...',
 *
 *      // ── 时间函数注入（必须）──
 *      // 返回 { value: number, unit: 's'|'ms'|'us'|'ns' }，引擎内部统一转换为毫秒
 *      getNow: () => ({ value: Date.now(), unit: 'ms' }),
 *
 *      // ── 定时器注入（必须）──
 *      // 引擎内部所有定时器均通过此注入调用
 *      timerAPI: {
 *          setTimeout,
 *          clearTimeout,
 *          setInterval,
 *          clearInterval,
 *      },
 *  });
 *
 *  // 3. 注册业务表
 *  engine.registerTable('users', {
 *      keyColumns:  ['user_id'],
 *      dataColumns: ['user_id', 'name', 'email', 'created_at', '_hlc'],
 *      validator:   (row) => { if (!row.user_id) throw new Error('missing user_id'); },
 *  });
 *
 *  // 4. 初始化 Schema & 触发器
 *  engine.initSchema();     // 创建 _sync_log, _sync_peers, _tombstones
 *  engine.initTriggers();   // 创建同步触发器（HLC 完整性 + 变更记录，可传 {changeLog:false} 等）
 *  // 若使用外部变更订阅回调：engine.initChangeSubscriptions();
 *  engine.start();
 *
 *  // 5. 外部传输层对接
 *  transport.on('connected',    (peerId, dir) => engine.peerConnected(peerId, { direction: dir }));
 *  transport.on('message',      (peerId, raw) => engine.receiveMessage(peerId, raw));
 *  transport.on('disconnected', (peerId)      => engine.peerDisconnected(peerId));
 *
 *  // 6. start() 后引擎自动调度 tickPull / tickCleanup / tickHeartbeat
 *  //    间隔通过 pullIntervalMs / cleanupIntervalMs / heartbeatIntervalMs 配置
 *  //    定时器实现通过 timerAPI 注入（默认使用全局 setTimeout/setInterval）
 *
 *  // 7. 写操作：已注册表的所有增删改必须通过 engine.write() 或手动事务包裹
 *  //    ⚠️ 不要直接 db.run('INSERT ...') — 引擎需要管理事务和 2PC 同步
 *  const result = await engine.write(async (db) => {
 *      const ts = engine.hlc.tick();
 *      db.run('INSERT INTO users ...', [ts, ...]);
 *      return { userId: 'u1' };
 *  }, userId);
 *
 * ═══════════════════════════════════════════════════════════════════
 *  2PC 写操作流程（多节点强一致）
 * ═══════════════════════════════════════════════════════════════════
 *
 *  // 高阶 API（推荐）：引擎自动处理 BEGIN/COMMIT/ROLLBACK 和 2PC 协调
 *  const result = await engine.write(async (db) => {
 *      const ts = engine.hlc.tick();
 *      db.run('INSERT INTO users ...', [ts, ...]);
 *      return { userId: 'u1' };  // 返回值透传
 *  }, userId);  // shardKey 用于路由
 *
 *  // 低阶 API（需要精细控制时使用）
 *  const writeId = crypto.randomUUID();
 *  engine.beginManualTransaction(writeId);
 *  try {
 *      db.run('INSERT INTO users ...', [...]);  // 业务写操作
 *      const entries = engine.getManualTransactionEntries(writeId);
 *      const shardId = engine.getShardId(userId);
 *      const term = engine.getStatus().shards[shardId].term;
 *      await engine.waitForPrepareAck(writeId, entries, term, shardId);
 *      engine.commitManualTransaction(writeId);
 *      engine.broadcastCommit(writeId);
 *  } catch (err) {
 *      engine.rollbackManualTransaction(writeId);
 *      engine.broadcastAbort(writeId, err.message);
 *      throw err;
 *  }
 *
 * ═══════════════════════════════════════════════════════════════════
 *  握手 Gap 检测 + 全量重同步（防止 peer 长期离线后的静默丢数据）
 * ═══════════════════════════════════════════════════════════════════
 *
 *  背景：_sync_log 会被 tickCleanup 按 TTL 回收。若某 peer 离线时间超过 TTL，
 *  其 lastSyncId 可能小于本地 _sync_log 的最小 id —— 此时按普通增量拉取会
 *  "WHERE id > lastSyncId" 静默跳过已清理的历史条目，造成数据丢失。
 *
 *  解决方案（enableFullResync=true 默认开启）：
 *    1. 握手消息携带 (lastSeq, minLogId)。
 *    2. 对端收到后比较 peerLastSeq < localMinLogId - 1 → 触发全量重同步。
 *    3. 发送方逐表分页扫描(fullSyncChunkSize) + 墓碑扫描，依次发送
 *       full_sync_rows / full_sync_tombstones，末尾 full_sync_complete。
 *    4. 接收方每收到一页，在同一事务内：应用数据 + 写入 _full_sync_progress，
 *       保证数据与进度一致；单页独立提交，已落盘部分不因后续异常丢失。
 *    5. 断点续传：接收方下次握手时若发现 _full_sync_progress 有残留记录，
 *       主动发出 full_sync_resume_req{resume:{phase,tableName,lastKey}}，
 *       发送方基于该游标继续扫描（发送方无状态，每次从 DB 重新生成流）。
 *    6. 全量流以 watermarkSeq = MAX(id) FROM _sync_log 作为水位；完成后接收
 *       方将自身 lastSyncId 推进到 watermarkSeq，回到增量路径。
 *    7. 应用全量行使用 HLC LWW + 墓碑优先，天然幂等；重传/重连不会覆盖更新的本地数据。
 *    8. 孤儿记录清理：对端永久失联时 _full_sync_progress 记录留存，由 tickCleanup
 *       按 fullSyncProgressTtlMs（默认 24h）删除。
 *
 * ═══════════════════════════════════════════════════════════════════
 *  模块结构
 * ═══════════════════════════════════════════════════════════════════
 *
 *  sync-lib/
 *  ├── index.js            ← 统一导出
 *  ├── hlc.js              ← HLC 混合逻辑时钟（纯算法）
 *  ├── leader-election.js  ← Raft 简化版选举状态机（onXXX 回调）
 *  ├── sync-protocol.js    ← 消息编解码 + 幂等数据应用（DatabaseAdapter 接口）
 *  ├── dialects.js         ← 内置 SQL 方言（SQLite/PostgreSQL/MySQL），所有 SQL 逻辑的唯一来源
 *  └── sync-engine.js      ← 主引擎（组合以上模块，对外统一入口）
 */

// 主引擎
export { SyncEngine } from './sync-engine.js';

// 底层组件（高级用途 / 单独使用）
export { HLC, timeValueToMs, SUPPORTED_TIME_UNITS } from './hlc.js';
export { LeaderElection } from './leader-election.js';

// 内置方言
export { DIALECTS, applyDialect, validateDialectMethods } from './dialects.js';

// 协议层工具（高级用途：自定义消息处理）
export {
    // 消息构造
    makeHandshake,
    makeSync,
    makeAck,
    makePullRequest,
    makePing,
    makePong,
    makeLeaderHeartbeat,
    makeVoteReq,
    makeVoteAck,
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
    // 数据应用
    applyEntries,
    applyEntriesInTransaction,
    applyFullSyncRows,
    applyFullSyncTombstones,
    // sync_log 操作
    readSyncLog,
    getMaxSeq,
    getMinLogId,
    cleanSyncLog,
    logChange,
    scanTableRowsPage,
    scanTombstonesPage,
    // 全量重同步 needs/claims（接收方侧区间调度）
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
    // peer 操作
    getPeerLastSyncId,
    getPeerWatermark,
    updatePeerSyncId,
    // 墓碑
    cleanTombstones,
} from './sync-protocol.js';
