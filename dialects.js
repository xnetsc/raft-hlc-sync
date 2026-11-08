/**
 * 内置 SQL 方言定义
 *
 * 支持 sqlite、postgresql、mysql 三种方言。
 * 每种方言提供以下 DatabaseAdapter 方法的默认实现：
 *   - beginTransaction / commit / rollback（通过 SQL 字符串 + db.exec）
 *   - upsertSQL
 *   - infraSchemaSQL
 *   - triggersSQL / dropTriggersSQL
 *
 * 使用方式：
 *   new SyncEngine({ dialect: 'sqlite', db: { run, get, all, exec }, ... })
 *
 * 指定 dialect 后，db 上缺失的方言方法由内置实现自动填充。
 * db 上已有的方法优先级更高（不会被覆盖）。
 *
 * 若不指定 dialect 或指定不认识的值，所有方言方法必须由 db 自行实现，否则报错。
 */

// ═══════════════════════════════════════════════════
//  SQLite
// ═══════════════════════════════════════════════════

const SQLITE = {
    beginTransactionSQL: 'BEGIN IMMEDIATE',
    commitSQL: 'COMMIT',
    rollbackSQL: 'ROLLBACK',

    upsertSQL(table, columns, _keyColumns) {
        const cols = columns.join(', ');
        const ph = columns.map(() => '?').join(', ');
        return `INSERT OR REPLACE INTO ${table} (${cols}) VALUES (${ph})`;
    },

    infraSchemaSQL() {
        return [
            `CREATE TABLE IF NOT EXISTS _sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    row_key TEXT NOT NULL,
    row_data TEXT,
    _hlc TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
)`,
            `CREATE INDEX IF NOT EXISTS idx_sync_log_id ON _sync_log (id)`,
            `CREATE TABLE IF NOT EXISTS _sync_peers (
    peer_key TEXT NOT NULL,
    last_sync_id INTEGER NOT NULL DEFAULT 0,
    last_sync_at TEXT,
    PRIMARY KEY (peer_key)
)`,
            `CREATE TABLE IF NOT EXISTS _sync_pull_offsets (
    peer_key TEXT NOT NULL,
    last_pulled_seq INTEGER NOT NULL DEFAULT 0,
    last_sync_at TEXT,
    PRIMARY KEY (peer_key)
)`,
            `CREATE TABLE IF NOT EXISTS _tombstones (
    table_name TEXT NOT NULL,
    row_key TEXT NOT NULL,
    _hlc TEXT NOT NULL,
    PRIMARY KEY (table_name, row_key)
)`,
            `CREATE TABLE IF NOT EXISTS _full_sync_needs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phase TEXT NOT NULL,
    table_name TEXT,
    range_start TEXT,
    range_end TEXT,
    watermark_seq INTEGER NOT NULL,
    updated_at TEXT NOT NULL
)`,
            `CREATE INDEX IF NOT EXISTS idx_full_sync_needs_scope ON _full_sync_needs (phase, table_name)`,
            `CREATE TABLE IF NOT EXISTS _full_sync_claims (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phase TEXT NOT NULL,
    table_name TEXT,
    range_start TEXT,
    range_end TEXT,
    peer_node_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL
)`,
            `CREATE INDEX IF NOT EXISTS idx_full_sync_claims_scope ON _full_sync_claims (phase, table_name)`,
            `CREATE INDEX IF NOT EXISTS idx_full_sync_claims_peer ON _full_sync_claims (peer_node_id)`,
        ];
    },

    hlcTriggersSQL(tableName, _def) {
        return [
            // BEFORE INSERT：强制要求 _hlc 非空且非默认值 '0'
            `
CREATE TRIGGER IF NOT EXISTS _sync_trg_${tableName}_hlc_insert
BEFORE INSERT ON ${tableName}
BEGIN
    SELECT RAISE(ABORT, 'sync-lib: _hlc is required for INSERT on ${tableName}')
    WHERE NEW._hlc IS NULL OR NEW._hlc = '' OR NEW._hlc = '0';
END`,
            // BEFORE UPDATE：强制要求 _hlc 非空且非默认值 '0'，且必须严格大于旧值（防止时钟回退）
            `
CREATE TRIGGER IF NOT EXISTS _sync_trg_${tableName}_hlc_update
BEFORE UPDATE ON ${tableName}
BEGIN
    SELECT RAISE(ABORT, 'sync-lib: _hlc is required for UPDATE on ${tableName}')
    WHERE NEW._hlc IS NULL OR NEW._hlc = '' OR NEW._hlc = '0';
    SELECT RAISE(ABORT, 'sync-lib: _hlc must advance on UPDATE on ${tableName}')
    WHERE NEW._hlc <= OLD._hlc;
END`,
        ];
    },

    changeLogTriggersSQL(tableName, def) {
        const blobSet = new Set(def.blobColumns || []);

        const keyExpr = (ref) => {
            const parts = def.keyColumns.map((col) => `'${col}', ${ref}.${col}`);
            return `json_object(${parts.join(', ')})`;
        };

        const dataExpr = (ref) => {
            const parts = def.dataColumns.map((col) => {
                if (blobSet.has(col)) return `'${col}', hex(${ref}.${col})`;
                return `'${col}', ${ref}.${col}`;
            });
            return `json_object(${parts.join(', ')})`;
        };

        return [
            `
CREATE TRIGGER IF NOT EXISTS _sync_trg_${tableName}_insert
AFTER INSERT ON ${tableName}
BEGIN
    INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
    VALUES ('${tableName}', 'INSERT', ${keyExpr('NEW')}, ${dataExpr('NEW')}, NEW._hlc);
END`,
            `
CREATE TRIGGER IF NOT EXISTS _sync_trg_${tableName}_update
AFTER UPDATE ON ${tableName}
BEGIN
    INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
    VALUES ('${tableName}', 'UPDATE', ${keyExpr('NEW')}, ${dataExpr('NEW')}, NEW._hlc);
END`,
            `
CREATE TRIGGER IF NOT EXISTS _sync_trg_${tableName}_delete
AFTER DELETE ON ${tableName}
BEGIN
    INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
    VALUES ('${tableName}', 'DELETE', ${keyExpr('OLD')}, NULL, OLD._hlc);
END`,
        ];
    },

    triggersSQL(tableName, def) {
        return [
            ...SQLITE.hlcTriggersSQL(tableName, def),
            ...SQLITE.changeLogTriggersSQL(tableName, def),
        ];
    },

    dropHlcTriggersSQL(tableName) {
        return [
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc_insert`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc_update`,
        ];
    },

    dropChangeLogTriggersSQL(tableName) {
        return [
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_insert`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_update`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_delete`,
        ];
    },

    dropTriggersSQL(tableName) {
        return [
            ...SQLITE.dropHlcTriggersSQL(tableName),
            ...SQLITE.dropChangeLogTriggersSQL(tableName),
        ];
    },
};

// ═══════════════════════════════════════════════════
//  PostgreSQL
// ═══════════════════════════════════════════════════

const POSTGRESQL = {
    beginTransactionSQL: 'BEGIN',
    commitSQL: 'COMMIT',
    rollbackSQL: 'ROLLBACK',

    upsertSQL(table, columns, keyColumns) {
        const cols = columns.join(', ');
        const ph = columns.map(() => '?').join(', ');
        const keys = keyColumns.join(', ');
        const updates = columns
            .filter(c => !keyColumns.includes(c))
            .map(c => `${c} = EXCLUDED.${c}`);
        return updates.length === 0
            ? `INSERT INTO ${table} (${cols}) VALUES (${ph}) ON CONFLICT (${keys}) DO NOTHING`
            : `INSERT INTO ${table} (${cols}) VALUES (${ph}) ON CONFLICT (${keys}) DO UPDATE SET ${updates.join(', ')}`;
    },

    infraSchemaSQL() {
        return [
            `CREATE TABLE IF NOT EXISTS _sync_log (
                id BIGSERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                row_key TEXT NOT NULL,
                row_data TEXT,
                _hlc TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )`,
            `CREATE INDEX IF NOT EXISTS idx_sync_log_id ON _sync_log (id)`,
            `CREATE TABLE IF NOT EXISTS _sync_peers (
                peer_key TEXT PRIMARY KEY,
                last_sync_id BIGINT DEFAULT 0,
                last_sync_at TIMESTAMPTZ
            )`,
            `CREATE TABLE IF NOT EXISTS _sync_pull_offsets (
                peer_key TEXT PRIMARY KEY,
                last_pulled_seq BIGINT DEFAULT 0,
                last_sync_at TIMESTAMPTZ
            )`,
            `CREATE TABLE IF NOT EXISTS _tombstones (
                table_name TEXT NOT NULL,
                row_key TEXT NOT NULL,
                _hlc TEXT NOT NULL,
                PRIMARY KEY (table_name, row_key)
            )`,
            `CREATE TABLE IF NOT EXISTS _full_sync_needs (
                id BIGSERIAL PRIMARY KEY,
                phase TEXT NOT NULL,
                table_name TEXT,
                range_start TEXT,
                range_end TEXT,
                watermark_seq BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )`,
            `CREATE INDEX IF NOT EXISTS idx_full_sync_needs_scope ON _full_sync_needs (phase, table_name)`,
            `CREATE TABLE IF NOT EXISTS _full_sync_claims (
                id BIGSERIAL PRIMARY KEY,
                phase TEXT NOT NULL,
                table_name TEXT,
                range_start TEXT,
                range_end TEXT,
                peer_node_id TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            )`,
            `CREATE INDEX IF NOT EXISTS idx_full_sync_claims_scope ON _full_sync_claims (phase, table_name)`,
            `CREATE INDEX IF NOT EXISTS idx_full_sync_claims_peer ON _full_sync_claims (peer_node_id)`,
        ];
    },

    hlcTriggersSQL(tableName, _def) {
        return [
            // HLC 校验函数：INSERT/UPDATE 时强制要求 _hlc 有效且单调递增
            `CREATE OR REPLACE FUNCTION _sync_trg_${tableName}_hlc_fn() RETURNS TRIGGER AS $$
BEGIN
    IF NEW._hlc IS NULL OR NEW._hlc = '' OR NEW._hlc = '0' THEN
        RAISE EXCEPTION 'sync-lib: _hlc is required for % on ${tableName}', TG_OP;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW._hlc <= OLD._hlc THEN
        RAISE EXCEPTION 'sync-lib: _hlc must advance on UPDATE on ${tableName}';
    END IF;
    RETURN NEW;
END; $$ LANGUAGE plpgsql`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc ON ${tableName}`,
            `CREATE TRIGGER _sync_trg_${tableName}_hlc
BEFORE INSERT OR UPDATE ON ${tableName}
FOR EACH ROW EXECUTE FUNCTION _sync_trg_${tableName}_hlc_fn()`,
        ];
    },

    changeLogTriggersSQL(tableName, def) {
        const blobSet = new Set(def.blobColumns || []);

        const keyExprNew = def.keyColumns.map(c => `'${c}', NEW.${c}`).join(', ');
        const keyExprOld = def.keyColumns.map(c => `'${c}', OLD.${c}`).join(', ');

        const dataExpr = def.dataColumns.map(c => {
            if (blobSet.has(c)) return `'${c}', encode(NEW.${c}, 'hex')`;
            return `'${c}', NEW.${c}`;
        }).join(', ');

        return [
            // 变更记录函数：写入 _sync_log
            `CREATE OR REPLACE FUNCTION _sync_trg_${tableName}_fn() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
        VALUES ('${tableName}', 'DELETE', json_build_object(${keyExprOld})::text, NULL, OLD._hlc);
        RETURN OLD;
    ELSE
        INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
        VALUES ('${tableName}', TG_OP, json_build_object(${keyExprNew})::text, json_build_object(${dataExpr})::text, NEW._hlc);
        RETURN NEW;
    END IF;
END; $$ LANGUAGE plpgsql`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName} ON ${tableName}`,
            `CREATE TRIGGER _sync_trg_${tableName}
AFTER INSERT OR UPDATE OR DELETE ON ${tableName}
FOR EACH ROW EXECUTE FUNCTION _sync_trg_${tableName}_fn()`,
        ];
    },

    triggersSQL(tableName, def) {
        return [
            ...POSTGRESQL.hlcTriggersSQL(tableName, def),
            ...POSTGRESQL.changeLogTriggersSQL(tableName, def),
        ];
    },

    dropHlcTriggersSQL(tableName) {
        return [
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc ON ${tableName}`,
            `DROP FUNCTION IF EXISTS _sync_trg_${tableName}_hlc_fn`,
        ];
    },

    dropChangeLogTriggersSQL(tableName) {
        return [
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName} ON ${tableName}`,
            `DROP FUNCTION IF EXISTS _sync_trg_${tableName}_fn`,
        ];
    },

    dropTriggersSQL(tableName) {
        return [
            ...POSTGRESQL.dropHlcTriggersSQL(tableName),
            ...POSTGRESQL.dropChangeLogTriggersSQL(tableName),
        ];
    },
};

// ═══════════════════════════════════════════════════
//  MySQL
// ═══════════════════════════════════════════════════

const MYSQL = {
    beginTransactionSQL: 'START TRANSACTION',
    commitSQL: 'COMMIT',
    rollbackSQL: 'ROLLBACK',

    upsertSQL(table, columns, keyColumns) {
        const cols = columns.join(', ');
        const ph = columns.map(() => '?').join(', ');
        const updates = columns
            .filter(c => !keyColumns.includes(c))
            .map(c => `${c} = VALUES(${c})`);
        return updates.length === 0
            ? `INSERT IGNORE INTO ${table} (${cols}) VALUES (${ph})`
            : `INSERT INTO ${table} (${cols}) VALUES (${ph}) ON DUPLICATE KEY UPDATE ${updates.join(', ')}`;
    },

    infraSchemaSQL() {
        return [
            `CREATE TABLE IF NOT EXISTS _sync_log (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                operation VARCHAR(10) NOT NULL,
                row_key TEXT NOT NULL,
                row_data LONGTEXT,
                _hlc VARCHAR(64) NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )`,
            `CREATE TABLE IF NOT EXISTS _sync_peers (
                peer_key VARCHAR(255) PRIMARY KEY,
                last_sync_id BIGINT DEFAULT 0,
                last_sync_at DATETIME
            )`,
            `CREATE TABLE IF NOT EXISTS _sync_pull_offsets (
                peer_key VARCHAR(255) PRIMARY KEY,
                last_pulled_seq BIGINT DEFAULT 0,
                last_sync_at DATETIME
            )`,
            `CREATE TABLE IF NOT EXISTS _tombstones (
                table_name VARCHAR(255) NOT NULL,
                row_key VARCHAR(1024) NOT NULL,
                _hlc VARCHAR(64) NOT NULL,
                PRIMARY KEY (table_name, row_key(255))
            )`,
            `CREATE TABLE IF NOT EXISTS _full_sync_needs (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                phase VARCHAR(16) NOT NULL,
                table_name VARCHAR(255),
                range_start TEXT,
                range_end TEXT,
                watermark_seq BIGINT NOT NULL,
                updated_at DATETIME NOT NULL,
                INDEX idx_full_sync_needs_scope (phase, table_name)
            )`,
            `CREATE TABLE IF NOT EXISTS _full_sync_claims (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                phase VARCHAR(16) NOT NULL,
                table_name VARCHAR(255),
                range_start TEXT,
                range_end TEXT,
                peer_node_id VARCHAR(255) NOT NULL,
                expires_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                INDEX idx_full_sync_claims_scope (phase, table_name),
                INDEX idx_full_sync_claims_peer (peer_node_id)
            )`,
        ];
    },

    hlcTriggersSQL(tableName, _def) {
        return [
            // HLC 校验触发器：INSERT 时强制要求 _hlc 有效
            `CREATE TRIGGER _sync_trg_${tableName}_hlc_insert
BEFORE INSERT ON ${tableName} FOR EACH ROW
BEGIN
    IF NEW._hlc IS NULL OR NEW._hlc = '' OR NEW._hlc = '0' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sync-lib: _hlc is required for INSERT on ${tableName}';
    END IF;
END`,
            // HLC 校验触发器：UPDATE 时强制要求 _hlc 有效且单调递增
            `CREATE TRIGGER _sync_trg_${tableName}_hlc_update
BEFORE UPDATE ON ${tableName} FOR EACH ROW
BEGIN
    IF NEW._hlc IS NULL OR NEW._hlc = '' OR NEW._hlc = '0' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sync-lib: _hlc is required for UPDATE on ${tableName}';
    END IF;
    IF NEW._hlc <= OLD._hlc THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sync-lib: _hlc must advance on UPDATE on ${tableName}';
    END IF;
END`,
        ];
    },

    changeLogTriggersSQL(tableName, def) {
        const blobSet = new Set(def.blobColumns || []);

        const keyExpr = (ref) => def.keyColumns.map(c => `'${c}', ${ref}.${c}`).join(', ');
        const dataExpr = (ref) => def.dataColumns.map(c => {
            if (blobSet.has(c)) return `'${c}', HEX(${ref}.${c})`;
            return `'${c}', ${ref}.${c}`;
        }).join(', ');

        return [
            `CREATE TRIGGER _sync_trg_${tableName}_insert
AFTER INSERT ON ${tableName} FOR EACH ROW
INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
VALUES ('${tableName}', 'INSERT', JSON_OBJECT(${keyExpr('NEW')}), JSON_OBJECT(${dataExpr('NEW')}), NEW._hlc)`,
            `CREATE TRIGGER _sync_trg_${tableName}_update
AFTER UPDATE ON ${tableName} FOR EACH ROW
INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
VALUES ('${tableName}', 'UPDATE', JSON_OBJECT(${keyExpr('NEW')}), JSON_OBJECT(${dataExpr('NEW')}), NEW._hlc)`,
            `CREATE TRIGGER _sync_trg_${tableName}_delete
AFTER DELETE ON ${tableName} FOR EACH ROW
INSERT INTO _sync_log (table_name, operation, row_key, row_data, _hlc)
VALUES ('${tableName}', 'DELETE', JSON_OBJECT(${keyExpr('OLD')}), NULL, OLD._hlc)`,
        ];
    },

    triggersSQL(tableName, def) {
        return [
            ...MYSQL.hlcTriggersSQL(tableName, def),
            ...MYSQL.changeLogTriggersSQL(tableName, def),
        ];
    },

    dropHlcTriggersSQL(tableName) {
        return [
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc_insert`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_hlc_update`,
        ];
    },

    dropChangeLogTriggersSQL(tableName) {
        return [
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_insert`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_update`,
            `DROP TRIGGER IF EXISTS _sync_trg_${tableName}_delete`,
        ];
    },

    dropTriggersSQL(tableName) {
        return [
            ...MYSQL.dropHlcTriggersSQL(tableName),
            ...MYSQL.dropChangeLogTriggersSQL(tableName),
        ];
    },
};

// ═══════════════════════════════════════════════════
//  公共接口
// ═══════════════════════════════════════════════════

/** 所有内置方言 */
export const DIALECTS = {
    sqlite: SQLITE,
    postgresql: POSTGRESQL,
    postgres: POSTGRESQL,   // 别名
    pg: POSTGRESQL,         // 别名
    mysql: MYSQL,
};

/** 需要方言提供的全部方法名 */
const DIALECT_METHODS = [
    'beginTransaction', 'commit', 'rollback',
    'upsertSQL', 'infraSchemaSQL',
    'triggersSQL', 'dropTriggersSQL',
];

/**
 * 将内置方言应用到 db 适配器。
 *
 * - 已有的方法不覆盖（用户显式注入的优先级最高）。
 * - beginTransaction / commit / rollback 通过 db.exec(sql) 桥接。
 *
 * @param {object} db - DatabaseAdapter 实例
 * @param {string} dialectName - 方言名称
 */
export function applyDialect(db, dialectName) {
    const dialect = DIALECTS[dialectName];
    if (!dialect) {
        const supported = Object.keys(DIALECTS).filter(k => !['postgres', 'pg'].includes(k)).join(', ');
        throw new Error(
            `Unknown dialect "${dialectName}". Supported: ${supported}. ` +
            `Or omit dialect and implement all adapter methods manually.`
        );
    }

    // 事务控制：通过 SQL 字符串 + db.exec() 桥接
    if (typeof db.beginTransaction !== 'function') {
        db.beginTransaction = () => db.exec(dialect.beginTransactionSQL);
    }
    if (typeof db.commit !== 'function') {
        db.commit = () => db.exec(dialect.commitSQL);
    }
    if (typeof db.rollback !== 'function') {
        db.rollback = () => db.exec(dialect.rollbackSQL);
    }

    // 函数方法：直接挂载
    if (typeof db.upsertSQL !== 'function') {
        db.upsertSQL = dialect.upsertSQL;
    }
    if (typeof db.infraSchemaSQL !== 'function') {
        db.infraSchemaSQL = dialect.infraSchemaSQL;
    }
    if (typeof db.triggersSQL !== 'function') {
        db.triggersSQL = dialect.triggersSQL;
    }
    if (typeof db.dropTriggersSQL !== 'function') {
        db.dropTriggersSQL = dialect.dropTriggersSQL;
    }
    if (typeof db.hlcTriggersSQL !== 'function') {
        db.hlcTriggersSQL = dialect.hlcTriggersSQL;
    }
    if (typeof db.changeLogTriggersSQL !== 'function') {
        db.changeLogTriggersSQL = dialect.changeLogTriggersSQL;
    }
    if (typeof db.dropHlcTriggersSQL !== 'function') {
        db.dropHlcTriggersSQL = dialect.dropHlcTriggersSQL;
    }
    if (typeof db.dropChangeLogTriggersSQL !== 'function') {
        db.dropChangeLogTriggersSQL = dialect.dropChangeLogTriggersSQL;
    }
}

/**
 * 校验 db 适配器是否具备所有必需的方言方法。
 * 如果缺失任何方法，抛出包含缺失列表的错误。
 *
 * @param {object} db - DatabaseAdapter 实例
 */
export function validateDialectMethods(db) {
    const missing = DIALECT_METHODS.filter(m => typeof db[m] !== 'function');
    if (missing.length > 0) {
        throw new Error(
            `DatabaseAdapter missing required methods: ${missing.join(', ')}. ` +
            `Either specify a dialect (e.g. dialect: 'sqlite') or implement these methods on your db adapter.`
        );
    }
}
