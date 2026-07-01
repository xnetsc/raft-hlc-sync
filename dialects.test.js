import { describe, it, expect } from 'vitest';
import { DIALECTS, applyDialect, validateDialectMethods } from './dialects.js';

// ═══════════════════════════════════════════════════
//  DIALECTS registry
// ═══════════════════════════════════════════════════

describe('DIALECTS', () => {
    it('has sqlite dialect', () => {
        expect(DIALECTS.sqlite).toBeDefined();
    });

    it('has postgresql dialect (+ aliases)', () => {
        expect(DIALECTS.postgresql).toBeDefined();
        expect(DIALECTS.postgres).toBe(DIALECTS.postgresql);
        expect(DIALECTS.pg).toBe(DIALECTS.postgresql);
    });

    it('has mysql dialect', () => {
        expect(DIALECTS.mysql).toBeDefined();
    });
});

// ═══════════════════════════════════════════════════
//  SQLite dialect
// ═══════════════════════════════════════════════════

describe('SQLite dialect', () => {
    const sqlite = DIALECTS.sqlite;

    it('generates upsertSQL with INSERT OR REPLACE', () => {
        const sql = sqlite.upsertSQL('users', ['user_id', 'name', '_hlc'], ['user_id']);
        expect(sql).toContain('INSERT OR REPLACE');
        expect(sql).toContain('users');
        expect(sql).toContain('user_id, name, _hlc');
    });

    it('generates infraSchemaSQL statements', () => {
        const stmts = sqlite.infraSchemaSQL();
        expect(stmts[0]).toContain('_sync_log');
        expect(stmts.some(s => s.includes('_sync_peers'))).toBe(true);
        expect(stmts.some(s => s.includes('_tombstones'))).toBe(true);
        expect(stmts.some(s => s.includes('_full_sync_needs'))).toBe(true);
        expect(stmts.some(s => s.includes('_full_sync_claims'))).toBe(true);
    });

    it('generates triggersSQL with 5 triggers', () => {
        const stmts = sqlite.triggersSQL('users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', 'name', '_hlc'],
            blobColumns: [],
        });
        expect(stmts.length).toBe(5);
        for (const s of stmts) {
            expect(s).toContain('_sync_trg_users');
        }
    });

    it('generates triggersSQL with blob hex handling', () => {
        const stmts = sqlite.triggersSQL('files', {
            keyColumns: ['file_id'],
            dataColumns: ['file_id', 'content', '_hlc'],
            blobColumns: ['content'],
        });
        const afterInsert = stmts.find(s => s.includes('AFTER INSERT'));
        expect(afterInsert).toContain('hex(NEW.content)');
    });

    it('generates dropTriggersSQL with 5 drop statements', () => {
        const stmts = sqlite.dropTriggersSQL('users');
        expect(stmts.length).toBe(5);
        for (const s of stmts) {
            expect(s).toContain('DROP TRIGGER IF EXISTS');
        }
    });
});

// ═══════════════════════════════════════════════════
//  PostgreSQL dialect
// ═══════════════════════════════════════════════════

describe('PostgreSQL dialect', () => {
    const pg = DIALECTS.postgresql;

    it('generates upsertSQL with ON CONFLICT', () => {
        const sql = pg.upsertSQL('users', ['user_id', 'name', '_hlc'], ['user_id']);
        expect(sql).toContain('ON CONFLICT (user_id)');
        expect(sql).toContain('DO UPDATE SET');
    });

    it('generates upsertSQL with DO NOTHING when only key columns', () => {
        const sql = pg.upsertSQL('t', ['id'], ['id']);
        expect(sql).toContain('DO NOTHING');
    });

    it('generates infraSchemaSQL', () => {
        const stmts = pg.infraSchemaSQL();
        expect(stmts[0]).toContain('BIGSERIAL');
        expect(stmts.some(s => s.includes('_full_sync_needs'))).toBe(true);
        expect(stmts.some(s => s.includes('_full_sync_claims'))).toBe(true);
    });

    it('generates triggersSQL with PL/pgSQL functions', () => {
        const stmts = pg.triggersSQL('users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', 'name', '_hlc'],
            blobColumns: [],
        });
        expect(stmts.length).toBe(6);
        expect(stmts.some(s => s.includes('plpgsql'))).toBe(true);
    });

    it('generates dropTriggersSQL', () => {
        const stmts = pg.dropTriggersSQL('users');
        expect(stmts.length).toBe(4);
        expect(stmts.some(s => s.includes('DROP FUNCTION'))).toBe(true);
    });
});

// ═══════════════════════════════════════════════════
//  MySQL dialect
// ═══════════════════════════════════════════════════

describe('MySQL dialect', () => {
    const mysql = DIALECTS.mysql;

    it('generates upsertSQL with ON DUPLICATE KEY UPDATE', () => {
        const sql = mysql.upsertSQL('users', ['user_id', 'name', '_hlc'], ['user_id']);
        expect(sql).toContain('ON DUPLICATE KEY UPDATE');
    });

    it('generates upsertSQL with INSERT IGNORE when only key columns', () => {
        const sql = mysql.upsertSQL('t', ['id'], ['id']);
        expect(sql).toContain('INSERT IGNORE');
    });

    it('generates infraSchemaSQL', () => {
        const stmts = mysql.infraSchemaSQL();
        expect(stmts[0]).toContain('AUTO_INCREMENT');
        expect(stmts.some(s => s.includes('_full_sync_needs'))).toBe(true);
        expect(stmts.some(s => s.includes('_full_sync_claims'))).toBe(true);
    });

    it('generates triggersSQL', () => {
        const stmts = mysql.triggersSQL('users', {
            keyColumns: ['user_id'],
            dataColumns: ['user_id', 'name', '_hlc'],
            blobColumns: [],
        });
        expect(stmts.length).toBe(5);
        expect(stmts.some(s => s.includes('SIGNAL SQLSTATE'))).toBe(true);
    });

    it('generates MySQL triggers with HEX for blob columns', () => {
        const stmts = mysql.triggersSQL('files', {
            keyColumns: ['file_id'],
            dataColumns: ['file_id', 'content', '_hlc'],
            blobColumns: ['content'],
        });
        expect(stmts.some(s => s.includes('HEX(NEW.content)'))).toBe(true);
    });

    it('generates dropTriggersSQL', () => {
        const stmts = mysql.dropTriggersSQL('users');
        expect(stmts.length).toBe(5);
    });
});

// ═══════════════════════════════════════════════════
//  applyDialect
// ═══════════════════════════════════════════════════

describe('applyDialect', () => {
    it('throws on unknown dialect', () => {
        expect(() => applyDialect({}, 'oracle')).toThrow('Unknown dialect');
    });

    it('adds all methods to empty db adapter', () => {
        const db = { run() {}, get() {}, all() {}, exec() {} };
        applyDialect(db, 'sqlite');

        expect(typeof db.beginTransaction).toBe('function');
        expect(typeof db.commit).toBe('function');
        expect(typeof db.rollback).toBe('function');
        expect(typeof db.upsertSQL).toBe('function');
        expect(typeof db.infraSchemaSQL).toBe('function');
        expect(typeof db.triggersSQL).toBe('function');
        expect(typeof db.dropTriggersSQL).toBe('function');
    });

    it('does not override existing methods', () => {
        const customUpsert = () => 'CUSTOM SQL';
        const db = { run() {}, get() {}, all() {}, exec() {}, upsertSQL: customUpsert };
        applyDialect(db, 'sqlite');
        expect(db.upsertSQL).toBe(customUpsert);
    });

    it('transaction methods delegate to db.exec', () => {
        const execCalls = [];
        const db = { run() {}, get() {}, all() {}, exec(sql) { execCalls.push(sql); } };
        applyDialect(db, 'sqlite');
        db.beginTransaction();
        db.commit();
        db.rollback();
        expect(execCalls).toEqual(['BEGIN IMMEDIATE', 'COMMIT', 'ROLLBACK']);
    });

    it('works with postgresql alias', () => {
        const db = { run() {}, get() {}, all() {}, exec() {} };
        applyDialect(db, 'postgres');
        expect(typeof db.upsertSQL).toBe('function');
        const sql = db.upsertSQL('t', ['a', 'b'], ['a']);
        expect(sql).toContain('ON CONFLICT');
    });
});

// ═══════════════════════════════════════════════════
//  validateDialectMethods
// ═══════════════════════════════════════════════════

describe('validateDialectMethods', () => {
    it('passes when all methods are present', () => {
        const db = {
            beginTransaction() {}, commit() {}, rollback() {},
            upsertSQL() {}, infraSchemaSQL() {},
            triggersSQL() {}, dropTriggersSQL() {},
        };
        expect(() => validateDialectMethods(db)).not.toThrow();
    });

    it('throws with list of missing methods', () => {
        const db = { beginTransaction() {}, commit() {} };
        expect(() => validateDialectMethods(db)).toThrow('missing required methods');
        expect(() => validateDialectMethods(db)).toThrow('rollback');
    });

    it('throws for completely empty adapter', () => {
        expect(() => validateDialectMethods({})).toThrow('missing required methods');
    });
});
