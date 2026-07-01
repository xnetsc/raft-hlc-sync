import { describe, it, expect } from 'vitest';
import { HLC, timeValueToMs, SUPPORTED_TIME_UNITS } from './hlc.js';

// ═══════════════════════════════════════════════════
//  timeValueToMs
// ═══════════════════════════════════════════════════

describe('timeValueToMs', () => {
    it('converts milliseconds (factor = 1)', () => {
        expect(timeValueToMs({ value: 1000, unit: 'ms' })).toBe(1000);
    });

    it('converts seconds to milliseconds', () => {
        expect(timeValueToMs({ value: 1, unit: 's' })).toBe(1000);
        expect(timeValueToMs({ value: 2.5, unit: 's' })).toBe(2500);
    });

    it('converts microseconds to milliseconds', () => {
        expect(timeValueToMs({ value: 1000000, unit: 'us' })).toBe(1000);
        expect(timeValueToMs({ value: 1, unit: 'us' })).toBeCloseTo(0.001, 6);
    });

    it('converts nanoseconds to milliseconds', () => {
        expect(timeValueToMs({ value: 1000000000, unit: 'ns' })).toBe(1000);
        expect(timeValueToMs({ value: 1000000, unit: 'ns' })).toBe(1);
    });

    it('handles zero', () => {
        expect(timeValueToMs({ value: 0, unit: 'ms' })).toBe(0);
        expect(timeValueToMs({ value: 0, unit: 's' })).toBe(0);
    });

    it('handles large timestamps (epoch-like)', () => {
        const epochMs = 1713000000000;
        expect(timeValueToMs({ value: epochMs, unit: 'ms' })).toBe(epochMs);
        expect(timeValueToMs({ value: epochMs / 1000, unit: 's' })).toBe(epochMs);
        expect(timeValueToMs({ value: epochMs * 1000, unit: 'us' })).toBe(epochMs);
        expect(timeValueToMs({ value: epochMs * 1000000, unit: 'ns' })).toBe(epochMs);
    });

    it('throws on null / undefined input', () => {
        expect(() => timeValueToMs(null)).toThrow('getNow() must return');
        expect(() => timeValueToMs(undefined)).toThrow('getNow() must return');
    });

    it('throws on missing value', () => {
        expect(() => timeValueToMs({ unit: 'ms' })).toThrow('getNow() must return');
    });

    it('throws on missing unit', () => {
        expect(() => timeValueToMs({ value: 1 })).toThrow('getNow() must return');
    });

    it('throws on non-number value', () => {
        expect(() => timeValueToMs({ value: '1000', unit: 'ms' })).toThrow('getNow() must return');
    });

    it('throws on non-string unit', () => {
        expect(() => timeValueToMs({ value: 1000, unit: 123 })).toThrow('getNow() must return');
    });

    it('throws on unsupported unit', () => {
        expect(() => timeValueToMs({ value: 1, unit: 'hours' })).toThrow('Unsupported time unit');
        expect(() => timeValueToMs({ value: 1, unit: 'min' })).toThrow('Unsupported time unit');
    });
});

// ═══════════════════════════════════════════════════
//  SUPPORTED_TIME_UNITS
// ═══════════════════════════════════════════════════

describe('SUPPORTED_TIME_UNITS', () => {
    it('contains exactly s, ms, us, ns', () => {
        expect(SUPPORTED_TIME_UNITS).toEqual(['s', 'ms', 'us', 'ns']);
    });
});

// ═══════════════════════════════════════════════════
//  HLC constructor
// ═══════════════════════════════════════════════════

describe('HLC constructor', () => {
    it('throws when getNow is not a function', () => {
        expect(() => new HLC('node-1')).toThrow('getNow is required');
        expect(() => new HLC('node-1', null)).toThrow('getNow is required');
        expect(() => new HLC('node-1', 123)).toThrow('getNow is required');
    });

    it('throws when getNow returns invalid format', () => {
        expect(() => new HLC('node-1', () => 1000)).toThrow('getNow() must return');
        expect(() => new HLC('node-1', () => ({ value: 'x', unit: 'ms' }))).toThrow('getNow() must return');
    });

    it('initializes wallTime from getNow (ms)', () => {
        const hlc = new HLC('node-1', () => ({ value: 5000, unit: 'ms' }));
        expect(hlc.wallTime).toBe(5000);
        expect(hlc.counter).toBe(0);
        expect(hlc.nodeId).toBe('node-1');
    });

    it('initializes wallTime from getNow (seconds)', () => {
        const hlc = new HLC('node-1', () => ({ value: 5, unit: 's' }));
        expect(hlc.wallTime).toBe(5000);
    });

    it('initializes wallTime from getNow (microseconds)', () => {
        const hlc = new HLC('node-1', () => ({ value: 5000000, unit: 'us' }));
        expect(hlc.wallTime).toBe(5000);
    });

    it('initializes wallTime from getNow (nanoseconds)', () => {
        const hlc = new HLC('node-1', () => ({ value: 5000000000, unit: 'ns' }));
        expect(hlc.wallTime).toBe(5000);
    });
});

// ═══════════════════════════════════════════════════
//  HLC.tick()
// ═══════════════════════════════════════════════════

describe('HLC.tick', () => {
    it('advances wallTime when physical clock moves forward', () => {
        let clock = 1000;
        const hlc = new HLC('n1', () => ({ value: clock, unit: 'ms' }));

        clock = 2000;
        const ts = hlc.tick();
        expect(hlc.wallTime).toBe(2000);
        expect(hlc.counter).toBe(0);
        expect(ts).toContain('n1');
    });

    it('increments counter when physical clock does not advance', () => {
        const hlc = new HLC('n1', () => ({ value: 1000, unit: 'ms' }));

        hlc.tick(); // counter -> 1
        hlc.tick(); // counter -> 2
        expect(hlc.wallTime).toBe(1000);
        expect(hlc.counter).toBe(2);
    });

    it('resets counter when physical clock advances', () => {
        let clock = 1000;
        const hlc = new HLC('n1', () => ({ value: clock, unit: 'ms' }));

        hlc.tick(); // counter -> 1
        hlc.tick(); // counter -> 2
        expect(hlc.counter).toBe(2);

        clock = 2000;
        hlc.tick();
        expect(hlc.wallTime).toBe(2000);
        expect(hlc.counter).toBe(0);
    });

    it('returns properly formatted timestamp string', () => {
        const hlc = new HLC('abc', () => ({ value: 42, unit: 'ms' }));
        const ts = hlc.tick();
        // wallTime=42 → "000000000000042", counter=1 → "00001"
        expect(ts).toBe('000000000000042.00001.abc');
    });

    it('works with different time units', () => {
        let sec = 10;
        const hlc = new HLC('n1', () => ({ value: sec, unit: 's' }));
        // wallTime should be 10000 ms
        expect(hlc.wallTime).toBe(10000);

        sec = 11;
        hlc.tick();
        expect(hlc.wallTime).toBe(11000);
    });
});

// ═══════════════════════════════════════════════════
//  HLC.receive()
// ═══════════════════════════════════════════════════

describe('HLC.receive', () => {
    it('advances to physical clock when it exceeds both local and remote', () => {
        let clock = 1000;
        const hlc = new HLC('n1', () => ({ value: clock, unit: 'ms' }));

        clock = 5000;
        hlc.receive(3000, 10);
        expect(hlc.wallTime).toBe(5000);
        expect(hlc.counter).toBe(0);
    });

    it('follows remote wallTime when remote is ahead', () => {
        const hlc = new HLC('n1', () => ({ value: 1000, unit: 'ms' }));

        hlc.receive(9999, 5);
        expect(hlc.wallTime).toBe(9999);
        expect(hlc.counter).toBe(6); // remoteCounter + 1
    });

    it('increments counter when local wallTime is ahead of remote', () => {
        let clock = 5000;
        const hlc = new HLC('n1', () => ({ value: clock, unit: 'ms' }));
        hlc.wallTime = 9999; // artificially set local ahead

        hlc.receive(3000, 10);
        expect(hlc.wallTime).toBe(9999);
        expect(hlc.counter).toBe(1);
    });

    it('takes max counter + 1 when wallTimes are equal', () => {
        let clock = 1000;
        const hlc = new HLC('n1', () => ({ value: clock, unit: 'ms' }));
        hlc.wallTime = 5000;
        hlc.counter = 3;

        hlc.receive(5000, 7);
        expect(hlc.wallTime).toBe(5000);
        expect(hlc.counter).toBe(8); // max(3,7) + 1
    });

    it('returns a valid timestamp string', () => {
        const hlc = new HLC('n1', () => ({ value: 1000, unit: 'ms' }));
        const ts = hlc.receive(2000, 0);
        expect(typeof ts).toBe('string');
        const parsed = HLC.parse(ts);
        expect(parsed.wallTime).toBe(2000);
    });
});

// ═══════════════════════════════════════════════════
//  HLC.timestamp()
// ═══════════════════════════════════════════════════

describe('HLC.timestamp', () => {
    it('returns current state without advancing clock', () => {
        const hlc = new HLC('n1', () => ({ value: 1000, unit: 'ms' }));
        const ts1 = hlc.timestamp();
        const ts2 = hlc.timestamp();
        expect(ts1).toBe(ts2);
        expect(hlc.counter).toBe(0); // counter not advanced
    });

    it('pads wallTime to 15 digits and counter to 5 digits', () => {
        const hlc = new HLC('z', () => ({ value: 1, unit: 'ms' }));
        expect(hlc.timestamp()).toBe('000000000000001.00000.z');
    });
});

// ═══════════════════════════════════════════════════
//  HLC.parse()
// ═══════════════════════════════════════════════════

describe('HLC.parse', () => {
    it('parses a well-formed HLC timestamp', () => {
        const result = HLC.parse('000001713000000.00042.node-1');
        expect(result.wallTime).toBe(1713000000);
        expect(result.counter).toBe(42);
        expect(result.nodeId).toBe('node-1');
    });

    it('handles nodeId containing dots', () => {
        const result = HLC.parse('000001713000000.00001.a.b.c');
        expect(result.nodeId).toBe('a.b.c');
    });

    it('round-trips with timestamp()', () => {
        let clock = 1713000000000;
        const hlc = new HLC('test-node', () => ({ value: clock++, unit: 'ms' }));
        hlc.tick();
        const ts = hlc.timestamp();
        const parsed = HLC.parse(ts);
        expect(parsed.nodeId).toBe('test-node');
        expect(parsed.wallTime).toBe(hlc.wallTime);
        expect(parsed.counter).toBe(hlc.counter);
    });
});

// ═══════════════════════════════════════════════════
//  HLC.compare()
// ═══════════════════════════════════════════════════

describe('HLC.compare', () => {
    it('returns negative when a < b', () => {
        expect(HLC.compare('000000000001000.00000.n1', '000000000002000.00000.n1')).toBeLessThan(0);
    });

    it('returns positive when a > b', () => {
        expect(HLC.compare('000000000002000.00000.n1', '000000000001000.00000.n1')).toBeGreaterThan(0);
    });

    it('returns 0 when equal', () => {
        expect(HLC.compare('000000000001000.00000.n1', '000000000001000.00000.n1')).toBe(0);
    });

    it('compares by counter when wallTime is equal', () => {
        expect(HLC.compare('000000000001000.00001.n1', '000000000001000.00002.n1')).toBeLessThan(0);
    });

    it('compares by nodeId when wallTime and counter are equal', () => {
        expect(HLC.compare('000000000001000.00001.aaa', '000000000001000.00001.bbb')).toBeLessThan(0);
    });
});

// ═══════════════════════════════════════════════════
//  HLC.extractWallTime()
// ═══════════════════════════════════════════════════

describe('HLC.extractWallTime', () => {
    it('extracts wallTime from valid timestamp', () => {
        expect(HLC.extractWallTime('000001713000000.00042.node-1')).toBe(1713000000);
    });

    it('extracts from timestamp with large wallTime', () => {
        expect(HLC.extractWallTime('001713000000000.00000.x')).toBe(1713000000000);
    });
});

// ═══════════════════════════════════════════════════
//  HLC 因果性保证（集成）
// ═══════════════════════════════════════════════════

describe('HLC causality', () => {
    it('tick() always produces monotonically increasing timestamps', () => {
        let clock = 1000;
        const hlc = new HLC('n1', () => ({ value: clock, unit: 'ms' }));
        const timestamps = [];
        for (let i = 0; i < 50; i++) {
            timestamps.push(hlc.tick());
        }
        for (let i = 1; i < timestamps.length; i++) {
            expect(HLC.compare(timestamps[i - 1], timestamps[i])).toBeLessThan(0);
        }
    });

    it('receive → tick produces timestamp greater than received', () => {
        let clock = 1000;
        const hlc = new HLC('n1', () => ({ value: clock, unit: 'ms' }));
        const receivedTs = hlc.receive(9999, 100);
        const tickTs = hlc.tick();
        expect(HLC.compare(receivedTs, tickTs)).toBeLessThan(0);
    });

    it('two nodes sending events produce correctly ordered timestamps', () => {
        let clockA = 1000, clockB = 1000;
        const hlcA = new HLC('a', () => ({ value: clockA, unit: 'ms' }));
        const hlcB = new HLC('b', () => ({ value: clockB, unit: 'ms' }));

        // A writes
        const tsA1 = hlcA.tick();
        const parsedA1 = HLC.parse(tsA1);

        // B receives A's event, then writes
        hlcB.receive(parsedA1.wallTime, parsedA1.counter);
        const tsB1 = hlcB.tick();

        // B's timestamp must be greater than A's
        expect(HLC.compare(tsA1, tsB1)).toBeLessThan(0);
    });
});
