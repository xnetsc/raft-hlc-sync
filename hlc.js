/**
 * HLC (Hybrid Logical Clock) - 混合逻辑时钟
 *
 * 纯算法实现，无外部依赖。
 * 解决分布式节点间操作排序问题，不依赖严格的 NTP 时钟同步。
 *
 * 时间戳格式: "壁钟时间(15位).逻辑计数器(5位).nodeId"
 *   - 字符串字典序比较即可确定先后
 *   - 保证因果关系: 如果 A happens-before B，则 ts(A) < ts(B)
 *   - 对于真正并发的操作，wallTime 大的胜出；wallTime 相同则 counter 大的胜出；
 *     都相同则 nodeId 字典序大的胜出
 *
 * getNow 返回值约定:
 *   { value: number, unit: 's' | 'ms' | 'us' | 'ns' }
 *   value — 当前时间戳数值
 *   unit  — 时间单位（秒、毫秒、微秒、纳秒）
 *   内部统一转换为毫秒用于 wallTime 排序。
 */

// ╔═══════════════════════════════════════╗
// ║       时间单位转换工具（纯函数）        ║
// ╚═══════════════════════════════════════╝

/**
 * 支持的时间单位及其到毫秒的换算因子
 * @type {Readonly<Record<string, number>>}
 */
const TIME_UNIT_TO_MS = Object.freeze({ s: 1000, ms: 1, us: 1e-3, ns: 1e-6 });

/**
 * 支持的时间单位列表
 * @type {string[]}
 */
export const SUPPORTED_TIME_UNITS = Object.keys(TIME_UNIT_TO_MS);

/**
 * 将 getNow() 返回的 { value, unit } 对象转换为毫秒数值
 *
 * @param {{ value: number, unit: 's' | 'ms' | 'us' | 'ns' }} timeObj
 * @returns {number} 毫秒级时间戳
 */
export function timeValueToMs(timeObj) {
    if (!timeObj || typeof timeObj.value !== 'number' || typeof timeObj.unit !== 'string') {
        throw new Error(
            'getNow() must return { value: number, unit: string }. ' +
            'Supported units: ' + SUPPORTED_TIME_UNITS.join(', ')
        );
    }
    const factor = TIME_UNIT_TO_MS[timeObj.unit];
    if (factor === undefined) {
        throw new Error(
            'Unsupported time unit: "' + timeObj.unit + '". ' +
            'Supported units: ' + SUPPORTED_TIME_UNITS.join(', ')
        );
    }
    return timeObj.value * factor;
}

export class HLC {
    /**
     * @param {string} nodeId - 节点唯一标识
     * @param {function(): { value: number, unit: 's' | 'ms' | 'us' | 'ns' }} getNow
     *   获取当前时间戳的函数（必须外部注入）。
     *   返回值包含时间戳数值和单位，内部自动转换为毫秒。
     */
    constructor(nodeId, getNow) {
        if (typeof getNow !== 'function') {
            throw new Error(
                'HLC: getNow is required and must be a function returning { value: number, unit: "s"|"ms"|"us"|"ns" }.'
            );
        }
        this.nodeId = nodeId;
        this._getNow = getNow;
        this._getNowMs = () => timeValueToMs(this._getNow());
        this.wallTime = this._getNowMs();
        this.counter = 0;
    }

    /**
     * 本地事件（写操作）— 产生新的 HLC 时间戳
     * @returns {string} HLC 时间戳字符串
     */
    tick() {
        const now = this._getNowMs();
        if (now > this.wallTime) {
            this.wallTime = now;
            this.counter = 0;
        } else {
            this.counter++;
        }
        return this.timestamp();
    }

    /**
     * 接收远程事件 — 合并远程时钟后产生新时间戳
     * @param {number} remoteWallTime
     * @param {number} remoteCounter
     * @returns {string} HLC 时间戳字符串
     */
    receive(remoteWallTime, remoteCounter) {
        const now = this._getNowMs();
        if (now > this.wallTime && now > remoteWallTime) {
            this.wallTime = now;
            this.counter = 0;
        } else if (remoteWallTime > this.wallTime) {
            this.wallTime = remoteWallTime;
            this.counter = remoteCounter + 1;
        } else if (this.wallTime > remoteWallTime) {
            this.counter++;
        } else {
            // wallTime 相等
            this.counter = Math.max(this.counter, remoteCounter) + 1;
        }
        return this.timestamp();
    }

    /**
     * 返回当前 HLC 时间戳（不推进时钟）
     * @returns {string}
     */
    timestamp() {
        const wt = this.wallTime.toString().padStart(15, '0');
        const ct = this.counter.toString().padStart(5, '0');
        return `${wt}.${ct}.${this.nodeId}`;
    }

    /**
     * 从 HLC 时间戳字符串解析出各分量
     * @param {string} ts - HLC 时间戳字符串
     * @returns {{ wallTime: number, counter: number, nodeId: string }}
     */
    static parse(ts) {
        const parts = ts.split('.');
        return {
            wallTime: parseInt(parts[0], 10),
            counter: parseInt(parts[1], 10),
            nodeId: parts.slice(2).join('.'),
        };
    }

    /**
     * 比较两个 HLC 时间戳
     * @param {string} a
     * @param {string} b
     * @returns {number} 负数: a < b, 0: a === b, 正数: a > b
     */
    static compare(a, b) {
        if (a < b) return -1;
        if (a > b) return 1;
        return 0;
    }

    /**
     * 从 HLC 时间戳提取 wallTime（用于墓碑清理等）
     * @param {string} ts
     * @returns {number} 毫秒级 UNIX 时间戳
     */
    static extractWallTime(ts) {
        const dotIdx = ts.indexOf('.');
        return parseInt(ts.substring(0, dotIdx), 10);
    }
}
