import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { RestartGuard } from '../../health/restartGuard.js';
import type { HealthMonitorConfig } from '../../health/healthMonitor.types.js';
import { createHealthMonitor } from '../../health/healthMonitor.js';

const SILENT_CONFIG: HealthMonitorConfig = { alertDedupMs: 300_000, recoveryGraceMs: 90_000, batchFlushMs: 3000, digestIntervalMs: 1_800_000 };

const noopLogger = { info: () => undefined, warn: () => undefined, error: () => undefined };

function makeGuard(canRestart: boolean): RestartGuard & { recordRestart: ReturnType<typeof vi.fn> } {
  return {
    canRestart: () => canRestart,
    recordRestart: vi.fn(),
    getRecentRestartCount: () => 0,
  };
}

function makeMonitor(overrides: Partial<HealthMonitorConfig> = {}, canRestart: boolean = true) {
  const alertList: string[] = [];
  const sendAlert = vi.fn((message: string) => { alertList.push(message); });
  const onRestart = vi.fn();
  const restartGuard = makeGuard(canRestart);
  const monitor = createHealthMonitor({
    config: { ...SILENT_CONFIG, ...overrides },
    sendAlert,
    restartGuard,
    onRestart,
    logger: noopLogger,
  });

  return { monitor, sendAlert, onRestart, restartGuard, alertList };
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('createHealthMonitor', () => {
  it('escalates a silent stream to a restart after the recovery grace window', async () => {
    const { monitor, sendAlert, onRestart, restartGuard } = makeMonitor();

    monitor.report({ kind: 'streamSilent', interval: '30m', silenceMs: 50_000 });

    expect(sendAlert).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(80_000);
    expect(onRestart).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(20_000);

    expect(restartGuard.recordRestart).toHaveBeenCalledTimes(1);
    expect(onRestart).toHaveBeenCalledTimes(1);
  });

  it('cancels the pending restart when the stream resumes within the grace window', async () => {
    const { monitor, onRestart, sendAlert } = makeMonitor();

    monitor.report({ kind: 'streamSilent', interval: '30m', silenceMs: 50_000 });
    await vi.advanceTimersByTimeAsync(40_000);
    monitor.report({ kind: 'streamResumed', interval: '30m', silenceMs: 60_000 });

    await vi.advanceTimersByTimeAsync(120_000);

    expect(onRestart).not.toHaveBeenCalled();
    const recoveredAlert = sendAlert.mock.calls.map((call) => call[0]).find((message) => message.toLowerCase().includes('resumed'));
    expect(recoveredAlert).toBeDefined();
  });

  it('does not restart when the loop guard is exhausted, escalating to a human instead', async () => {
    const { monitor, onRestart, restartGuard, alertList } = makeMonitor({}, false);

    monitor.report({ kind: 'streamSilent', interval: '30m', silenceMs: 50_000 });
    await vi.advanceTimersByTimeAsync(120_000);

    expect(onRestart).not.toHaveBeenCalled();
    expect(restartGuard.recordRestart).not.toHaveBeenCalled();
    expect(alertList.some((message) => message.toLowerCase().includes('manual intervention'))).toBe(true);
  });

  it('escalates a critical transport notification but only alerts on a benign one', async () => {
    const critical = makeMonitor();
    critical.monitor.report({ kind: 'transportNotify', message: 'CRITICAL: max retries (15) exceeded' });
    await vi.advanceTimersByTimeAsync(120_000);
    expect(critical.onRestart).toHaveBeenCalledTimes(1);

    const benign = makeMonitor();
    benign.monitor.report({ kind: 'transportNotify', message: 'reconnecting after disruption' });
    await vi.advanceTimersByTimeAsync(120_000);
    expect(benign.onRestart).not.toHaveBeenCalled();
    expect(benign.sendAlert).toHaveBeenCalled();
  });

  it('drops the kline-watchdog overdue/recovery transport summaries (covered by the digest)', async () => {
    const { monitor, sendAlert, onRestart } = makeMonitor();

    monitor.report({ kind: 'transportNotify', message: 'Bybit Futures — Kline subscriptions overdue (10 total)' });
    monitor.report({ kind: 'transportNotify', message: 'Bybit Futures — Kline recovery complete (10 symbols)' });
    await vi.advanceTimersByTimeAsync(120_000);

    expect(sendAlert).not.toHaveBeenCalled();
    expect(onRestart).not.toHaveBeenCalled();
  });

  it('keeps per-symbol stall/recovery signals silent (they feed the digest, not immediate alerts)', async () => {
    const { monitor, sendAlert } = makeMonitor();

    monitor.report({ kind: 'klineStreamStale', interval: '5m', symbol: 'AAAUSDT', ageMs: 70_000 });
    monitor.report({ kind: 'klineStreamRecovered', interval: '5m', symbol: 'AAAUSDT' });
    monitor.report({ kind: 'persistentStaleSymbol', interval: '30m', symbol: 'BBBUSDT' });
    monitor.report({ kind: 'persistentStaleRecovered', interval: '30m', symbol: 'BBBUSDT' });
    monitor.report({ kind: 'symbolLoadCompleted', interval: '5m', symbol: 'CCCUSDT' });
    monitor.report({ kind: 'symbolLoadFailed', interval: '5m', symbol: 'DDDUSDT' });

    await vi.advanceTimersByTimeAsync(10_000);

    expect(sendAlert).not.toHaveBeenCalled();
  });

  it('still alerts immediately when a stream fails to recover, batching a burst into one message', async () => {
    const { monitor, sendAlert, onRestart, alertList } = makeMonitor();

    monitor.report({ kind: 'klineStreamRecoveryFailed', interval: '5m', symbol: 'AAAUSDT', consecutiveFailCount: 3 });
    monitor.report({ kind: 'klineStreamRecoveryFailed', interval: '5m', symbol: 'BBBUSDT', consecutiveFailCount: 3 });

    expect(sendAlert).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(3000);

    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(alertList[0]).toContain('AAAUSDT');
    expect(alertList[0]).toContain('BBBUSDT');
    expect(alertList[0].toLowerCase()).toContain('recovery failed');

    await vi.advanceTimersByTimeAsync(120_000);
    expect(onRestart).not.toHaveBeenCalled();
  });

  it('sends a digest summarizing degraded and recovered streams after the digest interval', async () => {
    const { monitor, sendAlert, alertList } = makeMonitor({ digestIntervalMs: 5000 });

    monitor.report({ kind: 'klineStreamStale', interval: '5m', symbol: 'AAAUSDT', ageMs: 70_000 });
    monitor.report({ kind: 'persistentStaleSymbol', interval: '30m', symbol: 'BBBUSDT' });
    monitor.report({ kind: 'klineStreamRecovered', interval: '5m', symbol: 'AAAUSDT' });

    expect(sendAlert).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(5000);

    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(alertList[0].toLowerCase()).toContain('degraded');
    expect(alertList[0].toLowerCase()).toContain('recovered');
    expect(alertList[0]).toContain('2');
    expect(alertList[0]).toContain('1');
  });

  it('lists streams that failed to recover in the digest until they recover', async () => {
    const { monitor, alertList } = makeMonitor({ digestIntervalMs: 5000 });

    monitor.report({ kind: 'klineStreamRecoveryFailed', interval: '5m', symbol: 'ZZZUSDT', consecutiveFailCount: 3 });
    await vi.advanceTimersByTimeAsync(5000);

    const digest = alertList.find((message) => message.toLowerCase().includes('unrecovered'));
    expect(digest).toBeDefined();
    expect(digest).toContain('ZZZUSDT');

    monitor.report({ kind: 'klineStreamRecovered', interval: '5m', symbol: 'ZZZUSDT' });
    await vi.advanceTimersByTimeAsync(5000);

    const lastDigest = alertList[alertList.length - 1];
    expect(lastDigest).not.toContain('ZZZUSDT');
  });

  it('reports all-healthy in the digest on a clean window', async () => {
    const { monitor, sendAlert, alertList } = makeMonitor({ digestIntervalMs: 5000 });

    await vi.advanceTimersByTimeAsync(5000);

    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(alertList[0].toLowerCase()).toContain('healthy');
  });

  it('announces feeder readiness immediately', () => {
    const { monitor, sendAlert, alertList } = makeMonitor();

    monitor.report({ kind: 'feederReady', exchangeName: 'bybit', host: '127.0.0.1', port: 7070 });

    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(alertList[0]).toContain('BYBIT');
    expect(alertList[0]).toContain('7070');
    expect(alertList[0].toLowerCase()).toContain('serving');
  });

  it('announces an on-demand interval load start and completion immediately', () => {
    const { monitor, sendAlert, alertList } = makeMonitor();

    monitor.report({ kind: 'intervalLoadStarted', interval: '30m', symbolCount: 585 });
    monitor.report({ kind: 'intervalLoadCompleted', interval: '30m', symbolCount: 585 });

    expect(sendAlert).toHaveBeenCalledTimes(2);
    expect(alertList[0].toLowerCase()).toContain('loading');
    expect(alertList[1].toLowerCase()).toContain('ready');
  });

  it('escalates a mass-stale source to a restart after the recovery grace window', async () => {
    const { monitor, sendAlert, onRestart, restartGuard } = makeMonitor();

    monitor.report({ kind: 'sourceMassStale', interval: '30m', staleCount: 200, symbolCount: 450 });

    expect(sendAlert).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(80_000);
    expect(onRestart).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(20_000);

    expect(restartGuard.recordRestart).toHaveBeenCalledTimes(1);
    expect(onRestart).toHaveBeenCalledTimes(1);
  });

  it('cancels the pending restart when mass staleness recovers within the grace window', async () => {
    const { monitor, onRestart, alertList } = makeMonitor();

    monitor.report({ kind: 'sourceMassStale', interval: '30m', staleCount: 200, symbolCount: 450 });
    await vi.advanceTimersByTimeAsync(40_000);
    monitor.report({ kind: 'sourceMassStaleRecovered', interval: '30m' });

    await vi.advanceTimersByTimeAsync(120_000);

    expect(onRestart).not.toHaveBeenCalled();
    expect(alertList.some((message) => message.toLowerCase().includes('cleared'))).toBe(true);
  });

  it('drains in-flight alert sends before shutdown resolves', async () => {
    let resolveSend: (() => void) | null = null;
    const sendAlert = vi.fn(() => new Promise<void>((resolve) => {
      resolveSend = resolve;
    }));
    const monitor = createHealthMonitor({ config: SILENT_CONFIG, sendAlert, restartGuard: makeGuard(true), onRestart: vi.fn(), logger: noopLogger });

    monitor.report({ kind: 'feederReady', exchangeName: 'bybit', host: '127.0.0.1', port: 7070 });

    let isDone = false;
    const shutdownPromise = monitor.shutdown().then(() => {
      isDone = true;
    });
    await Promise.resolve();
    await Promise.resolve();

    expect(isDone).toBe(false);

    resolveSend?.();
    await shutdownPromise;

    expect(isDone).toBe(true);
  });

  it('does not hang shutdown when an alert send never settles', async () => {
    const sendAlert = vi.fn(() => new Promise<void>(() => undefined));
    const monitor = createHealthMonitor({ config: SILENT_CONFIG, sendAlert, restartGuard: makeGuard(true), onRestart: vi.fn(), logger: noopLogger });

    monitor.report({ kind: 'feederReady', exchangeName: 'bybit', host: '127.0.0.1', port: 7070 });

    let isDone = false;
    monitor.shutdown().then(() => {
      isDone = true;
    });
    await vi.advanceTimersByTimeAsync(2500);

    expect(isDone).toBe(true);
  });
});
