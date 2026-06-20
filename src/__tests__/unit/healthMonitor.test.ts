import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { RestartGuard } from '../../health/restartGuard.js';
import type { HealthMonitorConfig } from '../../health/healthMonitor.types.js';
import { createHealthMonitor } from '../../health/healthMonitor.js';

const SILENT_CONFIG: HealthMonitorConfig = { alertDedupMs: 300_000, recoveryGraceMs: 90_000, batchFlushMs: 3000 };

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
  it('alerts on a light event without ever escalating to a restart', async () => {
    const { monitor, sendAlert, onRestart } = makeMonitor();

    monitor.report({ kind: 'persistentStaleSymbol', interval: '30m', symbol: 'FOOUSDT' });
    await vi.advanceTimersByTimeAsync(3000);

    expect(sendAlert).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(600_000);

    expect(onRestart).not.toHaveBeenCalled();
  });

  it('batches a burst of stuck symbols into a single alert', async () => {
    const { monitor, sendAlert, alertList } = makeMonitor();

    monitor.report({ kind: 'persistentStaleSymbol', interval: '30m', symbol: 'FOOUSDT' });
    monitor.report({ kind: 'persistentStaleSymbol', interval: '30m', symbol: 'FOOUSDT' });
    monitor.report({ kind: 'persistentStaleSymbol', interval: '30m', symbol: 'BARUSDT' });

    expect(sendAlert).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(3000);

    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(alertList[0]).toContain('FOOUSDT');
    expect(alertList[0]).toContain('BARUSDT');
    expect(alertList[0].toLowerCase()).toContain('stuck');
  });

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

  it('batches a burst of recovery-failed streams into one alert without restarting the whole process', async () => {
    const { monitor, onRestart, alertList } = makeMonitor();

    monitor.report({ kind: 'klineStreamRecoveryFailed', interval: '5m', symbol: 'AAAUSDT', consecutiveFailCount: 3 });
    monitor.report({ kind: 'klineStreamRecoveryFailed', interval: '5m', symbol: 'BBBUSDT', consecutiveFailCount: 3 });

    await vi.advanceTimersByTimeAsync(3000);

    const failedAlert = alertList.find((message) => message.toLowerCase().includes('recovery failed'));
    expect(failedAlert).toBeDefined();
    expect(failedAlert).toContain('AAAUSDT');
    expect(failedAlert).toContain('BBBUSDT');

    await vi.advanceTimersByTimeAsync(120_000);
    expect(onRestart).not.toHaveBeenCalled();
  });

  it('sends a single unstuck alert once stuck symbols produce fresh candles again', async () => {
    const { monitor, sendAlert, alertList } = makeMonitor();

    monitor.report({ kind: 'persistentStaleSymbol', interval: '30m', symbol: 'FOOUSDT' });
    await vi.advanceTimersByTimeAsync(3000);

    monitor.report({ kind: 'persistentStaleRecovered', interval: '30m', symbol: 'FOOUSDT' });
    await vi.advanceTimersByTimeAsync(3000);

    expect(sendAlert).toHaveBeenCalledTimes(2);
    expect(alertList[1]).toContain('FOOUSDT');
    expect(alertList[1].toLowerCase()).toContain('fresh candles again');
  });

  it('batches many stalled stream events into a single alert', async () => {
    const { monitor, sendAlert, alertList } = makeMonitor();

    monitor.report({ kind: 'klineStreamStale', interval: '5m', symbol: 'AAAUSDT', ageMs: 69_000 });
    monitor.report({ kind: 'klineStreamStale', interval: '5m', symbol: 'BBBUSDT', ageMs: 70_000 });

    expect(sendAlert).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(3000);

    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(alertList[0]).toContain('AAAUSDT');
    expect(alertList[0]).toContain('BBBUSDT');
  });

  it('sends a single recovered alert once stalled streams come back', async () => {
    const { monitor, sendAlert, alertList } = makeMonitor();

    monitor.report({ kind: 'klineStreamStale', interval: '5m', symbol: 'AAAUSDT', ageMs: 69_000 });
    monitor.report({ kind: 'klineStreamStale', interval: '5m', symbol: 'BBBUSDT', ageMs: 70_000 });
    await vi.advanceTimersByTimeAsync(3000);

    monitor.report({ kind: 'klineStreamRecovered', interval: '5m', symbol: 'AAAUSDT' });
    monitor.report({ kind: 'klineStreamRecovered', interval: '5m', symbol: 'BBBUSDT' });
    await vi.advanceTimersByTimeAsync(3000);

    expect(sendAlert).toHaveBeenCalledTimes(2);
    expect(alertList[1].toLowerCase()).toContain('recovered');
    expect(alertList[1]).toContain('AAAUSDT');
  });

  it('stays silent when a stalled stream recovers before the batch flush', async () => {
    const { monitor, sendAlert } = makeMonitor();

    monitor.report({ kind: 'klineStreamStale', interval: '5m', symbol: 'AAAUSDT', ageMs: 69_000 });
    monitor.report({ kind: 'klineStreamRecovered', interval: '5m', symbol: 'AAAUSDT' });

    await vi.advanceTimersByTimeAsync(3000);

    expect(sendAlert).not.toHaveBeenCalled();
  });

  it('sends a recovered alert even when the stall was never reported per symbol', async () => {
    const { monitor, sendAlert, alertList } = makeMonitor();

    monitor.report({ kind: 'klineStreamRecovered', interval: '5m', symbol: 'ZZZUSDT' });
    await vi.advanceTimersByTimeAsync(3000);

    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(alertList[0].toLowerCase()).toContain('recovered');
    expect(alertList[0]).toContain('ZZZUSDT');
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

  it('batches on-demand symbol loads into a single alert', async () => {
    const { monitor, sendAlert, alertList } = makeMonitor();

    monitor.report({ kind: 'symbolLoadCompleted', interval: '5m', symbol: 'NEWAUSDT' });
    monitor.report({ kind: 'symbolLoadCompleted', interval: '5m', symbol: 'NEWBUSDT' });

    expect(sendAlert).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(3000);

    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(alertList[0]).toContain('NEWAUSDT');
    expect(alertList[0]).toContain('NEWBUSDT');
  });
});
