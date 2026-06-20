import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { startChannelConnectivityMonitor } from '../../telegram/channelConnectivityMonitor.js';

const RETRY_MS = 15_000;
const RECHECK_MS = 60_000;

function makeLogger() {
  return { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
}

describe('startChannelConnectivityMonitor', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('screams in the logs while the probe fails and logs recovery once it succeeds', async () => {
    let attempt = 0;
    const probe = vi.fn(async () => {
      attempt += 1;

      if (attempt < 3) {
        throw new Error('ETELEGRAM');
      }
    });
    const logger = makeLogger();
    const monitor = startChannelConnectivityMonitor({ probe, logger, retryDelayMs: RETRY_MS, recheckIntervalMs: RECHECK_MS });

    await vi.advanceTimersByTimeAsync(1);
    expect(logger.error).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(logger.error).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(logger.info).toHaveBeenCalledTimes(1);

    monitor.stop();
  });

  it('rechecks periodically after becoming healthy and screams if it later goes down', async () => {
    let isUp = true;
    const probe = vi.fn(async () => {
      if (!isUp) {
        throw new Error('down');
      }
    });
    const logger = makeLogger();
    const monitor = startChannelConnectivityMonitor({ probe, logger, retryDelayMs: RETRY_MS, recheckIntervalMs: RECHECK_MS });

    await vi.advanceTimersByTimeAsync(1);
    expect(logger.error).not.toHaveBeenCalled();

    isUp = false;
    await vi.advanceTimersByTimeAsync(RECHECK_MS);

    expect(logger.error).toHaveBeenCalledTimes(1);

    monitor.stop();
  });

  it('stops probing after stop()', async () => {
    const probe = vi.fn(async () => undefined);
    const logger = makeLogger();
    const monitor = startChannelConnectivityMonitor({ probe, logger, retryDelayMs: RETRY_MS, recheckIntervalMs: RECHECK_MS });

    await vi.advanceTimersByTimeAsync(1);
    monitor.stop();
    const callCountAfterStop = probe.mock.calls.length;

    await vi.advanceTimersByTimeAsync(RECHECK_MS * 3);

    expect(probe.mock.calls.length).toBe(callCountAfterStop);
  });
});
