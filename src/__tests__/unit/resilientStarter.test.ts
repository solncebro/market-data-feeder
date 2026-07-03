import { afterEach, describe, expect, it, vi } from 'vitest';

import { createResilientStarter } from '../../utils/resilientStarter.js';

const noopLogger = { info: () => undefined, warn: () => undefined, error: () => undefined };

const RETRY_DELAY_MS = 60_000;

function makeStarter(attempt: () => Promise<void>) {
  return createResilientStarter({ attempt, retryDelayMs: RETRY_DELAY_MS, label: 'test-starter', logger: noopLogger });
}

describe('createResilientStarter', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('start resolves even when the attempt rejects (never fatal)', async () => {
    vi.useFakeTimers();
    const attempt = vi.fn(async () => {
      throw new Error('network down');
    });
    const starter = makeStarter(attempt);

    await expect(starter.start()).resolves.toBeUndefined();
    expect(attempt).toHaveBeenCalledTimes(1);
    starter.stop();
  });

  it('retries after the delay until an attempt succeeds, then stops retrying', async () => {
    vi.useFakeTimers();
    let failCount = 2;
    const attempt = vi.fn(async () => {
      if (failCount > 0) {
        failCount -= 1;
        throw new Error('still down');
      }
    });
    const starter = makeStarter(attempt);

    await starter.start();
    expect(attempt).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(RETRY_DELAY_MS);
    expect(attempt).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(RETRY_DELAY_MS);
    expect(attempt).toHaveBeenCalledTimes(3);

    await vi.advanceTimersByTimeAsync(RETRY_DELAY_MS * 3);
    expect(attempt).toHaveBeenCalledTimes(3);
    starter.stop();
  });

  it('requestRetry after a successful start schedules another attempt (polling died later)', async () => {
    vi.useFakeTimers();
    const attempt = vi.fn(async () => undefined);
    const starter = makeStarter(attempt);

    await starter.start();
    expect(attempt).toHaveBeenCalledTimes(1);

    starter.requestRetry();
    await vi.advanceTimersByTimeAsync(RETRY_DELAY_MS);
    expect(attempt).toHaveBeenCalledTimes(2);
    starter.stop();
  });

  it('deduplicates concurrent retry requests into a single attempt', async () => {
    vi.useFakeTimers();
    const attempt = vi.fn(async () => undefined);
    const starter = makeStarter(attempt);

    await starter.start();
    starter.requestRetry();
    starter.requestRetry();
    starter.requestRetry();

    await vi.advanceTimersByTimeAsync(RETRY_DELAY_MS * 2);
    expect(attempt).toHaveBeenCalledTimes(2);
    starter.stop();
  });

  it('stop cancels the pending retry and blocks future retries', async () => {
    vi.useFakeTimers();
    const attempt = vi.fn(async () => {
      throw new Error('down');
    });
    const starter = makeStarter(attempt);

    await starter.start();
    starter.stop();

    await vi.advanceTimersByTimeAsync(RETRY_DELAY_MS * 3);
    expect(attempt).toHaveBeenCalledTimes(1);

    starter.requestRetry();
    await vi.advanceTimersByTimeAsync(RETRY_DELAY_MS * 3);
    expect(attempt).toHaveBeenCalledTimes(1);
  });
});
