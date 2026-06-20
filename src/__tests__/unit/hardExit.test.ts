import { afterEach, describe, expect, it, vi } from 'vitest';

import { runWithHardExit } from '../../utils/hardExit.js';

describe('runWithHardExit', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('completes without firing the hard-exit when the task resolves in time', async () => {
    vi.useFakeTimers();
    const onTimeout = vi.fn();

    await runWithHardExit(async () => undefined, 5000, onTimeout);

    expect(onTimeout).not.toHaveBeenCalled();
  });

  it('fires the hard-exit when the task hangs past the timeout', async () => {
    vi.useFakeTimers();
    const onTimeout = vi.fn();

    runWithHardExit(() => new Promise<void>(() => undefined), 5000, onTimeout);

    await vi.advanceTimersByTimeAsync(5000);

    expect(onTimeout).toHaveBeenCalledTimes(1);
  });

  it('rethrows a task error after clearing the hard-exit timer', async () => {
    vi.useFakeTimers();
    const onTimeout = vi.fn();

    await expect(runWithHardExit(async () => {
      throw new Error('teardown failed');
    }, 5000, onTimeout)).rejects.toThrow('teardown failed');

    await vi.advanceTimersByTimeAsync(5000);

    expect(onTimeout).not.toHaveBeenCalled();
  });
});
