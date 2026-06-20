import { afterEach, describe, expect, it, vi } from 'vitest';

import type { FeederLogger } from '../../server/feederServer.types.js';
import { createCrashHandler } from '../../health/crashHandlers.js';

const NOOP_LOGGER: FeederLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

describe('createCrashHandler', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('writes a synchronous stderr line, attempts an alert and exits non-zero', async () => {
    vi.useFakeTimers();
    const exit = vi.fn();
    const writeStderr = vi.fn();
    const sendAlert = vi.fn(async () => undefined);
    const handleFatalError = createCrashHandler({ sendAlert, logger: NOOP_LOGGER, exit, writeStderr, gracePeriodMs: 5000 });

    handleFatalError(new Error('boom'), 'uncaughtException');

    expect(writeStderr).toHaveBeenCalledWith(expect.stringContaining('uncaughtException'));
    expect(writeStderr).toHaveBeenCalledWith(expect.stringContaining('boom'));
    expect(sendAlert).toHaveBeenCalledTimes(1);
    expect(sendAlert.mock.calls[0][0]).toContain('boom');

    await vi.waitFor(() => expect(exit).toHaveBeenCalledWith(1));
    expect(exit).toHaveBeenCalledTimes(1);
  });

  it('still exits when the alert never resolves (hard grace cap)', async () => {
    vi.useFakeTimers();
    const exit = vi.fn();
    const sendAlert = vi.fn(() => new Promise<void>(() => undefined));
    const handleFatalError = createCrashHandler({ sendAlert, logger: NOOP_LOGGER, exit, writeStderr: vi.fn(), gracePeriodMs: 5000 });

    handleFatalError(new Error('stuck'), 'unhandledRejection');

    expect(exit).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(5000);

    expect(exit).toHaveBeenCalledWith(1);
    expect(exit).toHaveBeenCalledTimes(1);
  });

  it('ignores a second fatal error while already handling the first', async () => {
    vi.useFakeTimers();
    const exit = vi.fn();
    const sendAlert = vi.fn(async () => undefined);
    const handleFatalError = createCrashHandler({ sendAlert, logger: NOOP_LOGGER, exit, writeStderr: vi.fn(), gracePeriodMs: 5000 });

    handleFatalError(new Error('first'), 'uncaughtException');
    handleFatalError(new Error('second'), 'unhandledRejection');

    expect(sendAlert).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(5000);

    expect(exit).toHaveBeenCalledTimes(1);
  });
});
