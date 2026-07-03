import { describe, expect, it, vi } from 'vitest';

import { runShutdownStepsBestEffort } from '../../utils/shutdownSteps.js';

describe('runShutdownStepsBestEffort', () => {
  it('runs every step even when an earlier one rejects', async () => {
    const runLog: string[] = [];
    const onStepError = vi.fn();

    await runShutdownStepsBestEffort(
      [
        { label: 'first', run: async () => { runLog.push('first'); } },
        { label: 'second', run: async () => { throw new Error('boom'); } },
        { label: 'third', run: async () => { runLog.push('third'); } },
      ],
      onStepError,
    );

    expect(runLog).toEqual(['first', 'third']);
    expect(onStepError).toHaveBeenCalledTimes(1);
    expect(onStepError).toHaveBeenCalledWith('second', expect.any(Error));
  });

  it('survives a synchronously throwing step', async () => {
    const runLog: string[] = [];
    const onStepError = vi.fn();

    await runShutdownStepsBestEffort(
      [
        { label: 'sync-throw', run: () => { throw new Error('sync boom'); } },
        { label: 'after', run: async () => { runLog.push('after'); } },
      ],
      onStepError,
    );

    expect(runLog).toEqual(['after']);
    expect(onStepError).toHaveBeenCalledWith('sync-throw', expect.any(Error));
  });

  it('runs steps sequentially in order', async () => {
    const runLog: string[] = [];

    await runShutdownStepsBestEffort(
      [
        { label: 'a', run: async () => { await Promise.resolve(); runLog.push('a'); } },
        { label: 'b', run: async () => { runLog.push('b'); } },
      ],
      () => undefined,
    );

    expect(runLog).toEqual(['a', 'b']);
  });
});
