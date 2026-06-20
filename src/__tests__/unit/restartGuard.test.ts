import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { createRestartGuard } from '../../health/restartGuard.js';

let stateDir: string;
let stateFilePath: string;

beforeEach(() => {
  stateDir = mkdtempSync(join(tmpdir(), 'feeder-restart-guard-'));
  stateFilePath = join(stateDir, 'restart-log.json');
});

afterEach(() => {
  rmSync(stateDir, { recursive: true, force: true });
});

describe('createRestartGuard', () => {
  it('allows a restart when there is no prior state', () => {
    const guard = createRestartGuard({ filePath: stateFilePath, windowMs: 30 * 60_000, maxRestarts: 3, now: () => 1000 });

    expect(guard.getRecentRestartCount()).toBe(0);
    expect(guard.canRestart()).toBe(true);
  });

  it('blocks once the restart count within the window reaches the limit', () => {
    let nowMs = 1_000_000;
    const guard = createRestartGuard({ filePath: stateFilePath, windowMs: 30 * 60_000, maxRestarts: 3, now: () => nowMs });

    guard.recordRestart();
    nowMs += 1000;
    guard.recordRestart();
    nowMs += 1000;
    guard.recordRestart();

    expect(guard.getRecentRestartCount()).toBe(3);
    expect(guard.canRestart()).toBe(false);
  });

  it('forgets restarts that fall outside the window', () => {
    let nowMs = 1_000_000;
    const windowMs = 30 * 60_000;
    const guard = createRestartGuard({ filePath: stateFilePath, windowMs, maxRestarts: 3, now: () => nowMs });

    guard.recordRestart();
    guard.recordRestart();
    guard.recordRestart();
    expect(guard.canRestart()).toBe(false);

    nowMs += windowMs + 1;

    expect(guard.getRecentRestartCount()).toBe(0);
    expect(guard.canRestart()).toBe(true);
  });

  it('persists state across guard instances (survives a process restart)', () => {
    const nowMs = 5_000_000;
    const first = createRestartGuard({ filePath: stateFilePath, windowMs: 30 * 60_000, maxRestarts: 2, now: () => nowMs });

    first.recordRestart();
    first.recordRestart();

    const second = createRestartGuard({ filePath: stateFilePath, windowMs: 30 * 60_000, maxRestarts: 2, now: () => nowMs });

    expect(second.getRecentRestartCount()).toBe(2);
    expect(second.canRestart()).toBe(false);
  });

  it('treats unreadable/corrupt state as no restarts (fail-open) without throwing', () => {
    const guard = createRestartGuard({ filePath: join(stateDir, 'missing', 'nested.json'), windowMs: 30 * 60_000, maxRestarts: 3, now: () => 1000 });

    expect(() => guard.getRecentRestartCount()).not.toThrow();
    expect(guard.getRecentRestartCount()).toBe(0);
    expect(guard.canRestart()).toBe(true);
  });
});
